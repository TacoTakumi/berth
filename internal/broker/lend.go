// Copyright 2026 Trevin Tindall (T-Crypt)
// SPDX-License-Identifier: Apache-2.0

package broker

// Lends. A sized waiter that does not fit beside a serving holder is granted
// beside it anyway: the holder's engine gives the shortfall, keeps serving,
// and takes the memory back once the borrower is gone (docs/design.md). The
// engine's call is the runner's; the broker only decides, asks, and believes a
// card read. One lend is open per holder at a time, and a lease in a lend is
// never parked: one state machine per share.

import (
	"fmt"
	"time"

	"github.com/T-Crypt/berth/internal/gpu"
	"github.com/T-Crypt/berth/internal/proto"
)

// lendState is a lease's place in the lend half of its state machine. The
// lease state stays active throughout: a lending engine serves, and can be
// drained like any other.
type lendState int

const (
	lendNone lendState = iota // nothing lent, no ask open
	lending                   // the runner was asked: the engine has not answered,
	// or its answer is not confirmed by a card read yet
	lent // the engine gave it and a card read shows the memory free, so it
	// counts no more here; with no borrower (lentTo nil) the take-back waits
	returning // the runner was asked to take it back, and has not answered
)

func (s lendState) String() string {
	return [...]string{"none", "lending", "lent", "returning"}[s]
}

// returnRetry is how soon a holder is asked for a take-back again after an
// ask went out or was refused. The engine's "still in use" answer says nothing
// new for a while, so a faster loop is a call storm.
const returnRetry = time.Second

// LendAsk is one lend the broker puts on a holder's runner connection.
type LendAsk struct {
	MiB     uint64 // the shortfall to give
	FreeMiB uint64 // what the driver last read free on that card
	GPU     string // the card's PCI address, "" when no read ever named one
}

// ReturnAsk asks a holder's runner to take one open lend back, naming the
// engine's ticket.
type ReturnAsk struct {
	ID string
}

// Lends delivers the wake for a lend this lease's runner has not picked up.
func (l *Lease) Lends() <-chan struct{} { return l.l.lendSig }

// Returns delivers the wake for a take-back this lease's runner has not
// picked up.
func (l *Lease) Returns() <-chan struct{} { return l.l.returnSig }

// TakeLend is the lend ask waiting on this lease's runner, or nil. The runner
// answers it with Lent or LendFailed on its own connection.
func (l *Lease) TakeLend() *LendAsk {
	l.b.mu.Lock()
	defer l.b.mu.Unlock()
	ask := l.l.lendAsk
	l.l.lendAsk = nil
	return ask
}

// TakeReturn is the take-back ask waiting on this lease's runner, or nil. The
// runner answers it with Returned or ReturnFailed.
func (l *Lease) TakeReturn() *ReturnAsk {
	l.b.mu.Lock()
	defer l.b.mu.Unlock()
	ask := l.l.returnAsk
	l.l.returnAsk = nil
	return ask
}

// Lent records the engine's answer: what it gave, and its ticket. Nothing is
// confirmed here. The lend stops counting against the card only when a card
// read taken after this moment shows the memory free (docs/design.md rule 1).
func (l *Lease) Lent(id string, bytes uint64) {
	b := l.b
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.l.lend != lending || l.l.lentTo == nil {
		return
	}
	l.l.lentID, l.l.lentBytes, l.l.lentRead = id, bytes, b.reads.Load()
	b.log.Info("engine lent", "lease", l.l.id, "engine", l.l.engine, "mib", bytes/gpu.MiB,
		"to", l.l.lentTo.engine, "ticket", id, "waiting", "a card read that shows the memory free")
}

// LendFailed records the runner's answer that nothing was lent. The waiter
// falls through to the park and drain ladder, and the holder keeps its full
// budget (rule 4). A lend the engine may still land (its 503) is a failed lend
// here too: its ticket is asked back, and the re-attach's ledger read
// (lend.Open) is what reconciles it.
func (l *Lease) LendFailed(why string) {
	b := l.b
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.l.lend != lending {
		return
	}
	b.failLend(l.l, why)
	b.schedule(b.now()) // the waiter takes the drain or park ladder now
}

// Returned records that the engine took its memory back: the holder's budget
// is whole again, and the card may promise that space to someone else.
func (l *Lease) Returned() {
	b := l.b
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.l.lend != returning {
		return
	}
	gave := l.l.lentBytes
	clearLendState(l.l)
	b.log.Info("lend returned", "lease", l.l.id, "engine", l.l.engine, "mib", gave/gpu.MiB)
	b.emit(proto.Event{Kind: proto.EvReturned, Lease: l.l.id, Engine: l.l.engine, Bytes: gave,
		Detail: fmt.Sprintf("%s took back the %d MiB it lent", l.l.engine, gave/gpu.MiB)})
	b.schedule(b.now()) // the room is real again: whoever waits on it is told
}

// ReturnFailed records a take-back the engine refused. retry says the holder
// is still lending and Berth asks again on a later tick; without it the holder
// stops counting the memory as given, and the re-attach's ledger read
// (lend.Open) is what reconciles the two.
func (l *Lease) ReturnFailed(why string, retry bool) {
	b := l.b
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.l.lend != returning {
		return
	}
	gave := l.l.lentBytes
	if retry {
		l.l.lend = lent
		b.log.Info("return refused", "lease", l.l.id, "engine", l.l.engine, "mib", gave/gpu.MiB, "why", why)
		return
	}
	id := l.l.lentID
	clearLendState(l.l)
	b.log.Warn("return failed", "lease", l.l.id, "engine", l.l.engine, "mib", gave/gpu.MiB, "why", why)
	b.emit(proto.Event{Kind: proto.EvLendFailed, Lease: l.l.id, Engine: l.l.engine, Bytes: gave,
		Detail: fmt.Sprintf("%s will not give back the %d MiB it lent (ticket %s): %s", l.l.engine, gave/gpu.MiB, id, why)})
	b.schedule(b.now())
}

// lendRoom is step 3 of the make-room ladder (docs/design.md): before a park
// or a drain, ask a serving holder that can give the waiter's shortfall. It
// returns the line the waiter is told and whether to wait at all: wait is
// false when the ladder must run, because no holder here can lend this waiter
// or its lend just failed. drainable is the set a drain would be allowed to
// take, so priorities, holds, turns and juggles still decide whether this
// holder gives room at all; the lend only decides how.
func (b *Broker) lendRoom(w *lease, budget cardBudget, drainable []*lease, now time.Time) (string, bool) {
	if w.needs == 0 {
		return "", false // a whole-card waiter cannot take part of a card
	}
	// The holders can count more than the ceiling: a whole-card holder counts
	// the total, and the baseline, reserve and squatters sit inside it. avail
	// stops at zero, so that overlap is added back, or the lend leaves the
	// waiter short by it once the card confirms it.
	short := sat(w.needs, budget.avail()) + sat(budget.held, budget.ceiling())
	if short == 0 {
		return "", false // the fits-now branch answered already
	}
	for _, h := range drainable {
		if h.lentTo != w {
			continue
		}
		switch h.lend {
		case lending:
			if now.Sub(h.lendAsked) > b.cfg.ReleaseGrace {
				// Rule 4: a lend that never freed the memory falls back.
				b.failLend(h, "the card never showed the memory free")
				return "", false
			}
			return lendWhy(h, w, short), true
		case returning:
			// The memory was given to this waiter and the take-back is open
			// on it; it is coming back, so the waiter waits.
			return lendWhy(h, w, short), true
		}
	}
	for _, h := range drainable {
		if h.lend == lent && h.lentTo == nil {
			// The last borrower is gone and its take-back waits on the room:
			// this waiter is a better reason to take it back now.
			b.wantReturn(h, nil, w, now)
			if now.Sub(h.lendRetry) >= returnRetry {
				return lendWhy(h, w, short), true
			}
			continue
		}
		if h.lend != lendNone || !h.lendCap || h.noLend == w || b.budget(h) < short {
			continue
		}
		b.askLend(h, w, short, now)
		return lendWhy(h, w, short), true
	}
	return "", false
}

// lendWhy is the line a waiter that is waiting on a lend is told.
func lendWhy(h, w *lease, short uint64) string {
	return fmt.Sprintf("waiting for %s to lend %d MiB", h.engine, short/gpu.MiB)
}

// askLend puts a lend on a holder's runner connection and marks the holder as
// lending for w. Its budget stays whole until a card read confirms the lend:
// an engine's word never frees memory (rule 1).
func (b *Broker) askLend(h, w *lease, short uint64, now time.Time) {
	h.lend, h.lentTo, h.lendAsked = lending, w, now
	h.lendAsk = &LendAsk{MiB: (short + gpu.MiB - 1) / gpu.MiB, FreeMiB: b.snap.Free / gpu.MiB, GPU: b.cardAddr}
	select {
	case h.lendSig <- struct{}{}:
	default:
	}
	b.log.Info("lending", "lease", h.id, "engine", h.engine, "mib", short/gpu.MiB, "to", w.engine)
	b.emit(proto.Event{Kind: proto.EvLending, Lease: h.id, Engine: h.engine, Bytes: short,
		Detail: fmt.Sprintf("%s lends %d MiB to %s", h.engine, short/gpu.MiB, w.engine)})
}

// failLend takes a lend that did not land off the holder: its waiter falls
// through to the park and drain ladder, and the holder keeps its full budget
// (rule 4). A ticket the engine did open is asked back; a lend that only timed
// out has nothing to return until the ledger lists it (A2c reconciles that
// through lend.Open).
func (b *Broker) failLend(h *lease, why string) {
	w := h.lentTo
	id, gave := h.lentID, h.lentBytes
	h.noLend = w
	clearLendState(h)
	if id != "" {
		b.askReturn(h, id)
	}
	b.log.Warn("lend failed", "lease", h.id, "engine", h.engine, "to", engineOf(w), "mib", gave/gpu.MiB, "why", why)
	b.emit(proto.Event{Kind: proto.EvLendFailed, Lease: h.id, Engine: h.engine, Bytes: gave,
		Detail: fmt.Sprintf("%s did not lend %d MiB to %s: %s", h.engine, gave/gpu.MiB, engineOf(w), why)})
}

// clearLend takes an open lend off a holder that is draining or releasing:
// its memory goes with it. The engine is never asked to take the memory back,
// because the borrower may still hold it (rule 2) and the engine is stopping.
// A borrower that is still waiting is told by the card, not here: the memory
// it was promised is simply never freed, and it waits as it would behind a
// stowaway.
func (b *Broker) clearLend(h *lease, why string) {
	if h.lend == lendNone {
		return
	}
	gave := h.lentBytes
	clearLendState(h)
	if gave == 0 {
		return
	}
	b.log.Warn("lend dropped", "lease", h.id, "engine", h.engine, "mib", gave/gpu.MiB, "why", why)
	b.emit(proto.Event{Kind: proto.EvLendFailed, Lease: h.id, Engine: h.engine, Bytes: gave,
		Detail: fmt.Sprintf("%s gives up its lend of %d MiB: %s", h.engine, gave/gpu.MiB, why)})
}

// dropLend gives up the lends made for a waiter that left the queue without a
// grant: the holder's take-back is wanted again, and the waiter forgotten, so
// the next waiter may be lent that memory instead.
func (b *Broker) dropLend(w *lease, why string) {
	for _, h := range b.leases {
		if h.lend == lendNone || h.lentTo != w {
			continue
		}
		h.noLend = nil
		if h.lentID == "" {
			// Nothing was given, and the answer to that ask is worthless
			// now: the holder is whole, and the next waiter asks again.
			b.clearLend(h, w.engine+" "+why)
			continue
		}
		// The engine gave (or may yet give) the memory: it is owed back.
		b.wantReturn(h, w, nil, b.now())
		b.log.Info("lend given up", "lease", h.id, "engine", h.engine, "mib", h.lentBytes/gpu.MiB, "to", w.engine, "why", why)
	}
}

// lendFreed is a lease's freed event seen from any holder that lent to it: the
// card shows its memory gone, so each of them may take its own back now
// (rule 2). Called with b.mu held, from Tick and from the immediate frees.
func (b *Broker) lendFreed(w *lease) {
	for _, h := range b.leases {
		if h != w && h.lend == lent && h.lentTo == w {
			b.wantReturn(h, w, nil, b.now())
		}
	}
}

// wantReturn marks that a holder should take its lent memory back, naming the
// lease it is coming back for (next, nil = nobody) and the lease that is
// leaving the card (gone, whose own claim is not counted against the room). The
// ask goes out when the card has room for the memory again: a lease granted
// into that space holds the take-back until it lets go, and the engine is asked
// again on a later tick. A holder whose lend is still unconfirmed is included:
// its ticket is owed back as soon as the engine answered.
func (b *Broker) wantReturn(h *lease, gone, next *lease, now time.Time) {
	if h.lend == lendNone {
		return
	}
	h.lentTo = next
	// A confirmed lend stopped counting against the card, so the room has to
	// be there before the memory moves back: a lease granted into that space
	// holds the take-back until it lets go. An unconfirmed lend never counted,
	// so its ticket is owed back with no room to check.
	if h.lentID == "" || (h.lend != lending && !b.retirable(h, h.lentBytes, gone)) {
		return // nothing to return, or the room is taken: the tick asks again
	}
	if !h.lendRetry.IsZero() && now.Sub(h.lendRetry) < returnRetry {
		return
	}
	h.lend, h.lendRetry = returning, now
	b.askReturn(h, h.lentID)
}

// askReturn puts a take-back on the holder's runner connection.
func (b *Broker) askReturn(h *lease, id string) {
	h.returnAsk = &ReturnAsk{ID: id}
	select {
	case h.returnSig <- struct{}{}:
	default:
	}
}

// lendTick watches the waits a lend can sit in: the card read that confirms
// the memory the engine gave (rule 1), the release grace after which a lend
// that never landed falls back to a drain (rule 4), and a take-back that is
// owed another ask. Called with b.mu held, from Tick.
func (b *Broker) lendTick(key string, h *lease, now time.Time, read uint64) {
	switch h.lend {
	case lending:
		w := h.lentTo
		if w == nil {
			return
		}
		if h.lentID != "" && read > h.lentRead && sat(b.snap.Free, b.cfg.Reserve) >= w.needs {
			h.lend = lent
			b.log.Info("lend confirmed", "lease", key, "engine", h.engine, "mib", h.lentBytes/gpu.MiB, "to", w.engine)
			b.emit(proto.Event{Kind: proto.EvLent, Lease: key, Engine: h.engine, Bytes: h.lentBytes,
				Detail: fmt.Sprintf("%s lent %d MiB to %s; the card shows it free", h.engine, h.lentBytes/gpu.MiB, w.engine)})
			return
		}
		if now.Sub(h.lendAsked) > b.cfg.ReleaseGrace {
			b.failLend(h, "the card did not show the memory free within "+b.cfg.ReleaseGrace.String())
		}
	case returning:
		// An ask that never reached the runner, or a refusal to retry, is due
		// another one once the window passes.
		if h.returnAsk == nil && !h.lendRetry.IsZero() && now.Sub(h.lendRetry) >= returnRetry {
			b.wantReturn(h, nil, h.lentTo, now)
		}
	case lent:
		if h.lentTo == nil {
			b.wantReturn(h, nil, nil, now)
		}
	}
}

// clearLendState leaves the lend half of the machine: no ask open, nothing
// counted as given, no borrower. The waiter a lend failed for (noLend) belongs
// to the caller that failed it, and the engine's ticket is the caller's to ask
// back.
func clearLendState(h *lease) {
	h.lend, h.lentID, h.lentBytes, h.lentRead, h.lendAsked = lendNone, "", 0, 0, time.Time{}
	h.lentTo, h.lendAsk, h.returnAsk = nil, nil, nil
}

// heldWith is what the card would count with give bytes back in their holder's
// hands and minus gone from it: the state the card is moving to, not the one it
// is in now.
func (b *Broker) heldWith(give uint64, minus *lease) uint64 {
	var held uint64
	for _, l := range b.leases {
		if l != minus {
			held += b.budget(l)
		}
	}
	return held + give
}

// retirable is whether the card can take a holder's lent memory back: the
// ceiling still covers what the card would count once it is back, with gone off
// it. This is rule 2's no-overlap pointed the other way: Berth never hands a
// holder memory that another lease was granted into. A card with no measurement
// has nothing to check. The holder's own count stops at the ceiling: a
// whole-card holder counts the total, which already covers the baseline and
// reserve, so only the other leases can take its room away.
func (b *Broker) retirable(h *lease, give uint64, gone *lease) bool {
	if b.snap.Total == 0 {
		return true
	}
	ceiling := sat(b.snap.Total, b.cfg.Reserve+b.snap.Baseline()+b.squatBytes()+b.claimedBytes())
	own := b.budget(h)
	others := sat(b.heldWith(0, gone), own)
	return ceiling >= others+min(own+give, ceiling)
}

// engineOf names a lease's engine, or "" for a lease that is already gone.
func engineOf(l *lease) string {
	if l == nil {
		return ""
	}
	return l.engine
}
