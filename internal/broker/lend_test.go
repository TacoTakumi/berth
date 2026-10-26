// Copyright 2026 Trevin Tindall (T-Crypt)
// SPDX-License-Identifier: Apache-2.0

package broker

// Lends, driven through the lease table and the fake card: a sized waiter
// granted beside a whole-card holder that lends the shortfall, the driver read
// that confirms it (rule 1), the fall back to a drain when the lend fails or
// never lands (rule 4), the take-back once the borrower's memory is gone and
// the retry after a refusal (rule 2), and the holders that are never asked at
// all. The engine's call is the runner's; these tests answer as a runner
// would, through the same exported methods the daemon drives.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/T-Crypt/berth/internal/gpu"
	"github.com/T-Crypt/berth/internal/procs"
	"github.com/T-Crypt/berth/internal/proto"
)

// lending setup: a whole-card holder with lend calls, serving 22 GiB of a
// 24 GiB card, and a foreground waiter for 4 GiB that does not fit.
func lendTree(holder, holderProc, borrow, borrowProc int) procs.Fake {
	return procs.Fake{
		Parents: map[int]int{holderProc: holder, borrowProc: borrow, holder: 1, borrow: 1},
		Starts:  map[int]uint64{holder: 10, holderProc: 11, borrow: 20, borrowProc: 21},
	}
}

// serving takes a lease through its grant and the card read that measures its
// footprint.
func serving(t *testing.T, b *Broker, c *card, engine string, pid, proc int, used uint64) *Lease {
	t.Helper()
	l := must(t, acquire(b, Request{Engine: engine, Lend: true}))
	l.Attach(pid)
	c.set(gpu.Proc{PID: proc, Name: engine, Used: used})
	b.Tick(context.Background())
	return l
}

// takeLend waits for the broker's lend ask on a lease.
func takeLend(t *testing.T, l *Lease) LendAsk {
	t.Helper()
	select {
	case <-l.Lends():
		if ask := l.TakeLend(); ask != nil {
			return *ask
		}
	case <-time.After(time.Second):
	}
	t.Fatalf("%s was never asked to lend (last wait: %q)", l.l.engine, lastWaiting(l.b))
	return LendAsk{}
}

// takeReturn waits for the broker's take-back ask on a lease.
func takeReturn(t *testing.T, l *Lease) ReturnAsk {
	t.Helper()
	select {
	case <-l.Returns():
		if ask := l.TakeReturn(); ask != nil {
			return *ask
		}
	case <-time.After(time.Second):
	}
	t.Fatalf("%s was never asked to take a lend back", l.l.engine)
	return ReturnAsk{}
}

// noDrain fails when the lease is asked to drain: a lending holder keeps
// serving, and a lend that is open is not its way out.
func noDrain(t *testing.T, l *Lease) {
	t.Helper()
	select {
	case why := <-l.Drains():
		t.Fatalf("%s was asked to drain: %s", l.l.engine, why)
	case <-time.After(50 * time.Millisecond):
	}
}

// event is the newest event of one kind, and fails the test when none names
// it.
func event(t *testing.T, b *Broker, kind string) proto.Event {
	t.Helper()
	evs, _, _ := b.Events(0, false)
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == kind {
			return evs[i]
		}
	}
	t.Fatalf("no %s event in %v", kind, evs)
	return proto.Event{}
}

// statusOf is one lease's row in the broker's view.
func statusOf(t *testing.T, b *Broker, engine string) proto.Lease {
	t.Helper()
	for _, l := range b.Status().Leases {
		if l.Engine == engine {
			return l
		}
	}
	t.Fatalf("no lease for %s in status", engine)
	return proto.Lease{}
}

// A sized waiter that does not fit beside a whole-card holder is granted the
// shortfall from it: the ask names the shortfall, the grant waits for a card
// read newer than the engine's answer, and the holder is never drained.
func TestSizedWaiterIsGrantedByALend(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	b.cardAddr = "0000:09:00.0" // the address --gpu would have named
	holder := serving(t, b, c, "strata", 100, 101, 22*GiB)

	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 4 * GiB})
	pending(t, w)

	// The card has 2 GiB free and the waiter needs 4, so the shortfall is 4.
	if got, want := takeLend(t, holder), (LendAsk{MiB: 4096, FreeMiB: 2048, GPU: "0000:09:00.0"}); got != want {
		t.Fatalf("lend ask = %+v, want %+v", got, want)
	}
	if got := event(t, b, proto.EvLending); got.Bytes != 4*GiB ||
		got.Detail != "strata lends 4096 MiB to ninfer" {
		t.Fatalf("lending event = %+v", got)
	}
	noDrain(t, holder)

	// The engine's word is not a grant: the waiter stays queued on a card
	// that still reads 22 GiB held (rule 1).
	holder.Lent("t1", 4<<30)
	b.Tick(context.Background())
	pending(t, w)
	if why := lastWaiting(b); !strings.Contains(why, "waiting for strata to lend 4096 MiB") {
		t.Fatalf("wait reason = %q", why)
	}

	// The holder shrank its cache, and the next read shows the memory free.
	c.set(gpu.Proc{PID: 101, Name: "strata", Used: 18 * GiB})
	b.Tick(context.Background())
	borrower := granted(t, b, "ninfer", w)
	if got := event(t, b, proto.EvLent); got.Bytes != 4<<30 ||
		got.Detail != "strata lent 4096 MiB to ninfer; the card shows it free" {
		t.Fatalf("lent event = %+v", got)
	}
	noDrain(t, holder)
	if got := statusOf(t, b, "strata"); got.State != "active" {
		t.Fatalf("holder state = %s, want active: a lending engine keeps serving", got.State)
	}

	// The borrower runs beside it, and what the holder still counts against
	// the card is what the driver measures under it.
	borrower.Attach(200)
	c.set(gpu.Proc{PID: 101, Name: "strata", Used: 18 * GiB}, gpu.Proc{PID: 201, Name: "ninfer", Used: 4 * GiB})
	b.Tick(context.Background())
	if got := statusOf(t, b, "strata"); got.State != "active" || got.Used != 18*GiB {
		t.Fatalf("holder row = %+v, want active and holding 18 GiB", got)
	}
}

// A whole-card holder on a card with a baseline (a display, a driver's own
// memory) is asked for the waiter's need plus that baseline: the baseline was
// never the holder's to count, so a lend of the need alone leaves the waiter
// short by it, and the waiter is granted without a drain.
func TestLendOnACardWithABaselineCoversIt(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "maya", 100, 101, 22*GiB)
	c.used = 22*GiB + 512*gpu.MiB // 512 MiB that no process holds
	b.Tick(context.Background())

	w := acquire(b, Request{Engine: "voice", Priority: Foreground, Needs: 4 * GiB})
	pending(t, w)

	if got, want := takeLend(t, holder), (LendAsk{MiB: 4608, FreeMiB: 1536}); got != want {
		t.Fatalf("lend ask = %+v, want %+v", got, want)
	}

	holder.Lent("t1", 4608*gpu.MiB)
	c.used = 18 * GiB // the holder at 17.5 GiB, plus the baseline
	c.set(gpu.Proc{PID: 101, Name: "maya", Used: 17*GiB + 512*gpu.MiB})
	b.Tick(context.Background())
	borrower := granted(t, b, "voice", w)
	noDrain(t, holder)
	if got := statusOf(t, b, "maya"); got.State != "active" {
		t.Fatalf("holder state = %s, want active: a lending engine keeps serving", got.State)
	}

	// The borrower runs and goes. The whole-card holder's own count already
	// covers the baseline, so the take-back is asked once the borrower's
	// memory is gone, with nothing else on the card.
	borrower.Attach(200)
	c.used = 22 * GiB
	c.set(gpu.Proc{PID: 101, Name: "maya", Used: 17*GiB + 512*gpu.MiB}, gpu.Proc{PID: 201, Name: "voice", Used: 4 * GiB})
	b.Tick(context.Background())
	borrower.Release()
	c.used = 18 * GiB
	c.set(gpu.Proc{PID: 101, Name: "maya", Used: 17*GiB + 512*gpu.MiB})
	b.Tick(context.Background())
	if got, want := takeReturn(t, holder), (ReturnAsk{ID: "t1"}); got != want {
		t.Fatalf("return ask = %+v, want %+v", got, want)
	}
}

// A lend the engine refuses falls through to a drain of that same holder,
// once, and the waiter is told it is draining.
func TestRefusedLendFallsThroughToADrain(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "maya", 100, 101, 22*GiB)

	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 4 * GiB})
	pending(t, w)
	takeLend(t, holder)

	holder.LendFailed("the engine can lend at most 1024 MiB")
	if why := drained(t, holder); !strings.Contains(why, "ninfer") {
		t.Fatalf("drain reason %q does not name the requester", why)
	}
	if got := event(t, b, proto.EvLendFailed); !strings.Contains(got.Detail, "at most 1024 MiB") {
		t.Fatalf("lend_failed event = %+v", got)
	}
	if ask := holder.TakeLend(); ask != nil {
		t.Fatalf("a refused holder was asked again: %+v", ask)
	}
	if why := lastWaiting(b); !strings.Contains(why, "draining maya") {
		t.Fatalf("wait reason = %q, want the drain of the holder", why)
	}
	pending(t, w)

	// The drain is the runner's to run: its engine stops, the memory leaves
	// the card, and the waiter is granted. Nothing of the lend is owed back.
	if got := statusOf(t, b, "maya"); got.State != "draining" {
		t.Fatalf("holder state = %s, want draining", got.State)
	}
	holder.Release()
	c.set()
	b.Tick(context.Background())
	granted(t, b, "ninfer", w)
}

// A lend that is never confirmed within the release grace falls back to the
// drain, with the why on the event stream.
func TestUnconfirmedLendFallsBackAfterTheGrace(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "strata", 100, 101, 22*GiB)

	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 4 * GiB})
	pending(t, w)
	takeLend(t, holder)

	// The runner answers that it lent, but the card never shows the memory
	// free, and the grace passes.
	holder.Lent("t1", 4<<30)
	b.nowFn = func() time.Time { return time.Now().Add(2 * time.Hour) }
	b.Tick(context.Background())

	if why := drained(t, holder); !strings.Contains(why, "ninfer") {
		t.Fatalf("drain reason %q does not name the requester", why)
	}
	got := event(t, b, proto.EvLendFailed)
	if !strings.Contains(got.Detail, "the card did not show the memory free within 1h0m0s") {
		t.Fatalf("lend_failed event = %+v, want the grace in its detail", got)
	}
	if got := statusOf(t, b, "strata"); got.State != "draining" {
		t.Fatalf("holder state = %s, want draining", got.State)
	}
	pending(t, w)
}

// The take-back waits for the borrower's memory to be gone (rule 2), a
// refusal to retry keeps the memory lent and asks again on a later tick, and
// a completed take-back makes the whole card countable again.
func TestLendReturnsWhenTheBorrowerIsGone(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "strata", 100, 101, 22*GiB)
	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 4 * GiB})
	takeLend(t, holder)
	holder.Lent("t1", 4<<30)
	c.set(gpu.Proc{PID: 101, Name: "strata", Used: 18 * GiB})
	b.Tick(context.Background())
	borrower := granted(t, b, "ninfer", w)
	borrower.Attach(200)
	c.set(gpu.Proc{PID: 101, Name: "strata", Used: 18 * GiB}, gpu.Proc{PID: 201, Name: "ninfer", Used: 4 * GiB})
	b.Tick(context.Background())

	// A release is not a take-back: the borrower's memory is still on the
	// card, so the holder keeps its reduced budget and is not asked yet.
	borrower.Release()
	b.Tick(context.Background())
	select {
	case <-holder.Returns():
		t.Fatal("the take-back was asked before the borrower's memory was gone")
	case <-time.After(50 * time.Millisecond):
	}

	// The driver shows the borrower gone: now the holder is asked, by ticket.
	c.set(gpu.Proc{PID: 101, Name: "strata", Used: 18 * GiB})
	b.Tick(context.Background())
	if got, want := takeReturn(t, holder), (ReturnAsk{ID: "t1"}); got != want {
		t.Fatalf("return ask = %+v, want %+v", got, want)
	}

	// The engine refuses: the memory is still in use. It stays lent, so a
	// waiter may still be granted that space, and the ask comes again once
	// the retry window passes.
	holder.ReturnFailed("the lent memory is still in use", true)
	b.nowFn = func() time.Time { return time.Now().Add(2 * time.Second) }
	b.Tick(context.Background())
	if got, want := takeReturn(t, holder), (ReturnAsk{ID: "t1"}); got != want {
		t.Fatalf("retry ask = %+v, want %+v", got, want)
	}

	// The engine takes it back: the whole card counts again, so the next
	// sized waiter has to be lent to afresh.
	holder.Returned()
	if got := event(t, b, proto.EvReturned); got.Bytes != 4<<30 ||
		got.Detail != "strata took back the 4096 MiB it lent" {
		t.Fatalf("returned event = %+v", got)
	}
	next := acquire(b, Request{Engine: "other", Priority: Foreground, Needs: 4 * GiB})
	pending(t, next)
	if got, want := takeLend(t, holder), (LendAsk{MiB: 4096, FreeMiB: 6144, GPU: ""}); got != want {
		t.Fatalf("next lend ask = %+v, want %+v", got, want)
	}
}

// A waiter that gives up while a lend is open hands the memory back: the
// holder is asked for it, and while its borrower still waits it is not.
func TestGiveUpReturnsAnOpenLend(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "strata", 100, 101, 22*GiB)

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan result, 1)
	go func() {
		l, err := b.Acquire(ctx, Request{Engine: "ninfer", Priority: Foreground, Needs: 4 * GiB}, nil)
		ch <- result{l, err}
	}()
	pending(t, ch)
	takeLend(t, holder)
	holder.Lent("t1", 4<<30)

	// The lend is open for this waiter, so nothing is owed back yet.
	if ask := holder.TakeReturn(); ask != nil {
		t.Fatalf("the take-back was asked while the borrower still waits: %+v", ask)
	}

	// It gives up, and the holder is asked for its memory.
	cancel()
	if r := <-ch; r.err == nil {
		t.Fatalf("the waiter was granted after its context ended: %+v", r.l)
	}
	if got, want := takeReturn(t, holder), (ReturnAsk{ID: "t1"}); got != want {
		t.Fatalf("return ask = %+v, want %+v", got, want)
	}
}

// A run that did not declare lend calls is never asked for one: today's
// ladder drains it, and a pinned hold is not even a candidate.
func TestHolderWithoutTheCapabilityIsDrained(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := must(t, acquire(b, Request{Engine: "ninfer"}))
	holder.Attach(100)
	c.set(gpu.Proc{PID: 101, Name: "ninfer-serve", Used: 22 * GiB})
	b.Tick(context.Background())

	acquire(b, Request{Engine: "strata", Priority: Foreground, Needs: 4 * GiB})
	if why := drained(t, holder); !strings.Contains(why, "strata") {
		t.Fatalf("drain reason %q does not name the requester", why)
	}
	if ask := holder.TakeLend(); ask != nil {
		t.Fatalf("a run without lend calls was asked to lend: %+v", ask)
	}
	select {
	case <-holder.Lends():
		t.Fatal("a run without the capability got a lend ask")
	default:
	}
}

// A lend is asked only where a drain would be allowed: a pinned hold keeps
// its card, lends nothing, and the waiter is told who holds it.
func TestAPinnedHoldIsNeverAskedToLend(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := must(t, acquire(b, Request{Engine: "game", Priority: Hold, Pinned: true, Lend: true}))
	holder.Attach(100)
	c.set(gpu.Proc{PID: 101, Name: "game", Used: 22 * GiB})
	b.Tick(context.Background())

	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 4 * GiB})
	pending(t, w)
	if ask := holder.TakeLend(); ask != nil {
		t.Fatalf("a pinned hold was asked to lend: %+v", ask)
	}
	noDrain(t, holder)
	if why := lastWaiting(b); !strings.Contains(why, "held by game") {
		t.Fatalf("wait reason = %q, want the hold named", why)
	}
}

// The ask covers the whole shortfall: a part of a MiB rounds up, so the
// engine never gives less than the waiter needs and the lend can confirm.
func TestLendAskRoundsTheShortfallUp(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "strata", 100, 101, 22*GiB)
	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 4*GiB + 512})
	pending(t, w)
	if got := takeLend(t, holder).MiB; got != 4097 {
		t.Fatalf("lend ask = %d MiB, want 4097: 4 GiB and 512 bytes", got)
	}
}

// An engine that reports more lent than its whole budget leaves it counting
// what the driver measures, never a wrapped-around figure.
func TestLentMoreThanTheBudgetCountsTheReading(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "maya", 100, 101, 6*GiB)
	holder.l.lend, holder.l.lentBytes = lent, 30*GiB
	b.mu.Lock()
	got := b.budget(holder.l)
	b.mu.Unlock()
	if got != 6*GiB {
		t.Fatalf("budget = %d MiB, want 6144: the driver's reading", got/gpu.MiB)
	}
}

// A holder that drains while it lends is not asked to take the memory back:
// its engine is stopping, and the borrower still holds what it was lent
// (rule 2).
func TestDrainingLenderIsNotAskedToTakeBack(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	holder := serving(t, b, c, "strata", 100, 101, 22*GiB)
	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 4 * GiB})
	takeLend(t, holder)
	holder.Lent("t1", 4<<30)
	c.set(gpu.Proc{PID: 101, Name: "strata", Used: 18 * GiB})
	b.Tick(context.Background())
	borrower := granted(t, b, "ninfer", w)
	borrower.Attach(200)
	c.set(gpu.Proc{PID: 101, Name: "strata", Used: 18 * GiB}, gpu.Proc{PID: 201, Name: "ninfer", Used: 4 * GiB})
	b.Tick(context.Background())

	// A whole-card request the holder must make room for drains it.
	acquire(b, Request{Engine: "game", Priority: Foreground})
	drained(t, holder)
	select {
	case <-holder.Returns():
		t.Fatalf("a draining lender was asked to take back: %+v", holder.TakeReturn())
	case <-time.After(50 * time.Millisecond):
	}
}

// A holder that re-attaches naming its open lend comes back `lent`: the new
// daemon counts its budget less the lend, and the take-back is asked only
// once the card has room for the memory again (rule 2), the same wait the
// state already knows.
func TestReattachRestoresAnOpenLend(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	l, err := b.Reattach(Request{Engine: "maya", Lend: true, Needs: 20 * GiB, LendID: "t1", LentBytes: 8 * GiB}, 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if l.l.lend != lent || l.l.lentID != "t1" || l.l.lentBytes != 8*GiB || l.l.lentTo != nil {
		t.Fatalf("re-attached lend = state %v ticket %q bytes %d borrower %v, want lent, t1, 8 GiB, no borrower",
			l.l.lend, l.l.lentID, l.l.lentBytes, l.l.lentTo)
	}

	// A lease standing on the lent space holds the take-back: the first
	// reads after the restart show no room to bring the 8 GiB back.
	w := acquire(b, Request{Engine: "ninfer", Priority: Foreground, Needs: 8 * GiB})
	borrower := must(t, w)
	borrower.Attach(200)
	c.set(gpu.Proc{PID: 101, Name: "maya", Used: 10 * GiB}, gpu.Proc{PID: 201, Name: "ninfer", Used: 8 * GiB})
	b.Tick(context.Background())
	b.mu.Lock()
	got := b.budget(l.l)
	b.mu.Unlock()
	if got != 12*GiB {
		t.Fatalf("holder budget = %d MiB, want 12288: its 20 GiB less the 8 GiB it lent", got/gpu.MiB)
	}
	if l.l.lend != lent {
		t.Fatalf("the re-attached lend moved to %v while the card had no room", l.l.lend)
	}
	select {
	case <-l.Returns():
		t.Fatalf("the take-back was asked while the card had no room: %+v", l.TakeReturn())
	case <-time.After(50 * time.Millisecond):
	}

	// The borrower is gone and its lease is off the table: the card has
	// room, and the new daemon asks for the memory back, naming the ticket
	// it restored.
	borrower.Release()
	c.set(gpu.Proc{PID: 101, Name: "maya", Used: 10 * GiB})
	b.Tick(context.Background()) // the card shows the borrower's memory gone
	b.Tick(context.Background()) // the lease is gone from the table too
	if got, want := takeReturn(t, l), (ReturnAsk{ID: "t1"}); got != want {
		t.Fatalf("return ask = %+v, want %+v", got, want)
	}
}

// A re-attach that names no lend is today's: the holder counts its whole
// budget, exactly as before the lend existed.
func TestReattachWithoutALendCountsTheWholeBudget(t *testing.T) {
	b, c, _ := setup(t, lendTree(100, 101, 200, 201))
	l, err := b.Reattach(Request{Engine: "maya", Lend: true, Needs: 20 * GiB}, 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if l.l.lend != lendNone || l.l.lentID != "" || l.l.lentBytes != 0 {
		t.Fatalf("re-attached lend = state %v ticket %q bytes %d, want none", l.l.lend, l.l.lentID, l.l.lentBytes)
	}
	c.set(gpu.Proc{PID: 101, Name: "maya", Used: 10 * GiB})
	b.Tick(context.Background())
	b.mu.Lock()
	got := b.budget(l.l)
	b.mu.Unlock()
	if got != 20*GiB {
		t.Fatalf("holder budget = %d MiB, want 20480: its whole budget with no lend", got/gpu.MiB)
	}
}
