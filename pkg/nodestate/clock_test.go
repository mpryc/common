package nodestate_test

import (
	"context"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// TestClockStepForwardDoesNotEraseHistory is the scenario the design calls
// out: recovery from a total outage is exactly when NTP steps the clock.
//
// The age trim's only input is the wall clock. A step forward makes every
// record look ancient, and erasing them would hand a node that may be in a
// reboot loop a clean budget — the one thing the budget exists to prevent.
func TestClockStepForwardDoesNotEraseHistory(t *testing.T) {
	t.Parallel()

	start := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := nodestatetest.NewFakeClock(start)
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithClock(clk))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := s.Update(ctx, func(d *nodestate.Document) error {
			d.AppendReboot(nodestate.RebootEntry{
				TS: clk.Now(), Source: nodestate.ComponentSBR, Method: nodestate.MethodWatchdog,
			})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Minute)
	}

	// The real time source comes up with no battery-backed clock and jumps
	// five years ahead before NTP corrects it.
	clk.Set(start.AddDate(5, 0, 0))
	if _, err := s.Update(ctx, func(d *nodestate.Document) error {
		d.BootID = "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8"
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	doc, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Reboots) != 3 {
		t.Fatalf("got %d reboots after a five year clock step, want 3: the budget history was erased", len(doc.Reboots))
	}

	// NTP corrects the clock. The history must be usable again, not written
	// off because an earlier pass recorded a horizon in the future.
	clk.Set(start.Add(5 * time.Minute))
	if _, err := s.Update(ctx, func(d *nodestate.Document) error {
		d.BootID = "c4d5e6f7-0819-2a3b-4c5d-6e7f80912a3b"
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	doc, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Reboots) != 3 {
		t.Fatalf("got %d reboots after the clock corrected, want 3", len(doc.Reboots))
	}

	window := clk.Now().Add(-24 * time.Hour)
	if !nodestate.RebootHistoryComplete(window, doc) {
		t.Errorf("a 24 hour window reports incomplete (horizon %v); the node would be treated as "+
			"budget-exhausted for good and would never self-fence again", doc.RebootHistoryCompleteSince)
	}
	if got := nodestate.CountReboots(window, nil, doc); got != 3 {
		t.Errorf("CountReboots = %d, want 3", got)
	}
}

// TestHorizonInTheFutureIsDiscarded covers a file written by a build that had
// the defect above, or by any pass that ran against a wrong clock.
func TestHorizonInTheFutureIsDiscarded(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.RebootHistoryCompleteSince = now.AddDate(5, 0, 0)
	doc.AppendReboot(nodestate.RebootEntry{TS: now.Add(-time.Hour)})

	nodestate.DefaultRetention().Apply(doc, now)

	if doc.RebootHistoryCompleteSince.After(now) {
		t.Fatalf("horizon = %v, still in the future", doc.RebootHistoryCompleteSince)
	}
	if !nodestate.RebootHistoryComplete(now.Add(-24*time.Hour), doc) {
		t.Error("the poisoned horizon did not self-heal")
	}
}

func TestClockGoingBackwardsIsRecorded(t *testing.T) {
	t.Parallel()

	start := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := nodestatetest.NewFakeClock(start)
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithClock(clk))
	ctx := context.Background()

	if _, err := s.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Fatal(err)
	}

	clk.Set(start.Add(-2 * time.Hour))
	doc, err := s.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil })
	if err != nil {
		t.Fatalf("Update with a clock that went backwards: %v", err)
	}

	var found bool
	for _, e := range doc.Events {
		if e.Reason == nodestate.EventReasonClockWentBackwards {
			found = true
			if e.Type != "Warning" {
				t.Errorf("event type = %q, want Warning", e.Type)
			}
		}
	}
	if !found {
		t.Errorf("no %s event in %+v", nodestate.EventReasonClockWentBackwards, doc.Events)
	}
}

func TestRetentionFloor(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	ancient := now.AddDate(-1, 0, 0)

	t.Run("keeps the most recent records whatever their apparent age", func(t *testing.T) {
		t.Parallel()

		policy := nodestate.DefaultRetention()
		policy.MinRebootEntries = 5

		doc := nodestate.NewDocument(nodestate.ComponentSBR)
		for i := 0; i < 20; i++ {
			doc.AppendReboot(nodestate.RebootEntry{TS: ancient.Add(time.Duration(i) * time.Minute)})
		}
		policy.Apply(doc, now)

		if len(doc.Reboots) != 5 {
			t.Fatalf("got %d reboots, want the floor of 5", len(doc.Reboots))
		}
		// The newest five survive.
		if !doc.Reboots[4].TS.Equal(ancient.Add(19 * time.Minute)) {
			t.Errorf("newest surviving record is %v, want %v", doc.Reboots[4].TS, ancient.Add(19*time.Minute))
		}
		// Records were genuinely dropped here, so the horizon must move.
		if !doc.RebootHistoryCompleteSince.Equal(doc.Reboots[0].TS) {
			t.Errorf("horizon = %v, want the oldest retained record %v",
				doc.RebootHistoryCompleteSince, doc.Reboots[0].TS)
		}
	})

	t.Run("does not move the horizon when it saved everything", func(t *testing.T) {
		t.Parallel()

		policy := nodestate.DefaultRetention()
		policy.MinRebootEntries = 16

		doc := nodestate.NewDocument(nodestate.ComponentSBR)
		for i := 0; i < 3; i++ {
			doc.AppendReboot(nodestate.RebootEntry{TS: ancient.Add(time.Duration(i) * time.Minute)})
		}
		policy.Apply(doc, now)

		if len(doc.Reboots) != 3 {
			t.Fatalf("got %d reboots, want all 3", len(doc.Reboots))
		}
		if !doc.RebootHistoryCompleteSince.IsZero() {
			t.Errorf("horizon = %v, want zero: nothing was dropped, so nothing is missing",
				doc.RebootHistoryCompleteSince)
		}
	})

	t.Run("zero disables the floor", func(t *testing.T) {
		t.Parallel()

		policy := nodestate.DefaultRetention()
		policy.MinRebootEntries = 0

		doc := nodestate.NewDocument(nodestate.ComponentSBR)
		for i := 0; i < 3; i++ {
			doc.AppendReboot(nodestate.RebootEntry{TS: ancient.Add(time.Duration(i) * time.Minute)})
		}
		policy.Apply(doc, now)

		if len(doc.Reboots) != 0 {
			t.Fatalf("got %d reboots, want 0 with the floor disabled", len(doc.Reboots))
		}
		if !doc.RebootHistoryCompleteSince.Equal(now) {
			t.Errorf("horizon = %v, want now when every record was dropped", doc.RebootHistoryCompleteSince)
		}
	})

	t.Run("does not bind in normal operation", func(t *testing.T) {
		t.Parallel()

		policy := nodestate.DefaultRetention()
		doc := nodestate.NewDocument(nodestate.ComponentSBR)
		// 40 records over 10 days: 28 are inside the seven day window, which
		// is well above the floor of 16, so the floor changes nothing.
		for day := 0; day < 10; day++ {
			for n := 0; n < 4; n++ {
				doc.AppendReboot(nodestate.RebootEntry{
					TS: now.Add(-time.Duration(day)*24*time.Hour - time.Duration(n+1)*time.Minute),
				})
			}
		}
		policy.Apply(doc, now)

		if len(doc.Reboots) != 28 {
			t.Errorf("got %d reboots, want 28: the floor must not interfere with ordinary ageing", len(doc.Reboots))
		}
	})
}

func TestRetentionPolicy_ValidateFloor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  nodestate.RetentionPolicy
		wantErr bool
	}{
		{"default", nodestate.DefaultRetention(), false},
		{"negative floor", nodestate.RetentionPolicy{MinRebootEntries: -1}, true},
		{"floor above ceiling", nodestate.RetentionPolicy{MinRebootEntries: 100, MaxRebootEntries: 10}, true},
		{"floor with no ceiling", nodestate.RetentionPolicy{MinRebootEntries: 100}, false},
		{"floor equal to ceiling", nodestate.RetentionPolicy{MinRebootEntries: 10, MaxRebootEntries: 10}, false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.policy.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
