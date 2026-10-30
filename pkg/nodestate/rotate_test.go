package nodestate_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestRotation_IORing bounds the file. The I/O log is diagnostic, so the
// oldest entries are the ones to drop.
func TestRotation_IORing(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 150; i++ {
		doc.AppendIO(nodestate.IOEntry{
			TS:     base.Add(time.Duration(i) * time.Second),
			Op:     "write",
			Target: fmt.Sprintf("entry-%d", i),
			OK:     true,
		})
	}

	nodestate.DefaultRetention().Apply(doc, base.Add(time.Hour))

	if len(doc.IO) != 100 {
		t.Fatalf("got %d I/O entries, want 100", len(doc.IO))
	}
	if doc.IO[0].Target != "entry-50" {
		t.Errorf("oldest surviving entry is %q, want entry-50", doc.IO[0].Target)
	}
	if doc.IO[99].Target != "entry-149" {
		t.Errorf("newest surviving entry is %q, want entry-149", doc.IO[99].Target)
	}
}

// TestRotation_RebootsByAgeOnly is the rule that stops a looping node from
// earning a fresh budget: reboot records age out, they are never trimmed by
// count.
func TestRotation_RebootsByAgeOnly(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 7, 11, 0, 0, 0, 0, time.UTC)
	doc := nodestate.NewDocument(nodestate.ComponentSBR)

	// 50 reboots spread over 10 days, five per day.
	for day := 0; day < 10; day++ {
		for n := 0; n < 5; n++ {
			// The offset of at least a minute keeps the day 7 entries clear of
			// the cutoff instant, which is itself retained.
			ts := now.Add(-time.Duration(day)*24*time.Hour - time.Duration(n+1)*time.Minute)
			doc.AppendReboot(nodestate.RebootEntry{
				TS:     ts,
				Source: nodestate.ComponentSBR,
				Reason: fmt.Sprintf("day-%d-n-%d", day, n),
			})
		}
	}
	if len(doc.Reboots) != 50 {
		t.Fatalf("setup produced %d reboots", len(doc.Reboots))
	}

	nodestate.DefaultRetention().Apply(doc, now)

	// Days 0..6 are inside the 7 day window: 35 entries, regardless of count.
	if len(doc.Reboots) != 35 {
		t.Fatalf("got %d reboots, want 35", len(doc.Reboots))
	}
	cutoff := now.Add(-nodestate.DefaultRebootRetention)
	for _, r := range doc.Reboots {
		if r.TS.Before(cutoff) {
			t.Errorf("entry %q at %v is older than the cutoff %v", r.Reason, r.TS, cutoff)
		}
	}
	if !sortedByTime(doc.Reboots) {
		t.Error("reboot entries are not in ascending time order")
	}
}

func TestRotation_RebootsAreNotTrimmedByCount(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 7, 11, 0, 0, 0, 0, time.UTC)
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 500; i++ {
		doc.AppendReboot(nodestate.RebootEntry{TS: now.Add(-time.Duration(i) * time.Minute)})
	}

	nodestate.DefaultRetention().Apply(doc, now)

	if len(doc.Reboots) != 500 {
		t.Fatalf("got %d reboots, want all 500: trimming by count would hand a looping node a fresh budget", len(doc.Reboots))
	}
}

func TestRotation_RebootsHaveASafetyCeiling(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 7, 11, 0, 0, 0, 0, time.UTC)
	policy := nodestate.DefaultRetention()
	policy.MaxRebootEntries = 10

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 25; i++ {
		doc.AppendReboot(nodestate.RebootEntry{TS: now.Add(-time.Duration(i) * time.Minute), Reason: fmt.Sprint(i)})
	}

	policy.Apply(doc, now)

	if len(doc.Reboots) != 10 {
		t.Fatalf("got %d reboots, want 10", len(doc.Reboots))
	}
	if doc.Reboots[9].Reason != "0" {
		t.Errorf("the most recent entry did not survive; newest is %q", doc.Reboots[9].Reason)
	}

	// The ceiling dropped the records from 10 minutes ago and earlier, so the
	// history answers a 9 minute question and not a 20 minute one.
	oldestKept := now.Add(-9 * time.Minute)
	if !doc.RebootHistoryCompleteSince.Equal(oldestKept) {
		t.Errorf("RebootHistoryCompleteSince = %v, want the oldest surviving record at %v",
			doc.RebootHistoryCompleteSince, oldestKept)
	}
	if nodestate.RebootHistoryComplete(now.Add(-20*time.Minute), doc) {
		t.Error("a window reaching past the truncation reported as complete; a budget computed from it would under-count")
	}
	if !nodestate.RebootHistoryComplete(now.Add(-5*time.Minute), doc) {
		t.Error("a window inside the surviving history reported as incomplete")
	}
}

// TestRotation_HistoryHorizonSelfClears is the reason the completeness point
// is a time and not a flag. A node that once overflowed the ceiling must not
// be marked untrustworthy for the rest of its life: as the window moves
// forward past the gap, the same history becomes a complete answer again.
func TestRotation_HistoryHorizonSelfClears(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 7, 11, 0, 0, 0, 0, time.UTC)
	policy := nodestate.DefaultRetention()
	policy.MaxRebootEntries = 5

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 20; i++ {
		doc.AppendReboot(nodestate.RebootEntry{TS: now.Add(-time.Duration(i) * time.Hour)})
	}
	policy.Apply(doc, now)

	window := now.Add(-10 * time.Hour)
	if nodestate.RebootHistoryComplete(window, doc) {
		t.Fatal("the overflowing history reported as complete")
	}

	// A day later the same document answers a ten hour question exactly: the
	// records it is missing are all older than the window.
	later := now.Add(24 * time.Hour)
	if !nodestate.RebootHistoryComplete(later.Add(-10*time.Hour), doc) {
		t.Error("the horizon did not clear as the window moved past it")
	}
}

func TestRebootHistoryComplete_NilAndEmpty(t *testing.T) {
	t.Parallel()

	at := time.Date(2024, 7, 11, 0, 0, 0, 0, time.UTC)

	if !nodestate.RebootHistoryComplete(at) {
		t.Error("no documents reported as incomplete")
	}
	if !nodestate.RebootHistoryComplete(at, nil, nil) {
		t.Error("an absent peer reported as incomplete; it may simply never have run")
	}

	partial := nodestate.NewDocument(nodestate.ComponentSNR)
	partial.RebootHistoryCompleteSince = at.Add(time.Hour)
	full := nodestate.NewDocument(nodestate.ComponentSBR)

	if nodestate.RebootHistoryComplete(at, full, partial) {
		t.Error("a merged history reported as complete when one input is not; it is only as complete as its worst input")
	}
}

func TestRotation_ConditionsAreCapped(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 7, 11, 0, 0, 0, 0, time.UTC)
	policy := nodestate.DefaultRetention()
	policy.MaxConditions = 3

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 10; i++ {
		doc.SetCondition(nodestate.ConditionEntry{
			Type:       fmt.Sprintf("Device%dUnhealthy", i),
			Status:     "True",
			ObservedAt: now.Add(time.Duration(i) * time.Minute),
		})
	}

	policy.Apply(doc, now)

	if len(doc.Conditions) != 3 {
		t.Fatalf("got %d conditions, want 3", len(doc.Conditions))
	}
	// The three most recently observed survive, in their original order.
	for i, want := range []string{"Device7Unhealthy", "Device8Unhealthy", "Device9Unhealthy"} {
		if doc.Conditions[i].Type != want {
			t.Errorf("conditions[%d] = %q, want %q", i, doc.Conditions[i].Type, want)
		}
	}
}

func TestRotation_EventsAndConditions(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 150; i++ {
		doc.AppendEvent(nodestate.EventEntry{TS: now.Add(time.Duration(i) * time.Second), Reason: fmt.Sprint(i)})
	}
	for i := 0; i < 5; i++ {
		doc.SetCondition(nodestate.ConditionEntry{Type: fmt.Sprintf("Type%d", i), Status: "True", ObservedAt: now})
	}

	nodestate.DefaultRetention().Apply(doc, now)

	if len(doc.Events) != nodestate.DefaultMaxEventEntries {
		t.Errorf("got %d events, want %d", len(doc.Events), nodestate.DefaultMaxEventEntries)
	}
	if doc.Events[0].Reason != "50" {
		t.Errorf("oldest surviving event = %q, want 50", doc.Events[0].Reason)
	}
	if len(doc.Conditions) != 5 {
		t.Errorf("got %d conditions, want 5: conditions are keyed by type and must not be trimmed by age", len(doc.Conditions))
	}
}

func TestRotation_ZeroMeansUnlimited(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	policy := nodestate.RetentionPolicy{} // every limit zero

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 200; i++ {
		doc.AppendIO(nodestate.IOEntry{TS: now})
		doc.AppendEvent(nodestate.EventEntry{TS: now})
		doc.AppendReboot(nodestate.RebootEntry{TS: now.Add(-10000 * time.Hour)})
	}

	policy.Apply(doc, now)

	if len(doc.IO) != 200 || len(doc.Events) != 200 || len(doc.Reboots) != 200 {
		t.Errorf("a zero-valued policy trimmed something: io=%d events=%d reboots=%d",
			len(doc.IO), len(doc.Events), len(doc.Reboots))
	}
}

func TestRotation_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  nodestate.RetentionPolicy
		wantErr bool
	}{
		{"default", nodestate.DefaultRetention(), false},
		{"zero", nodestate.RetentionPolicy{}, false},
		{"negative io", nodestate.RetentionPolicy{MaxIOEntries: -1}, true},
		{"negative events", nodestate.RetentionPolicy{MaxEventEntries: -1}, true},
		{"negative reboots", nodestate.RetentionPolicy{MaxRebootEntries: -1}, true},
		{"negative age", nodestate.RetentionPolicy{RebootMaxAge: -time.Second}, true},
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

func TestRotation_NilDocumentIsANoOp(t *testing.T) {
	t.Parallel()
	nodestate.DefaultRetention().Apply(nil, time.Now()) // must not panic
}

func sortedByTime(entries []nodestate.RebootEntry) bool {
	for i := 1; i < len(entries); i++ {
		if entries[i].TS.Before(entries[i-1].TS) {
			return false
		}
	}
	return true
}
