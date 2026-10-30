package nodestate_test

import (
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

var mergeBase = time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)

// TestMergeReboots_OneRemediationCountsOnce is the reason both files are read.
// An SNR software reboot and an SBR watchdog release for the same remediation
// are one reboot, not two.
func TestMergeReboots_OneRemediationCountsOnce(t *testing.T) {
	t.Parallel()

	ref := &nodestate.CRRef{Kind: "SelfNodeRemediation", Name: "worker-0", UID: "uid-1", Generation: 2}

	snr := nodestate.NewDocument(nodestate.ComponentSNR)
	snr.AppendReboot(nodestate.RebootEntry{
		TS: mergeBase, Source: nodestate.ComponentSNR, Method: nodestate.MethodSoftware,
		Remediation: ref, Completed: false,
	})

	sbr := nodestate.NewDocument(nodestate.ComponentSBR)
	sbr.AppendReboot(nodestate.RebootEntry{
		TS: mergeBase.Add(time.Second), Source: nodestate.ComponentSBR, Method: nodestate.MethodWatchdog,
		Remediation: ref, Completed: true,
	})

	merged := nodestate.MergeReboots(snr, sbr)
	if len(merged) != 1 {
		t.Fatalf("got %d merged entries, want 1: %+v", len(merged), merged)
	}
	if !merged[0].Completed {
		t.Error("the merged entry must carry the completed flag from whichever writer observed it")
	}
	if !merged[0].TS.Equal(mergeBase) {
		t.Errorf("merged TS = %v, want the earliest observation %v", merged[0].TS, mergeBase)
	}
	if merged[0].Method != nodestate.MethodWatchdog {
		t.Errorf("Method = %q, want the method that actually took effect", merged[0].Method)
	}
}

func TestMergeReboots_DifferentGenerationsAreDifferentReboots(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.AppendReboot(nodestate.RebootEntry{
		TS: mergeBase, Remediation: &nodestate.CRRef{UID: "uid-1", Generation: 1},
	})
	doc.AppendReboot(nodestate.RebootEntry{
		TS: mergeBase.Add(time.Hour), Remediation: &nodestate.CRRef{UID: "uid-1", Generation: 2},
	})

	if got := len(nodestate.MergeReboots(doc)); got != 2 {
		t.Fatalf("got %d entries, want 2", got)
	}
}

func TestMergeReboots_AutonomousEntriesAreNeverCollapsed(t *testing.T) {
	t.Parallel()

	sbr := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 3; i++ {
		sbr.AppendReboot(nodestate.RebootEntry{
			TS:     mergeBase.Add(time.Duration(i) * time.Hour),
			Source: nodestate.ComponentSBR,
			Method: nodestate.MethodWatchdog,
			Reason: "TotalOutage",
		})
	}

	merged := nodestate.MergeReboots(sbr)
	if len(merged) != 3 {
		t.Fatalf("got %d entries, want 3: a reboot with no CR reference is always distinct", len(merged))
	}
}

func TestMergeReboots_IsSortedAndTolerantOfNils(t *testing.T) {
	t.Parallel()

	a := nodestate.NewDocument(nodestate.ComponentSBR)
	a.AppendReboot(nodestate.RebootEntry{TS: mergeBase.Add(2 * time.Hour), Reason: "second"})
	b := nodestate.NewDocument(nodestate.ComponentSNR)
	b.AppendReboot(nodestate.RebootEntry{TS: mergeBase, Reason: "first"})

	merged := nodestate.MergeReboots(nil, a, nil, b, nil)
	if len(merged) != 2 {
		t.Fatalf("got %d entries, want 2", len(merged))
	}
	if merged[0].Reason != "first" || merged[1].Reason != "second" {
		t.Errorf("merged order = %q, %q", merged[0].Reason, merged[1].Reason)
	}

	if got := nodestate.MergeReboots(); got == nil || len(got) != 0 {
		t.Errorf("MergeReboots() = %v, want an empty non-nil slice", got)
	}
}

func TestMergeReboots_DoesNotMutateItsInputs(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.AppendReboot(nodestate.RebootEntry{
		TS: mergeBase, Completed: false, Remediation: &nodestate.CRRef{UID: "u", Generation: 1},
	})
	other := nodestate.NewDocument(nodestate.ComponentSNR)
	other.AppendReboot(nodestate.RebootEntry{
		TS: mergeBase, Completed: true, Remediation: &nodestate.CRRef{UID: "u", Generation: 1},
	})

	_ = nodestate.MergeReboots(doc, other)

	if doc.Reboots[0].Completed {
		t.Error("MergeReboots mutated an input document")
	}
}

func TestCountReboots(t *testing.T) {
	t.Parallel()

	sbr := nodestate.NewDocument(nodestate.ComponentSBR)
	sbr.AppendReboot(nodestate.RebootEntry{TS: mergeBase.Add(-10 * time.Hour), Method: nodestate.MethodWatchdog})
	sbr.AppendReboot(nodestate.RebootEntry{TS: mergeBase.Add(-1 * time.Hour), Method: nodestate.MethodWatchdog})
	sbr.AppendReboot(nodestate.RebootEntry{
		TS: mergeBase.Add(-30 * time.Minute), Method: nodestate.MethodWatchdog,
		Remediation: &nodestate.CRRef{UID: "uid-9", Generation: 1},
	})

	autonomous := func(r nodestate.RebootEntry) bool { return r.Remediation == nil }
	requested := func(r nodestate.RebootEntry) bool { return r.Remediation != nil }

	if got := nodestate.CountReboots(mergeBase.Add(-2*time.Hour), autonomous, sbr); got != 1 {
		t.Errorf("autonomous count = %d, want 1", got)
	}
	if got := nodestate.CountReboots(mergeBase.Add(-2*time.Hour), requested, sbr); got != 1 {
		t.Errorf("requested count = %d, want 1", got)
	}
	if got := nodestate.CountReboots(mergeBase.Add(-24*time.Hour), nil, sbr); got != 3 {
		t.Errorf("unfiltered count = %d, want 3", got)
	}
	if got := nodestate.CountReboots(mergeBase, nil, sbr); got != 0 {
		t.Errorf("count after the newest entry = %d, want 0", got)
	}
}

func TestFindReboot(t *testing.T) {
	t.Parallel()

	ref := nodestate.CRRef{Kind: "SBR", Name: "worker-0", UID: "uid-1", Generation: 3}

	sbr := nodestate.NewDocument(nodestate.ComponentSBR)
	sbr.AppendReboot(nodestate.RebootEntry{TS: mergeBase, Remediation: &ref, Completed: true})

	got, ok := nodestate.FindReboot(ref, sbr)
	if !ok {
		t.Fatal("FindReboot did not find a recorded remediation")
	}
	if !got.Completed {
		t.Error("the found entry lost its completed flag")
	}

	if _, ok := nodestate.FindReboot(nodestate.CRRef{UID: "uid-1", Generation: 4}, sbr); ok {
		t.Error("FindReboot matched a different generation")
	}
	if _, ok := nodestate.FindReboot(nodestate.CRRef{UID: "other", Generation: 3}, sbr); ok {
		t.Error("FindReboot matched a different UID")
	}
	if _, ok := nodestate.FindReboot(ref); ok {
		t.Error("FindReboot matched with no documents")
	}
}

func TestPendingReboots(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSNR)
	doc.AppendReboot(nodestate.RebootEntry{TS: mergeBase, Completed: true, Reason: "done"})
	doc.AppendReboot(nodestate.RebootEntry{TS: mergeBase.Add(time.Hour), Completed: false, Reason: "pending"})

	pending := nodestate.PendingReboots(doc)
	if len(pending) != 1 || pending[0].Reason != "pending" {
		t.Fatalf("PendingReboots = %+v", pending)
	}
}

func TestCRRef_Key(t *testing.T) {
	t.Parallel()

	a := nodestate.CRRef{Kind: "SBR", Namespace: "ns", Name: "n", UID: "uid-1", Generation: 1}
	b := nodestate.CRRef{Kind: "Other", Namespace: "other", Name: "other", UID: "uid-1", Generation: 1}
	if a.Key() != b.Key() {
		t.Errorf("Key is not UID+generation based: %q vs %q", a.Key(), b.Key())
	}

	c := a
	c.Generation = 2
	if a.Key() == c.Key() {
		t.Error("Key ignores the generation")
	}

	var zero nodestate.CRRef
	if zero.Key() == a.Key() {
		t.Error("the zero reference collides with a real one")
	}
}
