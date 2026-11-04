package nodestate_test

import (
	"errors"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

var identityBase = time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC)

// TestUnrelatedRemediationsWithNoUIDAreNotMerged is the under-counting bug.
//
// The old UID/generation key let a zero UID produce the same key for
// every reference and MergeReboots folded them into one record. A consumer
// that filled kind, namespace and name but not UID — the least obvious field
// to populate — would see three reboots counted as one, and the requested
// budget would never engage the brake on a reboot loop.
func TestUnrelatedRemediationsWithNoUIDAreNotMerged(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.AppendReboot(nodestate.RebootEntry{
		TS: identityBase, Reason: "first",
		Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"},
	})
	doc.AppendReboot(nodestate.RebootEntry{
		TS: identityBase.Add(48 * time.Hour), Reason: "second",
		Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"},
	})
	doc.AppendReboot(nodestate.RebootEntry{
		TS: identityBase.Add(96 * time.Hour), Reason: "third",
		Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "StorageBasedRemediation", Name: "worker-0"},
	})

	merged := nodestate.MergeReboots(doc)
	if len(merged) != 3 {
		t.Fatalf("got %d merged entries, want 3: unidentifiable references were collapsed into one", len(merged))
	}
	if got := nodestate.CountReboots(identityBase.Add(-time.Hour), nil, doc); got != 3 {
		t.Errorf("CountReboots = %d, want 3: a budget computed from this would never brake a reboot loop", got)
	}
}

// TestMergeDoesNotCollapseUIDLessReferencesAlreadyOnDisk is the other half of
// the fix, and the half that matters in the field.
//
// AppendReboot strips an unusable reference going forward, but files written
// by an earlier build — or by any other implementation of this format —
// already hold them. MergeReboots has to refuse to de-duplicate on them in
// its own right, so the records are built here directly rather than through
// AppendReboot.
func TestMergeDoesNotCollapseUIDLessReferencesAlreadyOnDisk(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Reboots = []nodestate.RebootEntry{
		{TS: identityBase, Reason: "first",
			Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"}},
		{TS: identityBase.Add(48 * time.Hour), Reason: "second",
			Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"}},
		{TS: identityBase.Add(96 * time.Hour), Reason: "third",
			Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "StorageBasedRemediation", Name: "worker-0"}},
	}

	merged := nodestate.MergeReboots(doc)
	if len(merged) != 3 {
		t.Fatalf("got %d merged entries, want 3: three remediations counted as one", len(merged))
	}
	for i, want := range []string{"first", "second", "third"} {
		if merged[i].Reason != want {
			t.Errorf("merged[%d] = %q, want %q", i, merged[i].Reason, want)
		}
	}
	if got := nodestate.CountReboots(identityBase.Add(-time.Hour), nil, doc); got != 3 {
		t.Errorf("CountReboots = %d, want 3", got)
	}
}

// TestAppendRebootRetainsUnresolvedRequestedReference keeps the bad reference off disk
// in the first place.
func TestAppendRebootRetainsUnresolvedRequestedReference(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSNR)
	doc.AppendReboot(nodestate.RebootEntry{
		TS: identityBase, Reason: "no uid",
		Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"},
	})

	if doc.Reboots[0].Remediation == nil {
		t.Errorf("an unresolved requested reference was discarded: %+v", doc.Reboots[0].Remediation)
	}
	if doc.Reboots[0].Reason != "no uid" {
		t.Error("the record itself was discarded; only the reference should be")
	}

	// A usable reference is kept untouched.
	ref := &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0", UID: "uid-1", Generation: 2}
	doc.AppendReboot(nodestate.RebootEntry{TS: identityBase.Add(time.Hour), Remediation: ref})
	if doc.Reboots[1].Remediation == nil || doc.Reboots[1].Remediation.UID != "uid-1" {
		t.Errorf("a valid reference was dropped: %+v", doc.Reboots[1].Remediation)
	}
}

func TestAppendRebootChecked(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSNR)

	err := doc.AppendRebootChecked(nodestate.RebootEntry{
		TS: identityBase, Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"},
	})
	if !errors.Is(err, nodestate.ErrInvalidReference) {
		t.Fatalf("error = %v, want ErrInvalidReference", err)
	}
	if len(doc.Reboots) != 0 {
		t.Error("a rejected record was still appended")
	}

	if err := doc.AppendRebootChecked(nodestate.RebootEntry{
		TS: identityBase, Remediation: &nodestate.CRRef{NodeUID: "node-uid", UID: "uid-1", Generation: 1},
	}); err != nil {
		t.Fatalf("a valid record was rejected: %v", err)
	}
	// An autonomous reboot has no reference and is always acceptable.
	if err := doc.AppendRebootChecked(nodestate.RebootEntry{TS: identityBase, Reason: "TotalOutage"}); err != nil {
		t.Fatalf("an autonomous record was rejected: %v", err)
	}
	if len(doc.Reboots) != 2 {
		t.Fatalf("got %d records, want 2", len(doc.Reboots))
	}
}

func TestCRRef_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ref     nodestate.CRRef
		wantErr bool
	}{
		{"populated", nodestate.CRRef{NodeUID: "node-uid", UID: "uid-1", Generation: 1}, false},
		{"generation zero is valid", nodestate.CRRef{NodeUID: "node-uid", UID: "uid-1"}, false},
		{"zero value", nodestate.CRRef{NodeUID: "node-uid"}, true},
		{"kind and name but no uid", nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"}, true},
		{"negative generation", nodestate.CRRef{NodeUID: "node-uid", UID: "uid-1", Generation: -1}, true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.ref.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, nodestate.ErrInvalidReference) {
				t.Errorf("error %v does not wrap ErrInvalidReference", err)
			}
		})
	}
}

// TestMergeStillDeduplicatesIdentifiableReferences guards the behaviour the
// two-file model depends on, which the fix above must not weaken.
func TestMergeStillDeduplicatesIdentifiableReferences(t *testing.T) {
	t.Parallel()

	ref := &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", UID: "uid-1", Generation: 2}

	snr := nodestate.NewDocument(nodestate.ComponentSNR)
	snr.AppendReboot(nodestate.RebootEntry{
		TS: identityBase, Source: nodestate.ComponentSNR, Method: nodestate.MethodSoftware, Remediation: ref,
	})
	sbr := nodestate.NewDocument(nodestate.ComponentSBR)
	sbr.AppendReboot(nodestate.RebootEntry{
		TS: identityBase.Add(time.Second), Source: nodestate.ComponentSBR,
		Method: nodestate.MethodWatchdog, Remediation: ref, Completed: true,
	})

	if got := len(nodestate.MergeReboots(snr, sbr)); got != 1 {
		t.Fatalf("got %d entries, want 1: one remediation carried out two ways is one reboot", got)
	}
}

// TestUnidentifiableRecordsStayRequested matters for budget attribution: the
// record loses its reference but must not be mistaken for a self-fence the
// agent decided on alone... except that is exactly what it becomes, so the
// test pins the consequence rather than wishing it away.
func TestUnidentifiableRecordsStayDistinctAndRequested(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 3; i++ {
		doc.AppendReboot(nodestate.RebootEntry{
			TS:          identityBase.Add(time.Duration(i) * time.Hour),
			Remediation: &nodestate.CRRef{NodeUID: "node-uid", Kind: "SelfNodeRemediation", Name: "worker-0"},
		})
	}

	autonomous := nodestate.CountReboots(identityBase.Add(-time.Hour),
		func(r nodestate.RebootEntry) bool { return r.Remediation == nil }, doc)
	requested := nodestate.CountReboots(identityBase.Add(-time.Hour),
		func(r nodestate.RebootEntry) bool { return r.Remediation != nil }, doc)

	// Unknown requested identities remain requested and distinct; they must not
	// be charged to an unrelated autonomous budget.
	if autonomous != 0 || requested != 3 {
		t.Errorf("autonomous=%d requested=%d, want 0 and 3", autonomous, requested)
	}
}
