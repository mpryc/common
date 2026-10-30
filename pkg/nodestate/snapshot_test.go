package nodestate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

func writePeerDoc(t *testing.T, dir string, c nodestate.Component, mutate func(*nodestate.Document)) {
	t.Helper()
	store, err := nodestate.New(c, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	doc := nodestate.NewDocument(c)
	doc.Node = "worker-0"
	mutate(doc)
	if err := store.Write(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshot_UnreadablePeerMakesTheHistoryIncomplete is the hole the
// primitives leave open.
//
// ReadAll drops a peer it cannot read, and RebootHistoryComplete, handed
// only what survived, reports the merged history as whole. The count comes
// out low and confident, which is the worst shape a budget error can take:
// a reboot loop the peer carried out is invisible to the brake.
func TestSnapshot_UnreadablePeerMakesTheHistoryIncomplete(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; file permissions do not bite")
	}
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	now := time.Now()

	writePeerDoc(t, dir, nodestate.ComponentSBR, func(d *nodestate.Document) {
		d.AppendReboot(nodestate.RebootEntry{TS: now.Add(-time.Hour), Source: nodestate.ComponentSBR})
	})
	writePeerDoc(t, dir, nodestate.ComponentSNR, func(d *nodestate.Document) {
		d.AppendReboot(nodestate.RebootEntry{TS: now.Add(-2 * time.Hour), Source: nodestate.ComponentSNR})
	})
	if err := os.Chmod(filepath.Join(dir, nodestate.SNRFile), 0o000); err != nil {
		t.Fatal(err)
	}

	window := now.Add(-24 * time.Hour)

	// The old composition still behaves as it did, and that is the point:
	// it cannot know a file is missing.
	docs := s.ReadAll(ctx)
	if !nodestate.RebootHistoryComplete(window, docs[nodestate.ComponentSBR], docs[nodestate.ComponentSNR]) {
		t.Fatal("setup is wrong; the primitives are expected to miss this")
	}

	snap := s.Snapshot(ctx)
	if snap.HistoryComplete(window) {
		t.Error("HistoryComplete = true with an unreadable peer holding a reboot record")
	}
	if got := snap.Missing(); len(got) != 1 || got[0] != nodestate.ComponentSNR {
		t.Errorf("Missing() = %v, want [snr]", got)
	}
	if read := snap.Peers[nodestate.ComponentSNR]; read == nil || !read.Configured() {
		t.Error("the unreadable peer is not reported as a deployment fault")
	}
	if got := snap.CountReboots(window, nil); got != 1 {
		t.Errorf("CountReboots = %d, want 1: the count is still a lower bound, just an honest one", got)
	}
}

func TestSnapshot_CompleteWhenEverythingReads(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	now := time.Now()

	ref := &nodestate.CRRef{UID: "uid-1", Generation: 1}
	writePeerDoc(t, dir, nodestate.ComponentSBR, func(d *nodestate.Document) {
		d.AppendReboot(nodestate.RebootEntry{
			TS: now.Add(-time.Hour), Source: nodestate.ComponentSBR,
			Method: nodestate.MethodWatchdog, Remediation: ref, Completed: true,
		})
	})
	writePeerDoc(t, dir, nodestate.ComponentSNR, func(d *nodestate.Document) {
		d.AppendReboot(nodestate.RebootEntry{
			TS: now.Add(-time.Hour), Source: nodestate.ComponentSNR,
			Method: nodestate.MethodSoftware, Remediation: ref,
		})
		d.AppendReboot(nodestate.RebootEntry{TS: now.Add(-3 * time.Hour), Reason: "TotalOutage"})
	})

	snap := s.Snapshot(ctx)
	window := now.Add(-24 * time.Hour)

	if !snap.HistoryComplete(window) {
		t.Errorf("HistoryComplete = false with both files readable; Missing() = %v", snap.Missing())
	}
	if got := len(snap.Documents()); got != 2 {
		t.Fatalf("Documents() = %d, want 2", got)
	}
	// One remediation carried out by two agents is one reboot, plus the
	// autonomous one.
	if got := len(snap.Reboots()); got != 2 {
		t.Errorf("Reboots() = %d entries, want 2", len(snap.Reboots()))
	}
	if got := snap.CountReboots(window, func(r nodestate.RebootEntry) bool { return r.Remediation == nil }); got != 1 {
		t.Errorf("autonomous count = %d, want 1", got)
	}
	if got := snap.CountReboots(window, func(r nodestate.RebootEntry) bool { return r.Remediation != nil }); got != 1 {
		t.Errorf("requested count = %d, want 1", got)
	}
}

func TestSnapshot_AbsentPeerIsNotIncomplete(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	writePeerDoc(t, dir, nodestate.ComponentSBR, func(d *nodestate.Document) {})

	snap := s.Snapshot(ctx)
	if !snap.HistoryComplete(time.Now().Add(-time.Hour)) {
		t.Error("a peer that never ran made the history incomplete; it has no records to be missing")
	}
	if got := snap.Missing(); len(got) != 0 {
		t.Errorf("Missing() = %v, want empty", got)
	}
	if snap.Peers[nodestate.ComponentSNR].Status != nodestate.PeerAbsent {
		t.Errorf("snr status = %v, want PeerAbsent", snap.Peers[nodestate.ComponentSNR].Status)
	}
}

func TestSnapshot_NewerSchemaPeerMakesTheHistoryIncomplete(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	writePeerDoc(t, dir, nodestate.ComponentSBR, func(d *nodestate.Document) {})
	mustWrite(t, filepath.Join(dir, nodestate.SNRFile), `{"schemaVersion":99,"agent":{"component":"snr"}}`)

	snap := s.Snapshot(ctx)
	if snap.HistoryComplete(time.Now().Add(-time.Hour)) {
		t.Error("HistoryComplete = true with a peer this build cannot interpret")
	}
	if got := snap.Missing(); len(got) != 1 || got[0] != nodestate.ComponentSNR {
		t.Errorf("Missing() = %v, want [snr]", got)
	}
	// It is not a deployment fault; this side is simply older.
	if snap.Peers[nodestate.ComponentSNR].Configured() {
		t.Error("a newer peer reported as a local misconfiguration")
	}
}

func TestSnapshot_HorizonStillCounts(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	now := time.Now()

	writePeerDoc(t, dir, nodestate.ComponentSBR, func(d *nodestate.Document) {
		d.RebootHistoryCompleteSince = now.Add(-2 * time.Hour)
	})

	snap := s.Snapshot(ctx)
	if snap.HistoryComplete(now.Add(-24 * time.Hour)) {
		t.Error("a window reaching past a document's own horizon reported complete")
	}
	if !snap.HistoryComplete(now.Add(-time.Hour)) {
		t.Error("a window inside the horizon reported incomplete")
	}
	// A trimmed history is not a missing file.
	if got := snap.Missing(); len(got) != 0 {
		t.Errorf("Missing() = %v, want empty", got)
	}
}

func TestSnapshot_DoesNotWrite(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), "{corrupt")
	mustWrite(t, filepath.Join(dir, nodestate.SNRFile), "{also corrupt")

	snap := s.Snapshot(context.Background())
	if len(snap.Documents()) != 0 {
		t.Error("a document was produced from corrupt files")
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.corrupt")); len(matches) != 0 {
		t.Errorf("Snapshot quarantined something: %v; it is a read-only view", matches)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, nodestate.SBRFile)); string(got) != "{corrupt" {
		t.Error("Snapshot modified this Store's own damaged file")
	}
}

func TestSnapshot_NilAndCancelled(t *testing.T) {
	t.Parallel()

	var nilSnap *nodestate.Snapshot
	if nilSnap.HistoryComplete(time.Now()) {
		t.Error("a nil Snapshot claimed a complete history")
	}
	if nilSnap.Document(nodestate.ComponentSBR) != nil || nilSnap.Documents() != nil || nilSnap.Missing() != nil {
		t.Error("a nil Snapshot returned something")
	}

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := s.Snapshot(ctx); len(got.Peers) != 0 {
		t.Errorf("Snapshot on a cancelled context read %d components", len(got.Peers))
	}
}

func TestSnapshot_ExplicitComponents(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	third := nodestate.Component("fence-agent")

	writePeerDoc(t, dir, nodestate.ComponentSBR, func(d *nodestate.Document) {})
	writePeerDoc(t, dir, third, func(d *nodestate.Document) {})

	snap := s.Snapshot(context.Background(), nodestate.ComponentSBR, third, third)
	if len(snap.Peers) != 2 {
		t.Fatalf("read %d components, want 2", len(snap.Peers))
	}
	if snap.Document(third) == nil {
		t.Error("a third-party component was not read")
	}
	if snap.Document(nodestate.ComponentSNR) != nil {
		t.Error("a component that was not asked for was read")
	}
}
