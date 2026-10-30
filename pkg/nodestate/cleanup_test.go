package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// plantOrphan creates a temporary file of the shape atomicReplace leaves
// behind when the process is killed between creating it and renaming it.
func plantOrphan(t *testing.T, dir string, owner nodestate.Component, age time.Duration) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf(".%s.tmp-%d", owner.FileName(), time.Now().UnixNano()))
	mustWrite(t, path, `{"schemaVersion":1,"node":"half-written`)
	stamp := time.Now().Add(-age)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCleanup_ReclaimsOrphanedTemporaryFiles covers the case this product
// exists to cause: a process killed partway through a write. The watchdog
// resets the node between CreateTemp and Rename, and the temporary file is
// left behind with nothing that ever looks for it again.
func TestCleanup_ReclaimsOrphanedTemporaryFiles(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{OrphanGrace: time.Hour}))
	ctx := context.Background()

	stale := plantOrphan(t, dir, nodestate.ComponentSBR, 25*time.Hour)
	alsoStale := plantOrphan(t, dir, nodestate.ComponentSBR, 2*time.Hour)

	report, err := s.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if report.TemporaryFilesRemoved != 2 {
		t.Errorf("TemporaryFilesRemoved = %d, want 2", report.TemporaryFilesRemoved)
	}
	if report.BytesReclaimed == 0 {
		t.Error("BytesReclaimed = 0 after removing two non-empty files")
	}
	for _, path := range []string{stale, alsoStale} {
		if _, err := os.Stat(path); err == nil {
			t.Errorf("%s survived the sweep", filepath.Base(path))
		}
	}
}

// TestCleanup_LeavesAWriteThatMayStillBeInFlight is the other half of the
// rule. Deleting a temporary file that a live process is writing would turn
// housekeeping into the fault it is supposed to clean up after.
func TestCleanup_LeavesAWriteThatMayStillBeInFlight(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{OrphanGrace: time.Hour}))

	fresh := plantOrphan(t, dir, nodestate.ComponentSBR, time.Minute)

	report, err := s.Cleanup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.TemporaryFilesRemoved != 0 {
		t.Errorf("TemporaryFilesRemoved = %d, want 0", report.TemporaryFilesRemoved)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("a temporary file inside the grace period was removed: %v", err)
	}
}

// TestCleanup_NeverTouchesAPeersFiles applies the ownership rule to
// housekeeping. A peer mid-write has a temporary file of its own, and it is
// not this component's to judge.
func TestCleanup_NeverTouchesAPeersFiles(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{OrphanGrace: time.Minute}))

	peerOrphan := plantOrphan(t, dir, nodestate.ComponentSNR, 48*time.Hour)
	peerQuarantine := filepath.Join(dir, nodestate.SNRFile+".20200101T000000.000000000Z-aaaaaaaa.corrupt")
	mustWrite(t, peerQuarantine, "the peer's evidence")
	old := time.Now().Add(-365 * 24 * time.Hour)
	if err := os.Chtimes(peerQuarantine, old, old); err != nil {
		t.Fatal(err)
	}

	report, err := s.Cleanup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.TemporaryFilesRemoved != 0 || report.QuarantineFilesRemoved != 0 {
		t.Errorf("the sweep removed a peer's files: %+v", report)
	}
	for _, path := range []string{peerOrphan, peerQuarantine} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s was removed by a non-owner: %v", filepath.Base(path), err)
		}
	}
}

// TestFirstWriteSweepsWithoutBeingAsked matters because an agent that
// crash-loops is exactly the agent least likely to have a tidy start-up
// path. The reclaim must not depend on the caller remembering to ask.
func TestFirstWriteSweepsWithoutBeingAsked(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{OrphanGrace: time.Minute}))
	orphan := plantOrphan(t, dir, nodestate.ComponentSBR, time.Hour)

	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); err == nil {
		t.Error("the first write did not reclaim an orphan left by a previous process")
	}

	// The directory now holds exactly the state file: no orphan, no leftover
	// temporary file from the write that just happened.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != nodestate.SBRFile {
		t.Errorf("directory holds %v, want just %s", names(entries), nodestate.SBRFile)
	}
}

func TestCleanup_SweepsOnlyOncePerStoreOnTheWritePath(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
			t.Fatal(err)
		}
	}
	if got := rec.Count(nodestatetest.OpReadDir); got != 1 {
		t.Errorf("the write path listed the directory %d times, want 1: housekeeping must not cost a listing per write", got)
	}
}

func TestCleanup_ExpiresOldQuarantineFiles(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR,
		nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{
			QuarantineRetention: 5,
			QuarantineMaxAge:    7 * 24 * time.Hour,
		}))

	recent := filepath.Join(dir, nodestate.SBRFile+".20240101T000000.000000000Z-bbbbbbbb.corrupt")
	ancient := filepath.Join(dir, nodestate.SBRFile+".20200101T000000.000000000Z-aaaaaaaa.corrupt")
	mustWrite(t, recent, "recent evidence")
	mustWrite(t, ancient, "ancient evidence")
	old := time.Now().Add(-365 * 24 * time.Hour)
	if err := os.Chtimes(ancient, old, old); err != nil {
		t.Fatal(err)
	}

	report, err := s.Cleanup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.QuarantineFilesRemoved != 1 {
		t.Errorf("QuarantineFilesRemoved = %d, want 1", report.QuarantineFilesRemoved)
	}
	if _, err := os.Stat(ancient); err == nil {
		t.Error("an expired quarantine file survived")
	}
	if _, err := os.Stat(recent); err != nil {
		t.Errorf("a quarantine file inside its retention was removed: %v", err)
	}
}

func TestCleanup_ToleratesAnAbsentDirectory(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(tempStateDir(t), "not-created-yet")
	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}

	report, err := s.Cleanup(context.Background())
	if err != nil {
		t.Fatalf("Cleanup on an absent directory: %v, want nil", err)
	}
	if report.TemporaryFilesRemoved != 0 || report.QuarantineFilesRemoved != 0 {
		t.Errorf("report = %+v, want zero", report)
	}
}

func TestCleanup_RespectsACancelledContext(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Cleanup(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(rec.Operations()) != 0 {
		t.Errorf("operations ran on a cancelled context: %v", rec.Operations())
	}
}

// TestHousekeepingNeverEndangersTheLiveDocument is the invariant that makes
// all of the above safe to run on a node that may be killed at any instant:
// nothing in a sweep writes or replaces the state file, so a process dying
// halfway through one cannot damage it.
func TestHousekeepingNeverEndangersTheLiveDocument(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	s, dir := newStore(t, nodestate.ComponentSBR,
		nodestate.WithFileSystem(rec), nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{OrphanGrace: time.Minute}))
	ctx := context.Background()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Node = "worker-0"
	if err := s.Write(ctx, doc); err != nil {
		t.Fatal(err)
	}
	plantOrphan(t, dir, nodestate.ComponentSBR, time.Hour)
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile+".20200101T000000.000000000Z-cccccccc.corrupt"), "old")
	rec.Reset()

	if _, err := s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}

	live := s.Path(nodestate.ComponentSBR)
	for _, op := range rec.Operations() {
		switch op.Op {
		case nodestatetest.OpWrite, nodestatetest.OpCreateTemp:
			t.Errorf("a sweep wrote to %s", op.Path)
		case nodestatetest.OpRename:
			t.Errorf("a sweep renamed %s to %s", op.Path, op.Arg)
		case nodestatetest.OpRemove:
			if op.Path == live {
				t.Errorf("a sweep removed the live document at %s", op.Path)
			}
		}
	}

	back, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if back.Node != "worker-0" {
		t.Errorf("the live document changed across a sweep: %+v", back)
	}
}

// TestPreservedMembersAreCapped closes a way to wedge the agent out of its
// own file. Members this build does not recognise are carried forward
// unconditionally, and the document has a write limit — so without a ceiling,
// a peer with a bug, or anything else able to write the directory, could
// leave behind a file this agent could read but never replace.
func TestPreservedMembersAreCapped(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{MaxPreservedBytes: 4 << 10}))
	ctx := context.Background()

	raw := map[string]interface{}{
		"schemaVersion": 1,
		"node":          "worker-0",
		"agent":         map[string]string{"component": "sbr"},
		"futureBallast": strings.Repeat("x", 64<<10),
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), string(encoded))

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != nodestate.StatusOK {
		t.Fatalf("Status = %v, want StatusOK: the document itself is valid", res.Status)
	}
	if len(res.Document.Unknown) != 0 {
		t.Errorf("preserved members survived the cap: %v", keysOf(res.Document.Unknown))
	}
	if res.Document.Node != "worker-0" {
		t.Error("the cap discarded a recognised field")
	}

	var dropped bool
	for _, e := range res.Document.Events {
		if e.Reason == nodestate.EventReasonPreservedFieldsDropped {
			dropped = true
			if e.Type != "Warning" {
				t.Errorf("event type = %q, want Warning", e.Type)
			}
		}
	}
	if !dropped {
		t.Errorf("no %s event: a staggered upgrade just lost data silently",
			nodestate.EventReasonPreservedFieldsDropped)
	}

	// The agent can still write, which is the whole point.
	if _, err := s.Update(ctx, func(d *nodestate.Document) error {
		d.BootID = "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8"
		return nil
	}); err != nil {
		t.Fatalf("Update after the cap: %v", err)
	}
}

func TestPreservedMembersUnderTheCapAreKept(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{MaxPreservedBytes: 4 << 10}))

	mustWrite(t, filepath.Join(dir, nodestate.SBRFile),
		`{"schemaVersion":1,"node":"worker-0","agent":{"component":"sbr"},"futureKey":{"a":1}}`)

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Document.Unknown) != 1 {
		t.Fatalf("preserved members = %v, want futureKey kept", keysOf(res.Document.Unknown))
	}
	for _, e := range res.Document.Events {
		if e.Reason == nodestate.EventReasonPreservedFieldsDropped {
			t.Error("a document inside the cap reported members as dropped")
		}
	}
}

func TestPreservedMembersCapCountsEveryLevel(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{MaxPreservedBytes: 2 << 10}))

	// The ballast is spread across nested entries rather than the top level,
	// which a cap that only looked at Document.Unknown would miss.
	var reboots []string
	for i := 0; i < 40; i++ {
		reboots = append(reboots, fmt.Sprintf(
			`{"ts":"2024-01-01T00:00:00Z","completed":true,"futurePayload":%q}`, strings.Repeat("y", 200)))
	}
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), fmt.Sprintf(
		`{"schemaVersion":1,"agent":{"component":"sbr"},"reboots":[%s]}`, strings.Join(reboots, ",")))

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Document.Reboots) != 40 {
		t.Fatalf("got %d reboots, want 40: the records themselves must survive", len(res.Document.Reboots))
	}
	for i, r := range res.Document.Reboots {
		if len(r.Unknown) != 0 {
			t.Fatalf("reboots[%d] kept %v; nested members must count against the cap", i, keysOf(r.Unknown))
		}
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}
