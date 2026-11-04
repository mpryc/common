package nodestate_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// TestDurabilityConfirmationIsCached covers the cost side of the durability
// contract.
//
// A process cannot know whether its predecessor's last write survived, so the
// first Load must synchronize. Repeating that on every later Load buys
// nothing — the bytes were either written and synchronized by this process or
// already confirmed by it — while costing another chance to block in
// uninterruptible sleep on a node whose storage is failing, which is the only
// state this package exists for.
func TestDurabilityConfirmationIsCached(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))
	ctx := context.Background()
	initialize(t, s)

	rec.Reset()
	for i := 0; i < 5; i++ {
		if _, err := s.Load(ctx); err != nil {
			t.Fatalf("Load %d: %v", i, err)
		}
	}
	if got := rec.Count(nodestatetest.OpSyncFile); got != 0 {
		t.Errorf("%d file syncs across 5 Loads of bytes this process wrote; want 0", got)
	}
	if got := rec.Count(nodestatetest.OpSyncDir); got != 0 {
		t.Errorf("%d directory syncs across 5 Loads; want 0", got)
	}
}

// TestFirstLoadOfAProcessConfirms is the other half: the cache must not skip
// the confirmation that matters. A Store that has written nothing is looking
// at a predecessor's bytes and has to establish them itself.
func TestFirstLoadOfAProcessConfirms(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	first, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	initialize(t, first)

	// A new Store stands in for a restarted process.
	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	successor, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(dir), nodestate.WithFileSystem(rec))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := successor.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.Count(nodestatetest.OpSyncFile) == 0 || rec.Count(nodestatetest.OpSyncDir) == 0 {
		t.Error("the first Load of a new process did not confirm its predecessor's durability")
	}
}

// TestUnconfirmedWriteInvalidatesTheCache: once a write could not be
// confirmed, the next Load must try again rather than trust a stale mark.
func TestUnconfirmedWriteInvalidatesTheCache(t *testing.T) {
	t.Parallel()

	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	rec := nodestatetest.NewRecordingFS(faulty)
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))
	ctx := context.Background()
	initialize(t, s)

	faulty.FailOnce(nodestatetest.OpSyncDir, errors.New("EIO"))
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Node = "worker-0"
	if err := s.Write(ctx, doc); !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatalf("Write = %v, want ErrDurabilityUnconfirmed", err)
	}

	rec.Reset()
	if _, err := s.Load(ctx); err != nil {
		t.Fatalf("Load after an unconfirmed write: %v", err)
	}
	if rec.Count(nodestatetest.OpSyncFile) == 0 {
		t.Error("Load trusted a cached confirmation after a write that could not be confirmed")
	}
}

// TestConfirmDurabilityResolvesTheExactCandidate covers the resolution path
// design §5.6 asks for: confirm this record, without rerunning the mutator
// that produced it.
func TestConfirmDurabilityResolvesTheExactCandidate(t *testing.T) {
	t.Parallel()

	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	ctx := context.Background()
	initialize(t, s)

	faulty.FailOnce(nodestatetest.OpSyncDir, errors.New("EIO"))
	candidate, err := s.Update(ctx, func(d *nodestate.Document) error {
		d.Agent.ResetCommitted = true
		return nil
	})
	if !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatalf("Update = %v, want ErrDurabilityUnconfirmed", err)
	}
	if candidate == nil || !candidate.Agent.ResetCommitted {
		t.Fatal("the candidate was not returned for resolution")
	}

	if err := s.ConfirmDurability(ctx, candidate); err != nil {
		t.Fatalf("ConfirmDurability on the exact candidate: %v", err)
	}

	// A stale candidate must not resolve: it is not what is on disk.
	stale := candidate.Clone()
	stale.Node = "something-else"
	if err := s.ConfirmDurability(ctx, stale); !errors.Is(err, nodestate.ErrStateChanged) {
		t.Errorf("ConfirmDurability on a stale candidate = %v, want ErrStateChanged", err)
	}
	if err := s.ConfirmDurability(ctx, nil); err == nil {
		t.Error("ConfirmDurability(nil) succeeded")
	}
}

func TestConfirmDurabilityRefusesUncertainHistory(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	initialize(t, s)
	candidate, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), "{ truncated")
	if err := s.ConfirmDurability(ctx, candidate); !errors.Is(err, nodestate.ErrHistoryUnknown) {
		t.Errorf("ConfirmDurability against corrupt history = %v, want ErrHistoryUnknown", err)
	}
}

// TestOnlyCreatedDirectoriesAreSynchronized covers the start-up cost.
//
// Walking to the root would fsync /var, / and everything above the state
// directory — paths this agent never modified. That is wasted I/O where it is
// least affordable, and a failure on one of them would stop the agent
// starting over a directory it does not own.
func TestOnlyCreatedDirectoriesAreSynchronized(t *testing.T) {
	t.Parallel()

	root := tempStateDir(t)
	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	dir := filepath.Join(root, "var", "lib", "medik8s", "state")
	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir), nodestate.WithFileSystem(rec))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureDirectory(context.Background()); err != nil {
		t.Fatal(err)
	}

	var synced []string
	for _, op := range rec.Operations() {
		if op.Op == nodestatetest.OpSyncDir {
			synced = append(synced, op.Path)
		}
	}

	// Four directories were created, so their four parents are synchronized:
	// root, root/var, root/var/lib and root/var/lib/medik8s.
	want := []string{
		root,
		filepath.Join(root, "var"),
		filepath.Join(root, "var", "lib"),
		filepath.Join(root, "var", "lib", "medik8s"),
	}
	if len(synced) != len(want) {
		t.Fatalf("synchronized %v, want exactly %v", synced, want)
	}
	for i := range want {
		if synced[i] != want[i] {
			t.Errorf("sync %d = %q, want %q (a parent must be durable before the child naming it)",
				i, synced[i], want[i])
		}
	}
	for _, path := range synced {
		if path == "/" || path == "/tmp" {
			t.Errorf("synchronized %q, which this agent did not create", path)
		}
	}
}

// TestExistingDirectoryIsNotResynchronized: the ordinary start-up, where the
// host path is already mounted and present, must not fsync anything.
func TestExistingDirectoryIsNotResynchronized(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))

	if err := s.EnsureDirectory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.Count(nodestatetest.OpSyncDir); got != 0 {
		t.Errorf("%d directory syncs for a directory that already existed; want 0", got)
	}
}
