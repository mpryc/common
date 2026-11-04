package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestRetentionFitsTheWriteLimit ties three defaults together that were
// chosen independently and must stay consistent.
//
// Retention bounds a document; the Store refuses to write one over
// DefaultMaxDocumentBytes. If a document trimmed exactly to the retention
// ceiling does not fit, an agent that reaches the ceiling can never write its
// state file again — the worst state for a component whose job is to record
// what it committed to before it fences a node. Raising MaxRebootEntries or
// the entry size without raising the byte limit breaks this.
func TestRetentionFitsTheWriteLimit(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSNR)
	doc.Node = "worker-0.example.openshift.local"
	doc.NodeUID = "6f0c6c1e-3f3a-4f1f-9c1e-2b1d4a5e6f70"
	doc.BootID = "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8"
	doc.Config = map[string]string{
		"arbitrationMode": "PerNode", "storageIOTimeout": "5s", "maxRebootsPerWindow": "3",
	}

	now := time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < nodestate.DefaultMaxIOEntries; i++ {
		doc.AppendIO(nodestate.IOEntry{
			TS: now, Op: "write", OK: false, LatencyMS: 5000,
			Target: "/dev/disk/by-id/scsi-360000000000000000e00000000010001",
			Error:  "context deadline exceeded while writing the canary block",
		})
	}
	for i := 0; i < nodestate.DefaultMaxEventEntries; i++ {
		doc.AppendEvent(nodestate.EventEntry{
			TS: now, Type: "Warning", Reason: "StorageIOTimeout",
			Message: "write to the canary volume did not complete within the configured timeout",
		})
	}
	// A fully populated reboot record: resource reference, episode id and both
	// boot ids. This is the largest shape the schema allows.
	for i := 0; i < nodestate.DefaultMaxRebootEntries; i++ {
		doc.AppendReboot(nodestate.RebootEntry{
			TS: now, Source: nodestate.ComponentSNR, Method: nodestate.MethodSoftware,
			Reason: "RemediationCR", EpisodeID: "6f0c6c1e-3f3a-4f1f-9c1e-2b1d4a5e6f70",
			Remediation: &nodestate.CRRef{NodeUID: "node-uid",
				Kind: "SelfNodeRemediation", Namespace: "openshift-workload-availability",
				Name: "worker-0.example.openshift.local",
				UID:  "6f0c6c1e-3f3a-4f1f-9c1e-2b1d4a5e6f70", Generation: 3,
			},
			BeforeBootID: "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8",
			AfterBootID:  "c4d5e6f7-0819-2a3b-4c5d-6e7f80912a3b",
			Completed:    true,
		})
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("document at the retention ceiling: %d bytes against a %d byte write limit (%.0f%% used)",
		len(encoded), nodestate.DefaultMaxDocumentBytes,
		100*float64(len(encoded))/float64(nodestate.DefaultMaxDocumentBytes))

	if len(encoded) > nodestate.DefaultMaxDocumentBytes {
		t.Fatalf("a document trimmed to the retention ceiling is %d bytes and cannot be written (limit %d): "+
			"retention would wedge the agent out of its own state file",
			len(encoded), nodestate.DefaultMaxDocumentBytes)
	}

	// Require real headroom, not a hair's breadth, so that a caller's own
	// Config and Agent.Extra have somewhere to go.
	if headroom := nodestate.DefaultMaxDocumentBytes - len(encoded); headroom < nodestate.DefaultMaxDocumentBytes/4 {
		t.Errorf("only %d bytes of headroom left for caller data; want at least a quarter of the limit", headroom)
	}

	// And the Store must actually accept it.
	s, _ := newStore(t, nodestate.ComponentSNR)
	if err := s.Write(context.Background(), doc); err != nil {
		t.Fatalf("writing a document at the retention ceiling: %v", err)
	}
}

// TestTwoStoresOnOneFileDoNotLoseUpdates covers a consumer mistake rather
// than a hostile one: two Stores built for the same component in one process,
// one in main and one in a controller. Without a shared lock the two
// read-modify-write cycles interleave and records vanish with nothing to show
// for it.
func TestTwoStoresOnOneFileDoNotLoseUpdates(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	ctx := context.Background()

	const stores, perStore = 4, 20

	all := make([]*nodestate.Store, stores)
	for i := range all {
		s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
		if err != nil {
			t.Fatal(err)
		}
		all[i] = s
	}

	mustInitialize(t, all[0])
	var wg sync.WaitGroup
	errs := make(chan error, stores)
	for i, s := range all {
		i, s := i, s
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < perStore; n++ {
				if _, err := s.Update(ctx, func(d *nodestate.Document) error {
					d.AppendEvent(nodestate.EventEntry{TS: time.Now(), Reason: fmt.Sprintf("s%d-n%d", i, n)})
					return nil
				}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Update: %v", err)
	}

	doc, err := all[0].Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Events) != stores*perStore {
		t.Errorf("got %d events, want %d: Stores sharing a path must share a lock", len(doc.Events), stores*perStore)
	}
	// Each Store sees the others' revisions on disk, so the sequence advances
	// once per write and never repeats.
	if doc.Revision != uint64(stores*perStore+1) {
		t.Errorf("Revision = %d, want %d", doc.Revision, stores*perStore+1)
	}
}

func TestStoresOnDifferentFilesDoNotBlockEachOther(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	ctx := context.Background()

	sbr, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	snr, err := nodestate.New(nodestate.ComponentSNR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}

	// Holding one store's lock inside its mutator must not stop the other
	// making progress; if the lock were global rather than per file, this
	// would deadlock.
	mustInitialize(t, sbr)
	mustInitialize(t, snr)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := sbr.Update(ctx, func(*nodestate.Document) error {
			<-release
			return nil
		})
		done <- err
	}()

	if _, err := snr.Update(ctx, func(*nodestate.Document) error { return nil }); err != nil {
		t.Fatalf("the peer's store was blocked: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryRequiresExplicitReconciliation ensures ordinary reads and updates
// cannot conceal unusable history by returning a fresh usable document.
func TestRecoveryRequiresExplicitReconciliation(t *testing.T) {
	s, dir := newStore(t, nodestate.ComponentSBR)
	path := filepath.Join(dir, nodestate.SBRFile)
	mustWrite(t, path, "{truncated")
	_, err := s.Update(context.Background(), func(d *nodestate.Document) error { t.Fatal("mutator ran"); return nil })
	if !errors.Is(err, nodestate.ErrHistoryUnknown) {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "{truncated" {
		t.Fatal("evidence lost")
	}
	if _, err := s.Read(context.Background()); !errors.Is(err, nodestate.ErrHistoryUnknown) {
		t.Fatal(err)
	}
}

func TestRecoveryEventIsNotEmittedForAFirstBoot(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Document.Events) != 0 {
		t.Errorf("a first boot produced events %+v; an absent file is not lost history", res.Document.Events)
	}
}

// TestEnsureDirectory_DoesNotChmodThroughASymlink covers the window between
// creating the directory and setting its mode. MkdirAll treats an existing
// directory as success and resolves symlinks to find it, and Chmod follows
// symlinks too, so the ordering of those two calls decides whether a planted
// link can redirect a mode change onto another directory.
func TestEnsureDirectory_DoesNotChmodThroughASymlink(t *testing.T) {
	t.Parallel()

	root := tempStateDir(t)
	victim := filepath.Join(root, "victim")
	if err := os.Mkdir(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(victim, 0o755); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join(root, "state")
	if err := os.Symlink(victim, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(link))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureDirectory(context.Background()); err == nil {
		t.Fatal("EnsureDirectory accepted a symlinked state directory")
	}

	fi, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o755 {
		t.Errorf("the symlink target's mode was changed to %#o; a chmod was aimed through the link", got)
	}
}

// TestEnsureDirectory_LeavesACorrectModeAlone keeps the common path free of a
// write: there is nothing to adjust when the directory is already right.
func TestEnsureDirectory_LeavesACorrectModeAlone(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(tempStateDir(t), "state")
	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureDirectory(context.Background()); err != nil {
		t.Fatal(err)
	}

	fi, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o700 {
		t.Fatalf("directory mode = %#o, want 0700 whatever the umask", got)
	}
}
