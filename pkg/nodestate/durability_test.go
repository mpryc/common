package nodestate_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// TestSyncDirFailureIsNotALostWrite is the one error from a write that does
// not mean "nothing was written".
//
// Once rename succeeds the candidate is visible, but a failed directory sync
// must block new reset authorization until the exact candidate is confirmed.
func TestSyncDirFailureIsNotALostWrite(t *testing.T) {
	t.Parallel()

	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	ctx := context.Background()

	injected := errors.New("EIO on directory fsync")
	faulty.FailOnce(nodestatetest.OpSyncDir, injected)

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Node = "worker-0"
	doc.AppendReboot(nodestate.RebootEntry{
		TS: time.Now(), Source: nodestate.ComponentSBR, Reason: "pending-fence",
		Remediation: &nodestate.CRRef{NodeUID: "node-uid", UID: "uid-1", Generation: 1},
	})

	err := s.Write(ctx, doc)
	if !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatalf("Write error = %v, want ErrDurabilityUnconfirmed", err)
	}
	// The underlying cause stays reachable for logging.
	if !errors.Is(err, injected) {
		t.Errorf("the injected cause is not reachable through the error chain: %v", err)
	}

	// It must be distinguishable from the errors that do mean nothing was
	// written, or the caller cannot branch on it.
	for _, lost := range []error{nodestate.ErrTooLarge, nodestate.ErrNotOwner, nodestate.ErrInsecurePermissions} {
		if errors.Is(err, lost) {
			t.Errorf("an unconfirmed-durability result also reports as %v", lost)
		}
	}

	back, readErr := s.Read(ctx)
	if readErr != nil {
		t.Fatalf("Read: %v", readErr)
	}
	if back.Node != "worker-0" || len(back.Reboots) != 1 {
		t.Fatalf("the document is not on disk after the reported failure: %+v", back)
	}
}

// TestRevisionAdvancesPastAnUnconfirmedWrite pins the bookkeeping. Treating
// the write as lost left lastRevision behind the file, so the next write
// reissued the same revision and two different documents claimed it — which
// is the ordering foreign-write detection reads.
func TestRevisionAdvancesPastAnUnconfirmedWrite(t *testing.T) {
	t.Parallel()

	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	ctx := context.Background()

	faulty.FailOnce(nodestatetest.OpSyncDir, errors.New("EIO"))
	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatalf("Write error = %v", err)
	}
	first, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatalf("the following write failed: %v", err)
	}
	second, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if second.Revision <= first.Revision {
		t.Errorf("revision went from %d to %d; two documents claim the same revision",
			first.Revision, second.Revision)
	}
}

// TestUpdateReturnsTheDocumentWithAnUnconfirmedWrite: the caller needs to see
// what it recorded so it can confirm that exact candidate before acting.
func TestUpdateReturnsTheDocumentWithAnUnconfirmedWrite(t *testing.T) {
	t.Parallel()

	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	ctx := context.Background()
	mustInitialize(t, s)

	doc, err := s.Update(ctx, func(d *nodestate.Document) error {
		faulty.FailOnce(nodestatetest.OpSyncDir, errors.New("EIO"))
		d.Agent.ResetCommitted = true
		return nil
	})
	if !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatalf("Update error = %v, want ErrDurabilityUnconfirmed", err)
	}
	if doc == nil {
		t.Fatal("Update returned a nil document; the caller cannot see what it just committed to")
	}
	if !doc.Agent.ResetCommitted {
		t.Error("the returned document does not carry the mutation")
	}
}

// TestFailuresBeforeTheRenameAreLostWrites is the contrast case: everything
// up to and including the rename leaves the previous version in place, and
// must not be confused with the above.
func TestFailuresBeforeTheRenameAreLostWrites(t *testing.T) {
	t.Parallel()

	for _, failAt := range []nodestatetest.Op{
		nodestatetest.OpCreateTemp, nodestatetest.OpWrite, nodestatetest.OpSyncFile,
		nodestatetest.OpClose, nodestatetest.OpRename,
	} {
		failAt := failAt
		t.Run(string(failAt), func(t *testing.T) {
			t.Parallel()

			faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
			s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
			ctx := context.Background()

			good := nodestate.NewDocument(nodestate.ComponentSBR)
			good.Node = "worker-0"
			if err := s.Write(ctx, good); err != nil {
				t.Fatal(err)
			}

			faulty.OnlyFailWhen(func(r nodestatetest.Record) bool {
				return r.Op != nodestatetest.OpClose || strings.Contains(r.Path, ".tmp-")
			})
			faulty.FailOnce(failAt, errors.New("injected"))
			next := good.Clone()
			next.Node = "should-not-land"
			err := s.Write(ctx, next)
			if err == nil {
				t.Fatal("Write succeeded despite the injected fault")
			}
			if errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
				t.Fatalf("a failure at %s reports as ErrDurabilityUnconfirmed; "+
					"the caller would try to confirm a candidate that is not on disk", failAt)
			}

			back, readErr := s.Read(ctx)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if back.Node != "worker-0" {
				t.Errorf("the previous version was disturbed: Node = %q", back.Node)
			}
		})
	}
}

// TestUnconfirmedWriteLeavesNoTemporaryFile: the rename consumed it, so there
// is nothing to clean up and the sweep must not be given something to find.
func TestUnconfirmedWriteLeavesNoTemporaryFile(t *testing.T) {
	t.Parallel()

	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))

	faulty.FailOnce(nodestatetest.OpSyncDir, errors.New("EIO"))
	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatalf("Write error = %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != nodestate.SBRFile {
		t.Errorf("directory holds %v, want just %s", names(entries), nodestate.SBRFile)
	}
}
