package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// TestFsyncOrdering pins the durability contract. Any reordering of these four
// steps allows a reader, after a power loss, to observe a file that exists but
// has no contents.
func TestFsyncOrdering(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestatetest.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))

	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	want := []nodestatetest.Op{
		nodestatetest.OpWrite,
		nodestatetest.OpSyncFile,
		nodestatetest.OpClose,
		nodestatetest.OpRename,
		nodestatetest.OpSyncDir,
	}
	got := rec.Sequence(want...)
	if len(got) != len(want) {
		t.Fatalf("durability sequence = %v, want %v (full trace: %v)", got, want, rec.Operations())
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("durability sequence = %v, want %v (full trace: %v)", got, want, rec.Operations())
		}
	}

	// The temporary file must live in the same directory as the target, or the
	// rename is not atomic.
	var tmpPath, renameTo string
	for _, op := range rec.Operations() {
		switch op.Op {
		case nodestatetest.OpCreateTemp:
			tmpPath = op.Path
		case nodestatetest.OpRename:
			tmpPath, renameTo = op.Path, op.Arg
		}
	}
	if filepath.Dir(tmpPath) != filepath.Dir(renameTo) {
		t.Errorf("temporary file %q is not in the target directory %q; rename would not be atomic", tmpPath, renameTo)
	}
	if renameTo != s.Path(nodestate.ComponentSBR) {
		t.Errorf("renamed to %q, want %q", renameTo, s.Path(nodestate.ComponentSBR))
	}
}

// TestAtomicWrite_ReaderNeverSeesPartial is the reason the design needs no
// lock. Two writers hammer the directory while a reader runs flat out; every
// read either misses the file or parses completely.
func TestAtomicWrite_ReaderNeverSeesPartial(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the timed concurrency test in short mode")
	}
	t.Parallel()

	dir := tempStateDir(t)
	duration := 3 * time.Second
	if deadline, ok := t.Deadline(); ok {
		if remaining := time.Until(deadline) / 3; remaining < duration {
			duration = remaining
		}
	}

	sbr, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	snr, err := nodestate.New(nodestate.ComponentSNR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var (
		wg      sync.WaitGroup
		writes  int64
		reads   int64
		failure atomic.Value
	)
	fail := func(format string, args ...interface{}) {
		failure.CompareAndSwap(nil, fmt.Sprintf(format, args...))
		cancel()
	}

	write := func(s *nodestate.Store, self nodestate.Component) {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond) // 100 Hz
		defer ticker.Stop()
		n := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			n++
			doc := nodestate.NewDocument(self)
			doc.Node = "worker-0"
			doc.BootID = fmt.Sprintf("boot-%d", n)
			// Make the payload large and variable so a torn write is easy to spot.
			for i := 0; i < 50; i++ {
				doc.AppendIO(nodestate.IOEntry{
					TS:        time.Now(),
					Op:        "write",
					Target:    fmt.Sprintf("/dev/sbr-%d-%d", n, i),
					LatencyMS: int64(i),
					OK:        i%2 == 0,
				})
			}
			if err := s.Write(ctx, doc); err != nil {
				if ctx.Err() != nil {
					return
				}
				fail("write by %s: %v", self, err)
				return
			}
			atomic.AddInt64(&writes, 1)
		}
	}

	wg.Add(2)
	go write(sbr, nodestate.ComponentSBR)
	go write(snr, nodestate.ComponentSNR)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			for _, c := range []nodestate.Component{nodestate.ComponentSBR, nodestate.ComponentSNR} {
				raw, err := os.ReadFile(filepath.Join(dir, c.FileName()))
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				if err != nil {
					fail("raw read of %s: %v", c, err)
					return
				}
				if len(raw) == 0 {
					fail("read an empty %s file: a rename never exposes a zero-length document", c)
					return
				}
				var doc nodestate.Document
				if err := json.Unmarshal(raw, &doc); err != nil {
					fail("partial document observed in %s after %d bytes: %v\n%q", c, len(raw), err, raw)
					return
				}
				if doc.Node != "worker-0" {
					fail("document from %s has Node %q, want worker-0", c, doc.Node)
					return
				}
				if doc.Agent.Component != c {
					fail("file %s contains a document owned by %q", c, doc.Agent.Component)
					return
				}
				if len(doc.IO) != 50 {
					fail("document from %s has %d I/O entries, want 50", c, len(doc.IO))
					return
				}
				atomic.AddInt64(&reads, 1)
			}

			// Also exercise the library's own tolerant reader.
			if _, err := reader.ReadPeer(ctx, nodestate.ComponentSNR); err != nil && ctx.Err() == nil {
				fail("ReadPeer: %v", err)
				return
			}
		}
	}()

	wg.Wait()
	if msg, ok := failure.Load().(string); ok {
		t.Fatal(msg)
	}

	t.Logf("%d writes, %d complete reads", atomic.LoadInt64(&writes), atomic.LoadInt64(&reads))
	if atomic.LoadInt64(&writes) < 10 {
		t.Errorf("only %d writes completed; the test did not exercise the race", writes)
	}
	if atomic.LoadInt64(&reads) < 10 {
		t.Errorf("only %d reads completed; the test did not exercise the race", reads)
	}

	// No temporary file may survive a clean run.
	leftovers, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range leftovers {
		if e.Name() != nodestate.SBRFile && e.Name() != nodestate.SNRFile {
			t.Errorf("leftover file %q after %d writes", e.Name(), writes)
		}
	}
}

// TestConcurrentUpdate_NoLostUpdateWithinAProcess covers the realistic case: a
// single agent whose goroutines both record to the same file.
func TestConcurrentUpdate_NoLostUpdateWithinAProcess(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	const goroutines, perGoroutine = 8, 25

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				_, err := s.Update(ctx, func(d *nodestate.Document) error {
					d.AppendEvent(nodestate.EventEntry{
						TS:     time.Now(),
						Reason: fmt.Sprintf("g%d-i%d", g, i),
					})
					return nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Update: %v", err)
	}

	doc, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Retention caps the ring, so assert on the count that survives it.
	want := goroutines * perGoroutine
	if want > nodestate.DefaultMaxEventEntries {
		want = nodestate.DefaultMaxEventEntries
	}
	if len(doc.Events) != want {
		t.Errorf("got %d events, want %d: Update must serialise read-modify-write within a process", len(doc.Events), want)
	}
	if doc.Revision != uint64(goroutines*perGoroutine) {
		t.Errorf("Revision = %d, want %d", doc.Revision, goroutines*perGoroutine)
	}
}

func TestWrite_CleansUpTheTemporaryFileOnFailure(t *testing.T) {
	t.Parallel()

	// A failure before the rename leaves nothing at all; a failure at or after
	// it is the point of no return, where the replacement has already become
	// visible. Each case states which it is rather than being excused.
	cases := []struct {
		failAt      nodestatetest.Op
		wantReplace bool
	}{
		{nodestatetest.OpWrite, false},
		{nodestatetest.OpSyncFile, false},
		{nodestatetest.OpClose, false},
		{nodestatetest.OpRename, false},
		{nodestatetest.OpSyncDir, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.failAt), func(t *testing.T) {
			t.Parallel()

			injected := errors.New("injected host path fault")
			faulty := nodestatetest.NewFaultyFS(nodestatetest.OSFileSystem())
			faulty.FailOnce(tc.failAt, injected)

			s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
			err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR))
			if !errors.Is(err, injected) {
				t.Fatalf("Write error = %v, want the injected fault", err)
			}

			entries, readErr := os.ReadDir(dir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}

			want := []string{}
			if tc.wantReplace {
				want = []string{nodestate.SBRFile}
			}
			if strings.Join(names, ",") != strings.Join(want, ",") {
				t.Fatalf("after a failure at %s the directory holds %v, want %v: "+
					"no temporary file may survive, and the target exists only if the rename completed",
					tc.failAt, names, want)
			}
		})
	}
}

func TestWrite_FailureKeepsThePreviousVersionReadable(t *testing.T) {
	t.Parallel()

	faulty := nodestatetest.NewFaultyFS(nodestatetest.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	ctx := context.Background()

	good := nodestate.NewDocument(nodestate.ComponentSBR)
	good.Node = "worker-0"
	good.BootID = "boot-good"
	if err := s.Write(ctx, good); err != nil {
		t.Fatal(err)
	}

	faulty.FailOnce(nodestatetest.OpWrite, errors.New("ENOSPC"))
	next := good.Clone()
	next.BootID = "boot-bad"
	if err := s.Write(ctx, next); err == nil {
		t.Fatal("Write succeeded, want the injected failure")
	}

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load after a failed write: %v", err)
	}
	if res.Status != nodestate.StatusOK {
		t.Fatalf("Status = %v, want StatusOK: a failed write must leave the prior version intact", res.Status)
	}
	if res.Document.BootID != "boot-good" {
		t.Errorf("BootID = %q, want boot-good", res.Document.BootID)
	}
}

// TestWrite_ReportsAFaultWithoutPanicking covers the host path going away
// underneath a running agent, which is scenario 16 in the design.
func TestWrite_ReportsAFaultWithoutPanicking(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(stateDir))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := os.RemoveAll(stateDir); err != nil {
		t.Fatal(err)
	}
	// The directory is recreated rather than failing: a vanished host path is a
	// recoverable condition for the owner of the file.
	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatalf("write after the directory was removed: %v", err)
	}

	// Replacing the directory with a regular file is a fault the caller must see.
	if err := os.RemoveAll(stateDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err == nil {
		t.Fatal("Write succeeded with a regular file in place of the state directory")
	}
}
