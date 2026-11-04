package nodestate_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// TestLoad_FileThatGrowsBetweenStatAndReadIsRefused covers the window
// between deciding a file is worth reading and reading it. On a node under a
// storage fault that window is wide enough to matter, so the size limit is
// enforced on the bytes actually read and not only on what stat claimed.
func TestLoad_FileThatGrowsBetweenStatAndReadIsRefused(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	path := filepath.Join(dir, nodestate.SBRFile)
	mustWrite(t, path, `{"schemaVersion":1}`)

	fsys := &statLiesFS{FS: nodestate.OSFileSystem(), target: path, reportedSize: 16}
	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(dir), nodestate.WithFileSystem(fsys), nodestate.WithMaxReadBytes(32))
	if err != nil {
		t.Fatal(err)
	}

	// The file on disk is larger than the read limit, but Lstat under-reports
	// it. The limit must still be enforced on the bytes actually read.
	mustWrite(t, path, strings.Repeat("x", 1024))

	_, err = s.Load(context.Background())
	if !errors.Is(err, nodestate.ErrTooLarge) {
		t.Fatal(err)
	}
}

// statLiesFS under-reports the size of one file.
type statLiesFS struct {
	nodestate.FS
	target       string
	reportedSize int64
}

func (s *statLiesFS) Lstat(name string) (os.FileInfo, error) {
	fi, err := s.FS.Lstat(name)
	if err != nil || name != s.target {
		return fi, err
	}
	return shrunkInfo{FileInfo: fi, size: s.reportedSize}, nil
}

type shrunkInfo struct {
	os.FileInfo
	size int64
}

func (i shrunkInfo) Size() int64 { return i.size }

func TestLoad_FileRemovedBetweenStatAndOpenReadsAsAbsent(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	path := filepath.Join(dir, nodestate.SBRFile)
	mustWrite(t, path, `{"schemaVersion":1,"node":"worker-0"}`)

	vanishing := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	vanishing.FailOnce(nodestatetest.OpOpen, fs.ErrNotExist)

	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(dir), nodestate.WithFileSystem(vanishing))
	if err != nil {
		t.Fatal(err)
	}

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Status != nodestate.StatusAbsent {
		t.Fatalf("Status = %v, want StatusAbsent: a file that disappears is absent, not corrupt", res.Status)
	}
	// Nothing must have been quarantined: there was nothing to preserve.
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.corrupt")); len(matches) != 0 {
		t.Errorf("quarantined %v for a file that was simply gone", matches)
	}
}

// TestLoad_DoesNotAttemptQuarantineOnReadOnlyStorage covers the read-only root
// filesystem, the wrong SELinux label, and the directory the agent may read
// but not write.
//
// The file is unusable and cannot be moved aside. Refusing to hand back a
// document would mean the agent cannot classify its start-up or run at all,
// for a document it was going to start from nothing anyway. It gets one, in
// memory, with the fault reported — and the damaged file is left exactly
// where it was.
func TestLoad_DoesNotAttemptQuarantineOnReadOnlyStorage(t *testing.T) {
	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	path := filepath.Join(dir, nodestate.SBRFile)
	mustWrite(t, path, "{bad")
	faulty.FailAlways(nodestatetest.OpRename, errors.New("read-only"))
	res, err := s.Load(context.Background())
	if err != nil || res.Status != nodestate.StatusCorrupt || res.QuarantineFailed {
		t.Fatalf("%+v %v", res, err)
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "{bad" {
		t.Fatal("bytes changed")
	}
}

// TestLoad_RunningDegradedOnAReadOnlyHostPath is the end-to-end shape of the
// same fault: the agent starts, classifies, and keeps working without ever
// persisting anything.
func TestLoad_RunningDegradedOnAReadOnlyHostPath(t *testing.T) {
	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), "{bad")
	faulty.FailAlways(nodestatetest.OpWrite, errors.New("read-only"))
	res, err := s.Load(context.Background())
	if err != nil || res.Status != nodestate.StatusCorrupt {
		t.Fatalf("%+v %v", res, err)
	}
	_, err = s.Update(context.Background(), func(d *nodestate.Document) error { t.Fatal("unknown history mutated"); return nil })
	if !errors.Is(err, nodestate.ErrHistoryUnknown) {
		t.Fatal(err)
	}
}

func TestLoad_StatFailureIsAnError(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	broken := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	injected := errors.New("EIO")
	broken.FailAlways(nodestatetest.OpLstat, injected)

	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(dir), nodestate.WithFileSystem(broken))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Load(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Load error = %v, want the injected fault", err)
	}
	if _, err := s.Read(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Read error = %v, want the injected fault", err)
	}
	if err := s.EnsureDirectory(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("EnsureDirectory error = %v, want the injected fault", err)
	}
}

// TestHostPathFault_IsNotAStorageFault is the invariant the design calls out:
// being unable to keep a note is evidence about this agent's bookkeeping,
// never about the cluster's storage. The library makes that distinguishable
// by classification, so a caller cannot conflate the two by accident.
func TestHostPathFault_IsNotAStorageFault(t *testing.T) {
	t.Parallel()

	broken := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	broken.FailAlways(nodestatetest.OpCreateTemp, errors.New("ENOSPC"))

	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(tempStateDir(t)), nodestate.WithFileSystem(broken))
	if err != nil {
		t.Fatal(err)
	}

	err = s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR))
	if err == nil {
		t.Fatal("Write succeeded with no space")
	}
	var pe *nodestate.PathError
	if !errors.As(err, &pe) {
		t.Fatalf("error = %v, want a *PathError naming the host path", err)
	}
	if pe.Op != "createtemp" {
		t.Errorf("PathError.Op = %q, want createtemp", pe.Op)
	}
	// It must not be mistakable for a content problem.
	for _, sentinel := range []error{nodestate.ErrCorrupt, nodestate.ErrSchemaUnsupported, nodestate.ErrNotOwner} {
		if errors.Is(err, sentinel) {
			t.Errorf("a host-path fault reports as %v", sentinel)
		}
	}
}

func TestReadAll_ExplicitComponentSet(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatal(err)
	}

	third, err := nodestate.New(nodestate.Component("fence-agent"), nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Write(ctx, nodestate.NewDocument(nodestate.Component("fence-agent"))); err != nil {
		t.Fatal(err)
	}

	docs := s.ReadAll(ctx,
		nodestate.ComponentSBR,
		nodestate.ComponentSBR, // duplicate: must not be read twice or double-counted
		nodestate.Component("fence-agent"),
		nodestate.Component("../escape"), // invalid: skipped, not an error
	)
	if len(docs) != 2 {
		t.Fatalf("got %d documents, want 2: %v", len(docs), docs)
	}
	if docs[nodestate.Component("fence-agent")] == nil {
		t.Error("a third-party component's file was not read")
	}

	// A cancelled context yields an empty result rather than a partial one.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if got := s.ReadAll(cancelled); len(got) != 0 {
		t.Errorf("ReadAll on a cancelled context returned %d documents", len(got))
	}
}

func TestReadAll_IncludesASelfComponentOutsideTheKnownSet(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	s, err := nodestate.New(nodestate.Component("fence-agent"), nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.Component("fence-agent"))); err != nil {
		t.Fatal(err)
	}

	docs := s.ReadAll(context.Background())
	if docs[nodestate.Component("fence-agent")] == nil {
		t.Fatalf("ReadAll with no arguments omitted this store's own component: %v", docs)
	}
}

func TestEnsureDirectory_RespectsACancelledContext(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.EnsureDirectory(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(rec.Operations()) != 0 {
		t.Errorf("operations ran on a cancelled context: %v", rec.Operations())
	}
}

func TestRecordingFS_CountAndReset(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
			t.Fatal(err)
		}
	}
	if got := rec.Count(nodestatetest.OpRename); got != 3 {
		t.Errorf("rename count = %d, want 3", got)
	}
	rec.Reset()
	if got := rec.Count(nodestatetest.OpRename); got != 0 {
		t.Errorf("rename count after Reset = %d, want 0", got)
	}
}

func TestFaultyFS_OnlyFailWhenAndClear(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	injected := errors.New("injected")
	faulty.FailAlways(nodestatetest.OpWrite, injected)
	faulty.OnlyFailWhen(func(r nodestatetest.Record) bool {
		return strings.Contains(r.Path, nodestate.SNRFile)
	})

	sbr, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir), nodestate.WithFileSystem(faulty))
	if err != nil {
		t.Fatal(err)
	}
	snr, err := nodestate.New(nodestate.ComponentSNR, nodestate.WithDir(dir), nodestate.WithFileSystem(faulty))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := sbr.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatalf("the unfiltered writer failed: %v", err)
	}
	if err := snr.Write(ctx, nodestate.NewDocument(nodestate.ComponentSNR)); !errors.Is(err, injected) {
		t.Fatalf("the filtered writer error = %v, want the injected fault", err)
	}

	faulty.Clear()
	if err := snr.Write(ctx, nodestate.NewDocument(nodestate.ComponentSNR)); err != nil {
		t.Fatalf("after Clear: %v", err)
	}
}
