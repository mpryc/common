package nodestate_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
)

// privateDirUnder creates a 0700 directory under parent, or skips when that
// filesystem is not available on this machine.
func privateDirUnder(t *testing.T, parent string) string {
	t.Helper()

	if _, err := os.Stat(parent); err != nil {
		t.Skipf("%s is not available: %v", parent, err)
	}
	dir, err := os.MkdirTemp(parent, "nodestate-durability-")
	if err != nil {
		t.Skipf("cannot create a directory under %s: %v", parent, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

// TestCheckDurableStorage_RejectsMemoryBackedStorage covers the deployment
// mistake with no symptom: the host path was not mounted, so the agent is
// writing into memory or into the container's own layer.
//
// Nothing errors when that happens. Every write succeeds, every read
// succeeds, and the state resets on every pod replacement or every reboot,
// so the reboot budget never accumulates and the brake never applies. An
// agent can run that way for months.
func TestCheckDurableStorage_RejectsMemoryBackedStorage(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("filesystem type is only reported on Linux")
	}
	t.Parallel()

	// /dev/shm is tmpfs wherever it exists, which is what an emptyDir with
	// medium Memory looks like, and close enough to what a missing mount
	// looks like for this check.
	dir := privateDirUnder(t, "/dev/shm")

	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}

	err = s.CheckDurableStorage(context.Background())
	if !errors.Is(err, nodestate.ErrNotDurable) {
		t.Fatalf("CheckDurableStorage = %v, want ErrNotDurable", err)
	}
	if !errContains(err, "tmpfs") {
		t.Errorf("the error does not name the filesystem: %v", err)
	}
	if !errContains(err, "reboot budget") {
		t.Errorf("the error does not say what the consequence is: %v", err)
	}

	// The check is advisory. It must not have broken anything: an agent that
	// decides to run degraded on memory-backed storage still can.
	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Errorf("Write refused on memory-backed storage: %v", err)
	}
}

// TestCheckDurableStorage_AcceptsADisk is the half that stops this from
// being a check that fails everywhere.
func TestCheckDurableStorage_AcceptsADisk(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("filesystem type is only reported on Linux")
	}
	t.Parallel()

	// /var/tmp is on persistent storage by convention, unlike /tmp, which
	// is tmpfs on most modern distributions — including the machine this
	// was written on, which is why t.TempDir is not used here.
	dir := privateDirUnder(t, "/var/tmp")

	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckDurableStorage(context.Background()); err != nil {
		t.Fatalf("CheckDurableStorage on persistent storage: %v", err)
	}
}

func TestCheckDurableStorage_ToleratesAnAbsentDirectory(t *testing.T) {
	t.Parallel()

	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(filepath.Join(tempStateDir(t), "not-created-yet")))
	if err != nil {
		t.Fatal(err)
	}
	// A missing directory is EnsureDirectory's business to report, not this
	// one's. Reporting it here too would make a start-up sequence that calls
	// both produce the same complaint twice.
	if err := s.CheckDurableStorage(context.Background()); err != nil {
		t.Fatalf("CheckDurableStorage on an absent directory: %v, want nil", err)
	}
}

func TestCheckDurableStorage_RespectsACancelledContext(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.CheckDurableStorage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func errContains(err error, want string) bool {
	return err != nil && strings.Contains(err.Error(), want)
}
