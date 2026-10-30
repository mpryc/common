package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

// The tests in this file run real operating system processes against one
// state directory.
//
// Everything else in the suite is single-process. TestAtomicWrite_
// ReaderNeverSeesPartial runs goroutines, which exercises the Go scheduler
// and the runtime's view of the filesystem — not the kernel's. The guarantee
// this package actually makes is about processes: a reader in one process
// never observes a document another process is partway through replacing,
// and a process killed between creating a temporary file and renaming it
// leaves nothing a later process cannot recover from. Neither can be
// demonstrated without fork and exec.
//
// The mechanism is the usual Go one: the test binary re-executes itself with
// an environment variable that TestMain intercepts before any test runs.

const (
	childRoleEnv  = "NODESTATE_TEST_CHILD_ROLE"
	childDirEnv   = "NODESTATE_TEST_CHILD_DIR"
	childCompEnv  = "NODESTATE_TEST_CHILD_COMPONENT"
	childUntilEnv = "NODESTATE_TEST_CHILD_UNTIL_MS"

	// childPayloadEntries makes each document a few kilobytes, comfortably
	// more than a page, so a write that was not atomic would be observed
	// torn rather than merely suspected.
	childPayloadEntries = 60
)

func TestMain(m *testing.M) {
	if role := os.Getenv(childRoleEnv); role != "" {
		os.Exit(runChild(role))
	}
	os.Exit(m.Run())
}

// runChild is the body of a helper process. It must never call anything from
// testing: it is not running a test, it is standing in for an agent.
func runChild(role string) int {
	dir := os.Getenv(childDirEnv)
	component := nodestate.Component(os.Getenv(childCompEnv))

	switch role {
	case "writer":
		untilMS, _ := strconv.Atoi(os.Getenv(childUntilEnv))
		return childWriter(dir, component, time.Duration(untilMS)*time.Millisecond)
	default:
		fmt.Fprintf(os.Stderr, "unknown child role %q\n", role)
		return 2
	}
}

// childWriter writes as fast as it can until the deadline, or until it is
// killed. Flat out on purpose: the window this is here to probe is the one
// between creating the temporary file and renaming it over the target, and
// the only way to be inside it when a signal lands is to be inside it most
// of the time.
func childWriter(dir string, component nodestate.Component, until time.Duration) int {
	store, err := nodestate.New(component, nodestate.WithDir(dir))
	if err != nil {
		fmt.Fprintf(os.Stderr, "child %s: new store: %v\n", component, err)
		return 1
	}

	ctx := context.Background()
	deadline := time.Now().Add(until)
	for n := 0; time.Now().Before(deadline); n++ {
		doc := nodestate.NewDocument(component)
		doc.Node = "worker-0"
		doc.BootID = fmt.Sprintf("boot-%d", n)
		doc.NodeUID = strconv.Itoa(os.Getpid())
		for i := 0; i < childPayloadEntries; i++ {
			doc.AppendIO(nodestate.IOEntry{
				TS:        time.Now(),
				Op:        "write",
				Target:    fmt.Sprintf("/dev/disk/by-id/scsi-3600000000000000000000000000%05d", i),
				LatencyMS: int64(i),
				OK:        i%2 == 0,
			})
		}
		if err := store.Write(ctx, doc); err != nil {
			if errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
				continue // written and visible; not a failure for this purpose
			}
			fmt.Fprintf(os.Stderr, "child %s: write: %v\n", component, err)
			return 1
		}
	}
	return 0
}

func startChildWriter(t *testing.T, dir string, component nodestate.Component, until time.Duration) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		childRoleEnv+"=writer",
		childDirEnv+"="+dir,
		childCompEnv+"="+string(component),
		childUntilEnv+"="+strconv.Itoa(int(until.Milliseconds())),
	)
	cmd.Stderr = &prefixWriter{t: t, prefix: string(component)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting child writer %s: %v", component, err)
	}
	return cmd
}

// prefixWriter funnels a child's stderr into the test log, so a child that
// fails explains itself instead of failing the parent silently.
type prefixWriter struct {
	t      *testing.T
	prefix string
	mu     sync.Mutex
}

func (w *prefixWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if line != "" {
			w.t.Logf("[%s] %s", w.prefix, line)
		}
	}
	return len(p), nil
}

// readWhole reads a state file the way a peer does and insists it is a whole
// document. It returns false when the file is simply absent.
func readWhole(t *testing.T, path string) (*nodestate.Document, bool) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if len(raw) == 0 {
		t.Fatalf("%s is empty; a rename never exposes a zero-length file", filepath.Base(path))
	}

	var doc nodestate.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("partial document observed in %s after %d bytes: %v\nfirst 200 bytes: %.200q",
			filepath.Base(path), len(raw), err, raw)
	}
	return &doc, true
}

// TestMultiProcess_SeparateFiles is the deployment shape: two agents, two
// processes, one file each, on one node. It is the first test in the suite
// that proves atomic replace holds across processes rather than goroutines.
func TestMultiProcess_SeparateFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping a multi-process test in short mode")
	}

	dir := tempStateDir(t)
	const runFor = 3 * time.Second

	children := []*exec.Cmd{
		startChildWriter(t, dir, nodestate.ComponentSBR, runFor),
		startChildWriter(t, dir, nodestate.ComponentSNR, runFor),
	}

	reads := 0
	deadline := time.Now().Add(runFor)
	for time.Now().Before(deadline) {
		for _, c := range []nodestate.Component{nodestate.ComponentSBR, nodestate.ComponentSNR} {
			doc, ok := readWhole(t, filepath.Join(dir, c.FileName()))
			if !ok {
				continue
			}
			if doc.Node != "worker-0" {
				t.Fatalf("%s: Node = %q", c, doc.Node)
			}
			if doc.Agent.Component != c {
				t.Fatalf("%s holds a document owned by %q", c, doc.Agent.Component)
			}
			if len(doc.IO) != childPayloadEntries {
				t.Fatalf("%s: %d I/O entries, want %d; the document was observed mid-replacement",
					c, len(doc.IO), childPayloadEntries)
			}
			reads++
		}
	}

	for i, c := range children {
		if err := c.Wait(); err != nil {
			t.Errorf("child %d exited with %v", i, err)
		}
	}

	t.Logf("%d complete reads across two writing processes", reads)
	if reads < 50 {
		t.Errorf("only %d reads; the test did not exercise the race", reads)
	}

	// Each process is the sole writer of its own file, so both must be
	// present and attributable, and nothing may be left over.
	for _, c := range []nodestate.Component{nodestate.ComponentSBR, nodestate.ComponentSNR} {
		doc, ok := readWhole(t, filepath.Join(dir, c.FileName()))
		if !ok {
			t.Errorf("%s was never written", c)
			continue
		}
		if doc.Agent.PID == 0 || doc.Agent.PID == os.Getpid() {
			t.Errorf("%s: Agent.PID = %d, want a child's pid", c, doc.Agent.PID)
		}
	}
	assertNoLeftovers(t, dir)
}

// TestMultiProcess_SameFile is the contract violation: two processes writing
// one component's file, which a DaemonSet surge or a pod stuck terminating
// can produce.
//
// Records are lost — last writer wins, and that is why the contract exists.
// What must not happen is a torn read, because the whole lock-free design
// rests on a reader always seeing one whole version or another.
func TestMultiProcess_SameFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping a multi-process test in short mode")
	}

	dir := tempStateDir(t)
	const runFor = 3 * time.Second

	children := []*exec.Cmd{
		startChildWriter(t, dir, nodestate.ComponentSBR, runFor),
		startChildWriter(t, dir, nodestate.ComponentSBR, runFor),
		startChildWriter(t, dir, nodestate.ComponentSBR, runFor),
	}

	path := filepath.Join(dir, nodestate.SBRFile)
	reads, pids := 0, map[int]bool{}
	deadline := time.Now().Add(runFor)
	for time.Now().Before(deadline) {
		doc, ok := readWhole(t, path)
		if !ok {
			continue
		}
		if len(doc.IO) != childPayloadEntries {
			t.Fatalf("%d I/O entries, want %d; a reader saw a document mid-replacement",
				len(doc.IO), childPayloadEntries)
		}
		pids[doc.Agent.PID] = true
		reads++
	}

	for i, c := range children {
		if err := c.Wait(); err != nil {
			t.Errorf("child %d exited with %v", i, err)
		}
	}

	t.Logf("%d complete reads, %d distinct writing processes observed", reads, len(pids))
	if reads < 50 {
		t.Errorf("only %d reads; the test did not exercise the race", reads)
	}
	if len(pids) < 2 {
		t.Errorf("only %d writer observed; the processes did not actually contend", len(pids))
	}

	// And the violation is detectable after the fact, which is all the
	// library promises here.
	store, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatal(err)
	}
	// The parent has now written, so a child's leftover identity would show
	// as foreign on the next read. It wrote last, so this reads clean —
	// what matters is that the document is whole and attributable.
	doc, ok := readWhole(t, path)
	if !ok {
		t.Fatal("the file vanished")
	}
	if doc.Agent.PID != os.Getpid() {
		t.Errorf("Agent.PID = %d, want this process %d", doc.Agent.PID, os.Getpid())
	}
	assertNoLeftovers(t, dir)
}

// TestMultiProcess_KilledMidWrite is the crash path, for real rather than
// through fault injection.
//
// A watchdog reset, an OOM kill and a panic all arrive as "the process stops
// between two syscalls". The guarantee is that the previous version of the
// document survives whole, and that whatever the dead process left behind is
// recoverable by the next one.
func TestMultiProcess_KilledMidWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping a multi-process test in short mode")
	}

	dir := tempStateDir(t)
	path := filepath.Join(dir, nodestate.SBRFile)

	const rounds = 25
	killed, observed := 0, 0

	for i := 0; i < rounds; i++ {
		child := startChildWriter(t, dir, nodestate.ComponentSBR, 10*time.Second)

		// Let it get going, then kill it at a point that varies across
		// rounds so the signal lands at different places in the sequence.
		time.Sleep(time.Duration(5+(i*7)%40) * time.Millisecond)
		if err := child.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatalf("round %d: kill: %v", i, err)
		}
		_ = child.Wait()
		killed++

		// Whatever state the kill interrupted, a reader sees a whole
		// document or no document.
		if _, ok := readWhole(t, path); ok {
			observed++
		}
	}

	t.Logf("%d processes killed mid-write, %d rounds left a readable document", killed, observed)
	if observed == 0 {
		t.Fatal("no round produced a readable document; the children never got far enough to be a test")
	}

	// Count what the kills left behind before anything cleans up.
	orphans := 0
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "."+nodestate.SBRFile+".tmp-") {
			orphans++
		}
	}
	t.Logf("%d orphaned temporary files survived the kills", orphans)

	// The next process to start reclaims them, which is the half of the
	// guarantee that is this package's job rather than the kernel's.
	store, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(dir),
		nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{OrphanGrace: time.Nanosecond}))
	if err != nil {
		t.Fatal(err)
	}
	report, err := store.Cleanup(context.Background())
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if report.TemporaryFilesRemoved != orphans {
		t.Errorf("reclaimed %d temporary files, want %d", report.TemporaryFilesRemoved, orphans)
	}
	assertNoLeftovers(t, dir)

	// And the surviving document is still usable, not merely parseable.
	res, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after %d kills: %v", rounds, err)
	}
	if res.Status != nodestate.StatusOK && res.Status != nodestate.StatusAbsent {
		t.Errorf("Status = %v after kills, want OK or Absent: a torn file reached the parser (cause: %v)",
			res.Status, res.Cause)
	}
	if res.HistoryLost() {
		t.Errorf("history was lost to a kill, which atomic replace is supposed to prevent: %v", res.Cause)
	}
}

// assertNoLeftovers checks that the directory holds only state files.
func assertNoLeftovers(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch e.Name() {
		case nodestate.SBRFile, nodestate.SNRFile:
		default:
			t.Errorf("leftover file %q in the state directory", e.Name())
		}
	}
}
