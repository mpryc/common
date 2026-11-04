package nodestate_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestTwoStoresInOneProcessAreNotForeignToEachOther is the false positive the
// per-Store random identity produced.
//
// Two Stores for the same component in one process is a consumer mistake —
// one built in main, one in a controller — but the shared lock already makes
// it safe. Reporting it as a foreign write said the opposite, and told the
// consumer to raise an alert about a condition the library had just handled.
func TestTwoStoresInOneProcessAreNotForeignToEachOther(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	ctx := context.Background()

	a, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	b, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}

	if a.WriterID() != b.WriterID() {
		t.Errorf("two Stores in one process have different writer ids (%q, %q); "+
			"the identity must describe the process", a.WriterID(), b.WriterID())
	}

	mustInitialize(t, a)
	if _, err := a.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Fatal(err)
	}

	res, err := a.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.ForeignWrite {
		t.Error("ForeignWrite = true for a second Store in the same process, which the shared lock already serialises")
	}
}

// TestWriterIdentityIsStableAcrossStoresAndCarriesTheProcess checks the parts
// are actually present, because an opaque identifier that happens to be
// random would pass the test above too.
func TestWriterIdentityIsStableAcrossStoresAndCarriesTheProcess(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	id := s.WriterID()

	parts := strings.Split(id, "/")
	if len(parts) != 3 {
		t.Fatalf("writer id %q is not bootID/pid/startedAt", id)
	}
	if parts[0] == "" {
		t.Error("the boot id component is empty")
	}
	if pid, err := strconv.Atoi(parts[1]); err != nil || pid != os.Getpid() {
		t.Errorf("pid component = %q, want %d", parts[1], os.Getpid())
	}
	if started, err := strconv.ParseInt(parts[2], 10, 64); err != nil || started <= 0 {
		t.Errorf("start time component = %q", parts[2])
	}
}

// TestWriteStampsTheProcessOntoTheDocument: PID and start time are what make
// a second live writer recognisable on the first read, rather than only
// after this process has written and been overwritten.
func TestWriteStampsTheProcessOntoTheDocument(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatal(err)
	}
	doc, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if doc.Agent.PID != os.Getpid() {
		t.Errorf("Agent.PID = %d, want %d", doc.Agent.PID, os.Getpid())
	}
	if doc.Agent.StartedAt.IsZero() {
		t.Error("Agent.StartedAt was not stamped")
	}
	if !strings.Contains(doc.WriterID, strconv.Itoa(os.Getpid())) {
		t.Errorf("WriterID %q does not identify this process", doc.WriterID)
	}
}

// TestWriteKeepsACallerSuppliedStartedAt: an agent that knows when it really
// started — from its own main, not from when it first touched this library —
// should not have that overwritten.
func TestWriteKeepsACallerSuppliedStartedAt(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	actualStart := doc.UpdatedAt.AddDate(-1, 0, 0) // deliberately distinctive
	doc.Agent.StartedAt = actualStart

	if err := s.Write(ctx, doc); err != nil {
		t.Fatal(err)
	}
	back, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !back.Agent.StartedAt.Equal(actualStart) {
		t.Errorf("Agent.StartedAt = %v, want the caller's %v", back.Agent.StartedAt, actualStart)
	}
}

// TestRestartIsNotAForeignWrite is the case the per-Store identity got wrong
// in the other direction: a new value on every construction made this
// process's predecessor indistinguishable from a stranger, so the only
// reason a restart did not report foreign was the haveWritten gate.
func TestRestartIsNotAForeignWrite(t *testing.T) {
	t.Parallel()

	dir := tempStateDir(t)
	ctx := context.Background()

	first, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	mustInitialize(t, first)
	if _, err := first.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Fatal(err)
	}

	// A restart within the same process cannot be simulated exactly, but a
	// Store that has written and then re-reads its own predecessor's file is
	// the same comparison.
	second, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Fatal(err)
	}
	res, err := second.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.ForeignWrite {
		t.Error("ForeignWrite = true after re-reading a file written by the same process")
	}
}

// TestAnotherProcessIsStillDetected guards against the fix above papering
// over the thing the mechanism exists for.
func TestAnotherProcessIsStillDetected(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithWriterID("some-other-boot/4242/1700000000000000000"))
	other, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(s.Directory()))
	ctx := context.Background()

	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatal(err)
	}
	if err := other.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatal(err)
	}

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !res.ForeignWrite {
		t.Error("ForeignWrite = false; a genuinely different writer went unnoticed")
	}
}
