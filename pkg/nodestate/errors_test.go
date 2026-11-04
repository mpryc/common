package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

func TestPathError(t *testing.T) {
	t.Parallel()

	cause := errors.New("underlying")
	err := &nodestate.PathError{Op: "rename", Path: "/var/lib/medik8s/state/sbr.json", Err: cause}

	msg := err.Error()
	for _, want := range []string{"rename", "/var/lib/medik8s/state/sbr.json", "underlying"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, want it to mention %q", msg, want)
		}
	}
	if !errors.Is(err, cause) {
		t.Error("PathError does not unwrap to its cause")
	}

	var target *nodestate.PathError
	if !errors.As(err, &target) || target.Op != "rename" {
		t.Error("errors.As did not recover the PathError")
	}
}

func TestSchemaVersionError(t *testing.T) {
	t.Parallel()

	err := &nodestate.SchemaVersionError{Found: 42, Min: 1, Max: 1}
	if !strings.Contains(err.Error(), "42") {
		t.Errorf("Error() = %q, want it to name the version found", err.Error())
	}
	if !errors.Is(err, nodestate.ErrSchemaUnsupported) {
		t.Error("SchemaVersionError does not unwrap to ErrSchemaUnsupported")
	}

	var target *nodestate.SchemaVersionError
	if !errors.As(err, &target) || target.Found != 42 {
		t.Error("errors.As did not recover the SchemaVersionError")
	}
}

// TestLoad_ErrorsCarryTheirPath matters because the operator reading the log
// has several files to choose from and needs to know which one broke.
func TestLoad_ErrorsCarryTheirPath(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	mustWrite(t, s.Path(nodestate.ComponentSBR), "{bad")

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var pe *nodestate.PathError
	if !errors.As(res.Cause, &pe) {
		t.Fatalf("Cause = %v, want a *PathError", res.Cause)
	}
	if pe.Path != s.Path(nodestate.ComponentSBR) {
		t.Errorf("PathError.Path = %q, want %q", pe.Path, s.Path(nodestate.ComponentSBR))
	}
	if !strings.Contains(pe.Path, dir) {
		t.Errorf("PathError.Path = %q, want it under %q", pe.Path, dir)
	}
}

func TestStoreAccessors(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSNR, nodestate.WithWriterID("agent-7"))
	if s.Directory() != dir {
		t.Errorf("Directory() = %q, want %q", s.Directory(), dir)
	}
	if s.WriterID() != "agent-7" {
		t.Errorf("WriterID() = %q, want agent-7", s.WriterID())
	}
	if s.Self() != nodestate.ComponentSNR {
		t.Errorf("Self() = %q", s.Self())
	}

	// A fixed writer id must reach the file, or a tool that audits the
	// directory cannot attribute a document to a process.
	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSNR)); err != nil {
		t.Fatal(err)
	}
	doc, err := s.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if doc.WriterID != "agent-7" {
		t.Errorf("WriterID on disk = %q, want agent-7", doc.WriterID)
	}
}

func TestStore_PathCannotEscapeTheStateDirectory(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	// Path reduces the component to a base name, so even an unvalidated value
	// stays inside the directory. Validation is still the real defence; this
	// is the belt to its braces.
	got := s.Path(nodestate.Component("../../etc/passwd"))
	if !strings.HasPrefix(got, dir) {
		t.Fatalf("Path escaped the state directory: %q", got)
	}
}

func TestOptions_RejectBadValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  nodestate.Option
	}{
		{"nil clock", nodestate.WithClock(nil)},
		{"empty writer id", nodestate.WithWriterID("")},
		{"negative quarantine retention", nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{QuarantineRetention: -1})},
		{"negative orphan grace", nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{OrphanGrace: -time.Second})},
		{"negative quarantine age", nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{QuarantineMaxAge: -2 * time.Second})},
		{"negative preserved bytes", nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{MaxPreservedBytes: -5})},
		{"negative retention", nodestate.WithRetention(nodestate.RetentionPolicy{MaxIOEntries: -5})},
		{"inconsistent security policy", nodestate.WithSecurityPolicy(nodestate.SecurityPolicy{
			DirMode: 0o777, FileMode: 0o600, ForbiddenDirModeBits: 0o077,
		})},
		{"security policy with no owner access", nodestate.WithSecurityPolicy(nodestate.SecurityPolicy{
			DirMode: 0, FileMode: 0o600,
		})},
		{"negative allowed uid", nodestate.WithSecurityPolicy(nodestate.SecurityPolicy{
			DirMode: 0o700, FileMode: 0o600, AllowedUIDs: []int{-1},
		})},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := nodestate.New(nodestate.ComponentSBR, tt.opt); err == nil {
				t.Fatal("New succeeded, want an error")
			}
		})
	}

	if _, err := nodestate.New(nodestate.ComponentSBR, nil); err == nil {
		t.Error("New accepted a nil option")
	}
}

func TestOptions_AcceptValidValues(t *testing.T) {
	t.Parallel()

	policy := nodestate.DefaultRetention()
	policy.MaxIOEntries = 7

	clk := nodestatetest.NewFakeClock(time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC))
	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(tempStateDir(t)),
		nodestate.WithRetention(policy),
		nodestate.WithClock(clk),
		nodestate.WithMaxDocumentBytes(1<<16),
		nodestate.WithMaxReadBytes(1<<16),
		nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{QuarantineRetention: 1}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mustInitialize(t, s)
	doc, err := s.Update(context.Background(), func(d *nodestate.Document) error {
		for i := 0; i < 20; i++ {
			d.AppendIO(nodestate.IOEntry{TS: clk.Now(), OK: true})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.IO) != 7 {
		t.Errorf("got %d I/O entries, want the configured 7", len(doc.IO))
	}
}

// TestSecurityPolicy_AllowedUIDs covers the explicit owner list, which is how
// a deployment that runs the agent as a non-root user states that intent.
func TestSecurityPolicy_AllowedUIDs(t *testing.T) {
	t.Parallel()

	policy := nodestate.DefaultSecurityPolicy()
	policy.AllowedUIDs = []int{os.Geteuid()}

	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(tempStateDir(t)), nodestate.WithSecurityPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureDirectory(context.Background()); err != nil {
		t.Fatalf("EnsureDirectory with the running user allowed: %v", err)
	}

	policy.AllowedUIDs = []int{os.Geteuid() + 1000}
	s2, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(tempStateDir(t)), nodestate.WithSecurityPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	// On Unix the filesystem reports an owner, so excluding the running user
	// must make the check bite. Elsewhere ownership is not reported and only
	// the permission bits are enforced, which this directory satisfies.
	err = s2.EnsureDirectory(context.Background())
	switch runtime.GOOS {
	case "windows", "js", "plan9":
		if err != nil {
			t.Fatalf("EnsureDirectory = %v, want nil where ownership is not reported", err)
		}
	default:
		if !errors.Is(err, nodestate.ErrInsecureOwner) {
			t.Fatalf("EnsureDirectory error = %v, want ErrInsecureOwner", err)
		}
	}
}

func TestEnsureDirectory_SurfacesACreationFailure(t *testing.T) {
	t.Parallel()

	// A regular file where a parent directory belongs: MkdirAll cannot
	// proceed, and the caller must be told rather than left to discover it on
	// the first write.
	root := tempStateDir(t)
	blocker := root + "/blocker"
	mustWrite(t, blocker, "")

	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(blocker+"/state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureDirectory(context.Background()); err == nil {
		t.Fatal("EnsureDirectory succeeded with a regular file as a parent")
	}
}

// TestConditionEntryJSONRoundTrip exercises the preservation machinery on a
// type the rest of the suite only touches indirectly.
func TestConditionEntryJSONRoundTrip(t *testing.T) {
	t.Parallel()

	const raw = `{"type":"SBRStorageUnhealthy","status":"True","reason":"IOTimeout","futureKey":[1,2]}`

	var entry nodestate.ConditionEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatal(err)
	}
	if entry.Type != "SBRStorageUnhealthy" || entry.Status != "True" || entry.Reason != "IOTimeout" {
		t.Fatalf("decoded %+v", entry)
	}

	out, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(t, got["futureKey"], json.RawMessage(`[1,2]`)) {
		t.Errorf("futureKey = %s", got["futureKey"])
	}
}

func TestDocumentJSON_RejectsAnUnencodablePreservedMember(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Unknown = map[string]json.RawMessage{"broken": json.RawMessage(`{not json`)}

	if _, err := json.Marshal(doc); err == nil {
		t.Fatal("marshal succeeded with an invalid preserved member; the library would write a file it cannot read back")
	}
}

func TestMergeReboots_TiesAreOrderedDeterministically(t *testing.T) {
	t.Parallel()

	at := time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.AppendReboot(nodestate.RebootEntry{TS: at, Source: nodestate.ComponentSNR, Reason: "b"})
	doc.AppendReboot(nodestate.RebootEntry{TS: at, Source: nodestate.ComponentSBR, Reason: "a"})

	first := nodestate.MergeReboots(doc)
	for i := 0; i < 20; i++ {
		again := nodestate.MergeReboots(doc)
		for j := range first {
			if first[j].Reason != again[j].Reason {
				t.Fatalf("merge order is not deterministic: %q then %q at index %d", first[j].Reason, again[j].Reason, j)
			}
		}
	}
}

func TestMergeReboots_FillsGapsFromEitherWriter(t *testing.T) {
	t.Parallel()

	at := time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)
	ref := &nodestate.CRRef{NodeUID: "node-uid", UID: "uid-1", Generation: 1}

	sparse := nodestate.NewDocument(nodestate.ComponentSBR)
	sparse.AppendReboot(nodestate.RebootEntry{TS: at, Remediation: ref})

	full := nodestate.NewDocument(nodestate.ComponentSNR)
	full.AppendReboot(nodestate.RebootEntry{
		TS: at, Remediation: ref, Source: nodestate.ComponentSNR, Method: nodestate.MethodSoftware,
		Reason: "RemediationCR", EpisodeID: "ep-1", BeforeBootID: "b0", AfterBootID: "b1",
	})

	merged := nodestate.MergeReboots(sparse, full)
	if len(merged) != 1 {
		t.Fatalf("got %d entries, want 1", len(merged))
	}
	got := merged[0]
	if got.EpisodeID != "ep-1" || got.BeforeBootID != "b0" || got.AfterBootID != "b1" {
		t.Errorf("gaps were not filled: %+v", got)
	}
	if got.Method != nodestate.MethodSoftware || got.Source != nodestate.ComponentSNR || got.Reason != "RemediationCR" {
		t.Errorf("descriptive fields were not filled: %+v", got)
	}
}
