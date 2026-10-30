package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// tempStateDir returns a private temporary directory suitable as a state
// directory.
//
// Two adjustments. The mode is set explicitly because t.TempDir's depends on
// the umask and on the Go release, and the library requires a directory no
// other user can act on. The path is resolved because the library refuses a
// state directory reached through a symlink, and a temporary directory is
// one on some platforms — macOS puts them under /var, which is a link to
// /private/var. A consumer writing its own tests will need the same.
func tempStateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolve %s: %v", dir, err)
	}
	return resolved
}

func newStore(t *testing.T, self nodestate.Component, opts ...nodestate.Option) (*nodestate.Store, string) {
	t.Helper()
	dir := tempStateDir(t)
	all := append([]nodestate.Option{nodestate.WithDir(dir)}, opts...)
	s, err := nodestate.New(self, all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, dir
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		self nodestate.Component
		opts []nodestate.Option
	}{
		{"empty component", nodestate.Component(""), nil},
		{"traversal component", nodestate.Component("../x"), nil},
		{"relative dir", nodestate.ComponentSBR, []nodestate.Option{nodestate.WithDir("relative/path")}},
		{"empty dir", nodestate.ComponentSBR, []nodestate.Option{nodestate.WithDir("")}},
		{"nil filesystem", nodestate.ComponentSBR, []nodestate.Option{nodestate.WithFileSystem(nil)}},
		{"negative read limit", nodestate.ComponentSBR, []nodestate.Option{nodestate.WithMaxReadBytes(-1)}},
		{"zero document limit", nodestate.ComponentSBR, []nodestate.Option{nodestate.WithMaxDocumentBytes(0)}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := nodestate.New(tt.self, tt.opts...); err == nil {
				t.Fatal("New() succeeded, want an error")
			}
		})
	}
}

func TestNew_DefaultsToTheContractDirectory(t *testing.T) {
	t.Parallel()

	s, err := nodestate.New(nodestate.ComponentSBR)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := s.Path(nodestate.ComponentSBR), filepath.Join(nodestate.Dir, nodestate.SBRFile); got != want {
		t.Errorf("Path = %q, want %q", got, want)
	}
	if got, want := s.Path(nodestate.ComponentSNR), filepath.Join(nodestate.Dir, nodestate.SNRFile); got != want {
		t.Errorf("Path = %q, want %q", got, want)
	}
	if s.Self() != nodestate.ComponentSBR {
		t.Errorf("Self = %q", s.Self())
	}
}

func TestNew_DoesNoIO(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestatetest.OSFileSystem())
	dir := t.TempDir()
	if _, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir), nodestate.WithFileSystem(rec)); err != nil {
		t.Fatalf("New: %v", err)
	}
	if ops := rec.Operations(); len(ops) != 0 {
		t.Errorf("New performed file operations %v; construction must be pure so that a caller can build a Store before the host path is ready", ops)
	}
}

// TestWriterOwnership is the invariant that removes the need for a lock: each
// file has exactly one writer, and the library refuses to break that even when
// asked directly.
func TestWriterOwnership(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	// Seed a peer file written by its legitimate owner.
	peer, _ := nodestate.New(nodestate.ComponentSNR, nodestate.WithDir(dir))
	peerDoc := nodestate.NewDocument(nodestate.ComponentSNR)
	peerDoc.Node = "worker-0"
	if err := peer.Write(ctx, peerDoc); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(dir, nodestate.SNRFile))
	if err != nil {
		t.Fatal(err)
	}

	forged := nodestate.NewDocument(nodestate.ComponentSBR)
	forged.Node = "forged"

	err = s.WriteTo(ctx, nodestate.ComponentSNR, forged)
	if !errors.Is(err, nodestate.ErrNotOwner) {
		t.Fatalf("WriteTo(peer) error = %v, want ErrNotOwner", err)
	}

	after, err := os.ReadFile(filepath.Join(dir, nodestate.SNRFile))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Error("the peer's file was modified by a non-owner")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("directory contains %d entries, want 1: a refused write must leave no temporary file", len(entries))
	}
}

func TestWriterOwnership_UpdateAndQuarantineAreSelfOnly(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	peerPath := filepath.Join(dir, nodestate.SNRFile)
	if err := os.WriteFile(peerPath, []byte(`{"schemaVersion":1,`), 0o600); err != nil {
		t.Fatal(err)
	}

	// Reading a corrupt peer must not quarantine it: only its owner may do that.
	doc, err := s.ReadPeer(ctx, nodestate.ComponentSNR)
	if err != nil || doc != nil {
		t.Fatalf("ReadPeer = %v, %v; want nil, nil", doc, err)
	}
	if _, err := os.Stat(peerPath); err != nil {
		t.Fatalf("the peer's corrupt file was removed or renamed by a non-owner: %v", err)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.corrupt"))
	if len(matches) != 0 {
		t.Errorf("a non-owner quarantined the peer's file: %v", matches)
	}
}

func TestWrite_RejectsAMismatchedDocumentComponent(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	doc := nodestate.NewDocument(nodestate.ComponentSNR)

	if err := s.Write(context.Background(), doc); !errors.Is(err, nodestate.ErrNotOwner) {
		t.Fatalf("Write of a document stamped with another component: error = %v, want ErrNotOwner", err)
	}
}

func TestWrite_RejectsNilDocument(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	if err := s.Write(context.Background(), nil); err == nil {
		t.Fatal("Write(nil) succeeded, want an error")
	}
}

func TestWrite_StampsMetadata(t *testing.T) {
	t.Parallel()

	clk := nodestatetest.NewFakeClock(time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC))
	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithClock(clk))
	ctx := context.Background()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	if err := s.Write(ctx, doc); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if doc.Revision != 1 {
		t.Errorf("Revision = %d, want 1", doc.Revision)
	}
	if !doc.UpdatedAt.Equal(clk.Now()) {
		t.Errorf("UpdatedAt = %v, want %v", doc.UpdatedAt, clk.Now())
	}
	if doc.WriterID == "" {
		t.Error("WriterID was not stamped")
	}
	if doc.SchemaVersion != nodestate.Schema {
		t.Errorf("SchemaVersion = %d, want %d", doc.SchemaVersion, nodestate.Schema)
	}

	clk.Advance(time.Minute)
	if err := s.Write(ctx, doc); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if doc.Revision != 2 {
		t.Errorf("Revision = %d, want 2", doc.Revision)
	}

	raw, err := os.ReadFile(filepath.Join(dir, nodestate.SBRFile))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(raw), "\n") {
		t.Error("the encoded document should end with a newline so that it is greppable on a host")
	}
}

func TestWrite_FileAndDirectoryModes(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := di.Mode().Perm(); got != 0o700 {
		t.Errorf("directory mode = %#o, want 0700", got)
	}

	fi, err := os.Stat(filepath.Join(dir, nodestate.SBRFile))
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("file mode = %#o, want 0600", got)
	}
}

func TestLoad_AbsentFileIsAFreshDocument(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Status != nodestate.StatusAbsent {
		t.Errorf("Status = %v, want StatusAbsent", res.Status)
	}
	if res.Document == nil {
		t.Fatal("Document is nil; Load must always return a usable document")
	}
	if res.Document.Agent.Component != nodestate.ComponentSBR {
		t.Errorf("fresh document component = %q", res.Document.Agent.Component)
	}
	if res.HistoryLost() {
		t.Error("an absent file is a first boot, not lost history")
	}
}

func TestLoad_RoundTrip(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Node = "worker-7"
	doc.NodeUID = "uid-7"
	doc.BootID = "boot-a"
	doc.Config = map[string]string{"arbitrationMode": "PerNode"}
	doc.AppendReboot(nodestate.RebootEntry{
		TS:          time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
		Source:      nodestate.ComponentSBR,
		Method:      nodestate.MethodWatchdog,
		Reason:      "TotalOutage",
		EpisodeID:   "ep-1",
		Remediation: &nodestate.CRRef{Kind: "SBR", Name: "n", UID: "u", Generation: 3},
		Completed:   true,
	})
	if err := s.Write(ctx, doc); err != nil {
		t.Fatalf("Write: %v", err)
	}

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Status != nodestate.StatusOK {
		t.Errorf("Status = %v, want StatusOK", res.Status)
	}
	got := res.Document
	if got.Node != "worker-7" || got.NodeUID != "uid-7" || got.BootID != "boot-a" {
		t.Errorf("identity round-trip failed: %+v", got)
	}
	if len(got.Reboots) != 1 || got.Reboots[0].Remediation.UID != "u" || got.Reboots[0].Remediation.Generation != 3 {
		t.Errorf("reboot round-trip failed: %+v", got.Reboots)
	}
	if got.Config["arbitrationMode"] != "PerNode" {
		t.Errorf("config round-trip failed: %+v", got.Config)
	}
}

// TestCorruptQuarantine covers the recovery path: a damaged own file is moved
// aside with its bytes intact, and the caller is handed a usable document plus
// an explicit signal that history was lost.
func TestCorruptQuarantine(t *testing.T) {
	t.Parallel()

	corruptions := map[string]string{
		"truncated json":    `{"schemaVersion":1,"node":"worker-0","rebo`,
		"empty file":        ``,
		"whitespace only":   "   \n\t ",
		"not an object":     `["a","b"]`,
		"trailing garbage":  `{"schemaVersion":1}{"schemaVersion":1}`,
		"binary":            "\x00\x01\x02\x03",
		"wrong field types": `{"schemaVersion":"one"}`,
	}

	for name, content := range corruptions {
		name, content := name, content
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, dir := newStore(t, nodestate.ComponentSBR)
			path := filepath.Join(dir, nodestate.SBRFile)
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}

			res, err := s.Load(context.Background())
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if res.Status != nodestate.StatusRecovered {
				t.Fatalf("Status = %v, want StatusRecovered", res.Status)
			}
			if !res.HistoryLost() {
				t.Error("HistoryLost() = false, want true")
			}
			if res.Cause == nil {
				t.Error("Cause is nil; the caller needs it for the event it must emit")
			}
			if res.Document == nil || len(res.Document.Reboots) != 0 {
				t.Fatalf("Document = %+v, want a fresh one", res.Document)
			}
			if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("the corrupt file is still in place: %v", err)
			}
			if res.QuarantinePath == "" {
				t.Fatal("QuarantinePath is empty")
			}
			if !strings.HasSuffix(res.QuarantinePath, ".corrupt") {
				t.Errorf("QuarantinePath = %q, want a *.corrupt suffix", res.QuarantinePath)
			}
			saved, err := os.ReadFile(res.QuarantinePath)
			if err != nil {
				t.Fatalf("read quarantine: %v", err)
			}
			if string(saved) != content {
				t.Errorf("quarantined bytes = %q, want %q: the evidence must be preserved byte for byte", saved, content)
			}
		})
	}
}

func TestCorruptQuarantine_SubsequentWriteSucceeds(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(dir, nodestate.SBRFile), []byte(`{`), 0o600); err != nil {
		t.Fatal(err)
	}

	doc, err := s.Update(ctx, func(d *nodestate.Document) error {
		d.Node = "worker-0"
		return nil
	})
	if err != nil {
		t.Fatalf("Update after corruption: %v", err)
	}
	if doc.Node != "worker-0" {
		t.Errorf("Node = %q", doc.Node)
	}

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != nodestate.StatusOK || res.Document.Node != "worker-0" {
		t.Errorf("after recovery: status %v, doc %+v", res.Status, res.Document)
	}
}

func TestCorruptQuarantine_RetainsABoundedNumberOfFiles(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithHousekeeping(nodestate.HousekeepingPolicy{QuarantineRetention: 2}))
	ctx := context.Background()
	path := filepath.Join(dir, nodestate.SBRFile)

	for i := 0; i < 5; i++ {
		if err := os.WriteFile(path, []byte("{bad"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Load(ctx); err != nil {
			t.Fatalf("Load %d: %v", i, err)
		}
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.corrupt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Errorf("got %d quarantine files, want 2: a boot loop must not fill the host path (%v)", len(matches), matches)
	}
}

func TestLoad_SchemaTooNewIsQuarantinedNotInterpreted(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	raw := `{"schemaVersion":99,"node":"worker-0","reboots":[{"ts":"2024-01-01T00:00:00Z","source":"sbr","completed":true}]}`
	if err := os.WriteFile(filepath.Join(dir, nodestate.SBRFile), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Status != nodestate.StatusIncompatible {
		t.Fatalf("Status = %v, want StatusIncompatible", res.Status)
	}
	if !errors.Is(res.Cause, nodestate.ErrSchemaUnsupported) {
		t.Errorf("Cause = %v, want ErrSchemaUnsupported", res.Cause)
	}
	if !res.HistoryLost() {
		t.Error("HistoryLost() = false; a document we cannot interpret is lost history to this caller")
	}
	if len(res.Document.Reboots) != 0 {
		t.Error("records from an unsupported schema version must not be interpreted")
	}
	saved, err := os.ReadFile(res.QuarantinePath)
	if err != nil {
		t.Fatalf("read quarantine: %v", err)
	}
	if string(saved) != raw {
		t.Error("the newer document's bytes were not preserved")
	}
}

func TestLoad_SchemaTooOldIsRejected(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	if err := os.WriteFile(filepath.Join(dir, nodestate.SBRFile), []byte(`{"schemaVersion":0}`), 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Status != nodestate.StatusIncompatible {
		t.Errorf("Status = %v, want StatusIncompatible", res.Status)
	}
}

func TestLoad_RefusesAnOversizedFile(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithMaxReadBytes(1024))
	big := make([]byte, 4096)
	for i := range big {
		big[i] = 'x'
	}
	if err := os.WriteFile(filepath.Join(dir, nodestate.SBRFile), big, 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Status != nodestate.StatusRecovered {
		t.Fatalf("Status = %v, want StatusRecovered", res.Status)
	}
	if !errors.Is(res.Cause, nodestate.ErrTooLarge) {
		t.Errorf("Cause = %v, want ErrTooLarge", res.Cause)
	}
}

func TestWrite_RefusesAnOversizedDocument(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithMaxDocumentBytes(512))
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Config = map[string]string{"big": strings.Repeat("x", 1024)}

	err := s.Write(context.Background(), doc)
	if !errors.Is(err, nodestate.ErrTooLarge) {
		t.Fatalf("Write error = %v, want ErrTooLarge", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, nodestate.SBRFile)); !errors.Is(statErr, fs.ErrNotExist) {
		t.Error("a refused write must not create the target file")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("a refused write left %d files behind", len(entries))
	}
}

// TestReadPeer_ToleratesEverything encodes the rule that a peer's bookkeeping
// problem must never stop this agent.
func TestReadPeer_ToleratesEverything(t *testing.T) {
	t.Parallel()

	type setup func(t *testing.T, dir, path string)

	cases := map[string]setup{
		"absent": func(t *testing.T, dir, path string) {},
		"empty": func(t *testing.T, dir, path string) {
			mustWrite(t, path, "")
		},
		"truncated": func(t *testing.T, dir, path string) {
			mustWrite(t, path, `{"schemaVersion":1,"node":`)
		},
		"wrong schema": func(t *testing.T, dir, path string) {
			mustWrite(t, path, `{"schemaVersion":4096}`)
		},
		"json array": func(t *testing.T, dir, path string) {
			mustWrite(t, path, `[]`)
		},
		"symlink": func(t *testing.T, dir, path string) {
			target := filepath.Join(dir, "target.json")
			mustWrite(t, target, `{"schemaVersion":1,"node":"attacker"}`)
			if err := os.Symlink(target, path); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
		},
		"dangling symlink": func(t *testing.T, dir, path string) {
			if err := os.Symlink(filepath.Join(dir, "nope.json"), path); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
		},
		"directory": func(t *testing.T, dir, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		},
		"oversized": func(t *testing.T, dir, path string) {
			mustWrite(t, path, strings.Repeat("x", 32<<10))
		},
		"world writable": func(t *testing.T, dir, path string) {
			mustWrite(t, path, `{"schemaVersion":1,"node":"worker-0"}`)
			if err := os.Chmod(path, 0o666); err != nil {
				t.Fatal(err)
			}
		},
	}

	for name, prepare := range cases {
		name, prepare := name, prepare
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithMaxReadBytes(8<<10))
			path := filepath.Join(dir, nodestate.SNRFile)
			prepare(t, dir, path)

			doc, err := s.ReadPeer(context.Background(), nodestate.ComponentSNR)
			if err != nil {
				t.Fatalf("ReadPeer returned error %v; it must never fail the caller for a peer's problem", err)
			}
			if doc != nil {
				t.Fatalf("ReadPeer returned a document %+v, want nil", doc)
			}
		})
	}
}

func TestReadPeer_ReturnsAHealthyPeer(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	peer, _ := nodestate.New(nodestate.ComponentSNR, nodestate.WithDir(dir))
	doc := nodestate.NewDocument(nodestate.ComponentSNR)
	doc.Node = "worker-0"
	if err := peer.Write(ctx, doc); err != nil {
		t.Fatal(err)
	}

	got, err := s.ReadPeer(ctx, nodestate.ComponentSNR)
	if err != nil {
		t.Fatalf("ReadPeer: %v", err)
	}
	if got == nil || got.Node != "worker-0" {
		t.Fatalf("ReadPeer = %+v", got)
	}
}

func TestReadPeer_RejectsAnInvalidComponent(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	if _, err := s.ReadPeer(context.Background(), nodestate.Component("../../etc/shadow")); !errors.Is(err, nodestate.ErrInvalidComponent) {
		t.Fatalf("error = %v, want ErrInvalidComponent: a bad component name is a programming error, not a peer fault", err)
	}
}

func TestReadAll_MergesWhatItCanAndSkipsTheRest(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	own := nodestate.NewDocument(nodestate.ComponentSBR)
	own.Node = "worker-0"
	if err := s.Write(ctx, own); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, nodestate.SNRFile), `{"schemaVersion":1,`)

	docs := s.ReadAll(ctx, nodestate.ComponentSBR, nodestate.ComponentSNR)
	if len(docs) != 1 {
		t.Fatalf("got %d documents, want 1: %v", len(docs), docs)
	}
	if docs[nodestate.ComponentSBR] == nil {
		t.Error("own document missing from ReadAll")
	}

	// ReadAll must not quarantine anything: it is a read-only view.
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.corrupt")); len(matches) != 0 {
		t.Errorf("ReadAll quarantined files: %v", matches)
	}

	// With no components named, it reads the known set.
	if got := s.ReadAll(ctx); len(got) != 1 {
		t.Errorf("ReadAll() with no arguments returned %d documents, want 1", len(got))
	}
}

func TestUpdate_AppliesRetentionAndPersists(t *testing.T) {
	t.Parallel()

	clk := nodestatetest.NewFakeClock(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithClock(clk))
	ctx := context.Background()

	for i := 0; i < 150; i++ {
		if _, err := s.Update(ctx, func(d *nodestate.Document) error {
			d.AppendIO(nodestate.IOEntry{TS: clk.Now(), OK: true})
			return nil
		}); err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
		clk.Advance(time.Second)
	}

	doc, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.IO) != nodestate.DefaultMaxIOEntries {
		t.Errorf("IO has %d entries, want %d", len(doc.IO), nodestate.DefaultMaxIOEntries)
	}
	if doc.Revision != 150 {
		t.Errorf("Revision = %d, want 150", doc.Revision)
	}
}

func TestUpdate_ErrNoChangeSkipsTheWrite(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestatetest.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))
	ctx := context.Background()

	if _, err := s.Update(ctx, func(d *nodestate.Document) error { return nil }); err != nil {
		t.Fatal(err)
	}
	rec.Reset()

	doc, err := s.Update(ctx, func(d *nodestate.Document) error { return nodestate.ErrNoChange })
	if err != nil {
		t.Fatalf("Update returned %v, want nil: ErrNoChange is not a failure", err)
	}
	if doc == nil {
		t.Fatal("Update returned a nil document")
	}
	for _, op := range rec.Operations() {
		if op.Op == nodestatetest.OpRename {
			t.Errorf("ErrNoChange still replaced the file: %v", rec.Operations())
			break
		}
	}
}

func TestUpdate_MutatorErrorAbortsWithoutWriting(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	sentinel := errors.New("caller decided not to proceed")
	_, err := s.Update(ctx, func(d *nodestate.Document) error {
		d.Node = "half-applied"
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Update error = %v, want the mutator's error", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, nodestate.SBRFile)); !errors.Is(statErr, fs.ErrNotExist) {
		t.Error("a failed mutator must not leave a written file")
	}
}

func TestUpdate_NilMutator(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	if _, err := s.Update(context.Background(), nil); err == nil {
		t.Fatal("Update(nil) succeeded, want an error")
	}
}

func TestUpdate_ReportsForeignModification(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	if _, err := s.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Fatal(err)
	}

	// Simulate a second process writing the file we own. This must never happen;
	// if it does, the library reports it rather than silently losing a record.
	foreign := nodestate.NewDocument(nodestate.ComponentSBR)
	foreign.Node = "worker-0"
	foreign.WriterID = "some-other-process"
	foreign.Revision = 97
	raw, err := json.Marshal(foreign)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), string(raw))

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !res.ForeignWrite {
		t.Error("ForeignWrite = false, want true")
	}

	// The default policy favours liveness: the write still goes through, and the
	// revision continues from what is on disk so it never goes backwards.
	doc, err := s.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil })
	if err != nil {
		t.Fatalf("Update after a foreign write: %v, want nil (liveness wins by default)", err)
	}
	if doc.Revision != 98 {
		t.Errorf("Revision = %d, want 98", doc.Revision)
	}
}

// TestUpdate_NeverRefusesOnAForeignWrite pins the decision that there is no
// strict mode.
//
// Refusing to write because another process touched the file would stall a
// fencing agent on the strength of a condition it cannot verify, and at
// start-up it would let a predecessor stuck in uninterruptible sleep keep its
// replacement from ever running — the exact liveness hazard the lock-free
// design exists to avoid.
func TestUpdate_NeverRefusesOnAForeignWrite(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	if _, err := s.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Fatal(err)
	}

	foreign := nodestate.NewDocument(nodestate.ComponentSBR)
	foreign.WriterID = "some-other-boot/999/1"
	foreign.Revision = 97
	raw, _ := json.Marshal(foreign)
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), string(raw))

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !res.ForeignWrite {
		t.Error("ForeignWrite = false; the violation must at least be visible")
	}

	doc, err := s.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil })
	if err != nil {
		t.Fatalf("Update refused after a foreign write: %v", err)
	}
	if doc.Revision != 98 {
		t.Errorf("Revision = %d, want 98: the sequence must continue from what is on disk", doc.Revision)
	}
}

func TestContextCancellationIsCheckedBeforeAnyIO(t *testing.T) {
	t.Parallel()

	rec := nodestatetest.NewRecordingFS(nodestatetest.OSFileSystem())
	s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(rec))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Write(ctx, nodestate.NewDocument(nodestate.ComponentSBR)); !errors.Is(err, context.Canceled) {
		t.Errorf("Write error = %v, want context.Canceled", err)
	}
	if _, err := s.Load(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Load error = %v, want context.Canceled", err)
	}
	if _, err := s.Update(ctx, func(*nodestate.Document) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("Update error = %v, want context.Canceled", err)
	}
	if _, err := s.ReadPeer(ctx, nodestate.ComponentSNR); !errors.Is(err, context.Canceled) {
		t.Errorf("ReadPeer error = %v, want context.Canceled", err)
	}
	if len(rec.Operations()) != 0 {
		t.Errorf("file operations ran on a cancelled context: %v", rec.Operations())
	}
}

func TestStatusString(t *testing.T) {
	t.Parallel()

	for _, st := range []nodestate.Status{
		nodestate.StatusOK, nodestate.StatusAbsent, nodestate.StatusRecovered, nodestate.StatusIncompatible,
	} {
		if s := st.String(); s == "" || strings.HasPrefix(s, "Status(") {
			t.Errorf("Status(%d).String() = %q", int(st), s)
		}
	}
	if got := nodestate.Status(42).String(); !strings.Contains(got, "42") {
		t.Errorf("unknown status string = %q", got)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
