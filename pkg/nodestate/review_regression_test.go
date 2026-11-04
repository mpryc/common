package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

func TestReviewHistorySurvivesRestartAndFailedReconciliation(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"{bad", `{"schemaVersion":99,"agent":{"extra":{"ownedRequest":"retained"}}}`} {
		faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
		s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
		path := s.Path(s.Self())
		mustWrite(t, path, raw)
		faulty.FailOnce(nodestatetest.OpSyncFile, syscall.EIO)
		_, err := s.ReconcileHistory(context.Background(), "test reconciliation", func(r *nodestate.LoadResult) (*nodestate.Document, error) {
			if !r.HistoryUncertain() {
				t.Fatal("lost uncertainty")
			}
			return nodestate.NewDocument(s.Self()), nil
		})
		if err == nil {
			t.Fatal("expected failed replacement")
		}
		after, _ := os.ReadFile(path)
		if string(after) != raw {
			t.Fatal("failed reconciliation lost evidence")
		}
		restarted, _ := nodestate.New(s.Self(), nodestate.WithDir(dir))
		result, err := restarted.Load(context.Background())
		if err != nil || !result.HistoryUncertain() {
			t.Fatalf("restart lost uncertainty: %+v %v", result, err)
		}
		_, err = restarted.Update(context.Background(), func(d *nodestate.Document) error { t.Fatal("mutator ran"); return nil })
		if !errors.Is(err, nodestate.ErrHistoryUnknown) {
			t.Fatal(err)
		}
	}
}

func TestReviewMissingHistoryNeverAutoInitializes(t *testing.T) {
	t.Parallel()
	s, dir := newStore(t, nodestate.ComponentSBR)
	mustInitialize(t, s)
	if err := os.Remove(s.Path(s.Self())); err != nil {
		t.Fatal(err)
	}
	restarted, _ := nodestate.New(s.Self(), nodestate.WithDir(dir))
	r, err := restarted.Load(context.Background())
	if err != nil || !r.HistoryUncertain() {
		t.Fatalf("absence trusted: %+v %v", r, err)
	}
	_, err = restarted.Update(context.Background(), func(d *nodestate.Document) error { t.Fatal("initialized automatically"); return nil })
	if !errors.Is(err, nodestate.ErrHistoryUnknown) {
		t.Fatal(err)
	}
}

func TestReviewOperationalReadFailuresNeverQuarantine(t *testing.T) {
	t.Parallel()
	for _, op := range []nodestatetest.Op{nodestatetest.OpOpen, nodestatetest.OpRead, nodestatetest.OpLstat} {
		t.Run(string(op), func(t *testing.T) {
			rec := nodestatetest.NewRecordingFS(nodestate.OSFileSystem())
			faulty := nodestatetest.NewFaultyFS(rec)
			s, _ := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
			d := nodestate.NewDocument(s.Self())
			d.Agent.Extra = map[string]string{"ownedRequest": "owner", "resetBudget": "spent"}
			if err := s.Write(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(s.Path(s.Self()))
			rec.Reset()
			faulty.FailOnce(op, syscall.EIO)
			_, err := s.Update(context.Background(), func(d *nodestate.Document) error { t.Fatal("read failure invoked mutation"); return nil })
			if !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			if rec.Count(nodestatetest.OpRename) != 0 || rec.Count(nodestatetest.OpRemove) != 0 {
				t.Fatal("read failure changed directory")
			}
			after, _ := os.ReadFile(s.Path(s.Self()))
			if string(after) != string(before) {
				t.Fatal("history changed")
			}
			r, err := s.Load(context.Background())
			if err != nil || r.Document.Agent.Extra["resetBudget"] != "spent" {
				t.Fatalf("recovery: %+v %v", r, err)
			}
		})
	}
}

func TestReviewConfirmDurabilityDoesNotRepeatMutation(t *testing.T) {
	t.Parallel()
	faulty := nodestatetest.NewFaultyFS(nodestate.OSFileSystem())
	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithFileSystem(faulty))
	mustInitialize(t, s)
	calls := 0
	candidate, err := s.Update(context.Background(), func(d *nodestate.Document) error {
		calls++
		d.Agent.ResetCommitted = true
		d.Agent.Extra = map[string]string{"resetBudget": "one"}
		faulty.FailAlways(nodestatetest.OpSyncDir, syscall.EIO)
		return nil
	})
	if !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) || candidate == nil {
		t.Fatalf("%+v %v", candidate, err)
	}
	if err := s.ConfirmDurability(context.Background(), candidate); !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatal(err)
	}
	// A new Store/process cannot treat read visibility as durability confirmation.
	restarted, _ := nodestate.New(s.Self(), nodestate.WithDir(dir), nodestate.WithFileSystem(faulty))
	loaded, err := restarted.Load(context.Background())
	if !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) || loaded.Status != nodestate.StatusDurabilityUnknown {
		t.Fatalf("%+v %v", loaded, err)
	}
	_, err = restarted.Update(context.Background(), func(d *nodestate.Document) error { t.Fatal("transition passed unresolved predecessor"); return nil })
	if !errors.Is(err, nodestate.ErrDurabilityUnconfirmed) {
		t.Fatal(err)
	}
	faulty.Clear()
	if err := restarted.ConfirmDurability(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	r, err := restarted.Load(context.Background())
	if err != nil || r.Document.Revision != candidate.Revision || calls != 1 || !r.Document.Agent.ResetCommitted {
		t.Fatalf("confirmation changed candidate: %+v %v", r, err)
	}
	_, err = restarted.Update(context.Background(), func(d *nodestate.Document) error { d.Node = "new"; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ConfirmDurability(context.Background(), candidate); !errors.Is(err, nodestate.ErrStateChanged) {
		t.Fatal(err)
	}
}

func TestReviewRequestIdentityAndLegacyRecords(t *testing.T) {
	t.Parallel()
	a := nodestate.CRRef{UID: "request", NodeUID: "node", Generation: 1}
	b := a
	b.Generation = 9
	d := nodestate.NewDocument(nodestate.ComponentSBR)
	at := time.Now()
	d.AppendReboot(nodestate.RebootEntry{TS: at, Remediation: &a})
	d.AppendReboot(nodestate.RebootEntry{TS: at.Add(time.Second), Remediation: &b, Completed: true})
	merged := nodestate.MergeReboots(d)
	if len(merged) != 1 || merged[0].Remediation.Generation != 1 {
		t.Fatalf("attempt identity changed: %+v", merged)
	}
	if _, ok := nodestate.FindReboot(b, d); !ok {
		t.Fatal("generation authorized a new identity")
	}
	b.NodeUID = "replacement"
	if _, ok := nodestate.FindReboot(b, d); ok {
		t.Fatal("replacement Node matched")
	}
	legacy := nodestate.CRRef{UID: "request", Generation: 1}
	if legacy.Validate() == nil {
		t.Fatal("legacy reference accepted")
	}
	d.AppendReboot(nodestate.RebootEntry{TS: at, Remediation: &legacy})
	if _, ok := nodestate.FindReboot(legacy, d); ok {
		t.Fatal("legacy identity matched")
	}
	if d.Reboots[2].Remediation == nil {
		t.Fatal("legacy requested entry became autonomous")
	}
}

func TestReviewReadPathsEnforceDirectorySecurity(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"ancestor-symlink", "directory-symlink", "permissions"} {
		t.Run(kind, func(t *testing.T) {
			root := tempStateDir(t)
			real := filepath.Join(root, "real")
			if err := os.Mkdir(real, 0700); err != nil {
				t.Fatal(err)
			}
			state := filepath.Join(real, "state")
			if err := os.Mkdir(state, 0700); err != nil {
				t.Fatal(err)
			}
			writer, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(state))
			mustInitialize(t, writer)
			dir := state
			switch kind {
			case "ancestor-symlink":
				link := filepath.Join(root, "link")
				if err := os.Symlink(real, link); err != nil {
					t.Fatal(err)
				}
				dir = filepath.Join(link, "state")
			case "directory-symlink":
				dir = filepath.Join(root, "link")
				if err := os.Symlink(state, dir); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(state, 0777); err != nil {
					t.Fatal(err)
				}
			}
			reader, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
			if _, err := reader.Load(context.Background()); err == nil {
				t.Fatal("Load trusted insecure path")
			}
			peer, err := reader.LoadPeer(context.Background(), nodestate.ComponentSBR)
			if err != nil || peer.Status != nodestate.PeerUnreadable || peer.Document != nil {
				t.Fatalf("peer: %+v %v", peer, err)
			}
			if len(reader.ReadAll(context.Background(), nodestate.ComponentSBR)) != 0 {
				t.Fatal("ReadAll trusted insecure path")
			}
			snap := reader.Snapshot(context.Background(), nodestate.ComponentSBR)
			if snap.HistoryComplete(time.Now().Add(-time.Hour)) || len(snap.Documents()) != 0 {
				t.Fatal("Snapshot trusted insecure path")
			}
		})
	}
}

func TestReviewLargeExtensionsAndRequiredRecordsSurvive(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, nodestate.ComponentSBR)
	d := nodestate.NewDocument(s.Self())
	value, _ := json.Marshal(strings.Repeat("x", 70<<10))
	d.Unknown = map[string]json.RawMessage{"future": value}
	d.Agent.Extra = map[string]string{"schemaVersion": "1", "activeAttempt": "retained", "outageState": "unresolved", "resetBudget": "spent"}
	if err := s.Write(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	out, err := s.Update(context.Background(), func(d *nodestate.Document) error {
		for i := 0; i < 150; i++ {
			d.AppendEvent(nodestate.EventEntry{TS: time.Now(), Message: "diagnostic"})
		}
		return nil
	})
	if err != nil || string(out.Unknown["future"]) != string(value) || out.Agent.Extra["resetBudget"] != "spent" || len(out.Agent.Extra) != 4 {
		t.Fatalf("lost required records: %v", err)
	}
}

type cancelOnOpenFS struct {
	nodestate.FS
	cancel context.CancelFunc
}

func (f cancelOnOpenFS) Open(path string, opts nodestate.OpenOptions) (nodestate.File, error) {
	file, err := f.FS.Open(path, opts)
	f.cancel()
	return file, err
}

func TestReviewSnapshotCancellationBetweenComponents(t *testing.T) {
	t.Parallel()
	s, dir := newStore(t, nodestate.ComponentSBR)
	mustInitialize(t, s)
	peer, _ := nodestate.New(nodestate.ComponentSNR, nodestate.WithDir(dir))
	mustInitialize(t, peer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, _ := nodestate.New(s.Self(), nodestate.WithDir(dir), nodestate.WithFileSystem(cancelOnOpenFS{nodestate.OSFileSystem(), cancel}))
	snap := reader.Snapshot(ctx, nodestate.ComponentSBR, nodestate.ComponentSNR)
	if snap.Err == nil || snap.HistoryComplete(time.Now()) || len(snap.Missing()) != 1 || snap.Missing()[0] != nodestate.ComponentSNR {
		t.Fatalf("partial snapshot claimed complete: %+v", snap)
	}
	invalid := s.Snapshot(context.Background(), nodestate.ComponentSBR, nodestate.Component("../invalid"))
	if invalid.HistoryComplete(time.Now()) || invalid.Err == nil {
		t.Fatal("invalid component silently omitted")
	}
}

func TestReviewRetentionTiesAndClockRollback(t *testing.T) {
	t.Parallel()
	at := time.Now().UTC()
	d := nodestate.NewDocument(nodestate.ComponentSBR)
	for i := 0; i < 3; i++ {
		d.AppendReboot(nodestate.RebootEntry{TS: at})
	}
	policy := nodestate.RetentionPolicy{MaxRebootEntries: 2}
	policy.Apply(d, at.Add(time.Hour))
	if nodestate.RebootHistoryComplete(at, d) {
		t.Fatal("discarded timestamp included in complete interval")
	}
	if !nodestate.RebootHistoryComplete(at.Add(time.Nanosecond), d) {
		t.Fatal("strictly later window should be complete")
	}
	policy.Apply(d, at.Add(-time.Hour))
	if nodestate.RebootHistoryComplete(at, d) {
		t.Fatal("clock rollback restored discarded history")
	}
	d = nodestate.NewDocument(nodestate.ComponentSBR)
	d.AppendReboot(nodestate.RebootEntry{})
	d.AppendReboot(nodestate.RebootEntry{TS: at})
	nodestate.RetentionPolicy{MaxRebootEntries: 1}.Apply(d, at)
	if !d.RebootHistoryUnknown || nodestate.RebootHistoryComplete(at.Add(time.Hour), d) {
		t.Fatal("discarded unknown timestamp claimed complete")
	}
}
