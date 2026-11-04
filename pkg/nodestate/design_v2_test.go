package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

func TestDesignV2ComponentSelection(t *testing.T) {
	for _, self := range []nodestate.Component{nodestate.ComponentSBR, "future-controller"} {
		t.Run(string(self), func(t *testing.T) {
			s, dir := newStore(t, self)
			mustInitialize(t, s)
			ctx := context.Background()
			// SNR is not a participant. Even a damaged leftover must not be read implicitly.
			mustWrite(t, filepath.Join(dir, nodestate.SNRFile), "broken")
			snap := s.Snapshot(ctx)
			if len(snap.Peers) != 1 || snap.Document(self) == nil || !snap.HistoryComplete(time.Now().Add(-time.Hour)) {
				t.Fatalf("own-only snapshot: %+v", snap)
			}
			if docs := s.ReadAll(ctx); len(docs) != 1 || docs[self] == nil {
				t.Fatalf("own-only ReadAll: %v", docs)
			}
			snap = s.Snapshot(ctx, self, nodestate.ComponentSNR)
			if snap.HistoryComplete(time.Now()) || len(snap.Missing()) != 1 {
				t.Fatal("explicit corrupt participant must make history incomplete")
			}
			if err := os.Remove(filepath.Join(dir, nodestate.SNRFile)); err != nil {
				t.Fatal(err)
			}
			if s.Snapshot(ctx, self, nodestate.ComponentSNR).HistoryComplete(time.Now()) {
				t.Fatal("explicit absent participant must remain unknown")
			}
			writePeerDoc(t, dir, nodestate.ComponentSNR, func(d *nodestate.Document) {})
			if len(s.ReadAll(ctx)) != 1 {
				t.Fatal("an unrelated valid file was implicitly included")
			}
			if got := s.Snapshot(ctx, self, nodestate.ComponentSNR); len(got.Peers) != 2 || !got.HistoryComplete(time.Now()) {
				t.Fatal("explicit multi-component support lost")
			}
		})
	}
}

func TestDesignV2OpaqueJournalSurvivesRetentionAndRestart(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithRetention(nodestate.RetentionPolicy{MaxEventEntries: 1, MaxIOEntries: 1}))
	d := nodestate.NewDocument(nodestate.ComponentSBR)
	d.NodeUID = "node-uid"
	d.BootID = "11111111-1111-4111-8111-111111111111"
	d.Config = map[string]string{"uid": "config-uid", "sharedStorageVolumeMode": "BlockPerNode"}
	// These strings follow design §5.6. Common must not interpret SBR's phases.
	d.Agent.Extra = map[string]string{
		"schemaVersion": "1",
		"activeAttempt": `{"attemptID":"request-uid","source":"NHC","requestUID":"request-uid","nodeUID":"node-uid","configUID":"config-uid","initialBootID":"11111111-1111-4111-8111-111111111111","outageEpoch":"epoch-uuid","commitState":"Committed"}`,
		"resetBudget":   `[{"attemptID":"request-uid","source":"NHC","requestUID":"request-uid","nodeUID":"node-uid","initialBootID":"11111111-1111-4111-8111-111111111111","outageEpoch":"epoch-uuid","commitState":"Committed"},{"attemptID":"local-uid","source":"Autonomous","outageEpoch":"epoch-uuid","commitState":"Committed"}]`,
		"outageState":   `{"outageEpoch":"epoch-uuid","resolution":"Unresolved"}`,
	}
	d.Agent.ResetCommitted = true
	want := d.Clone()
	if err := s.Write(ctx, d); err != nil {
		t.Fatal(err)
	}
	// Diagnostic pruning and new-boot bookkeeping must preserve all required strings.
	if _, err := s.Update(ctx, func(doc *nodestate.Document) error {
		doc.BootID = "22222222-2222-4222-8222-222222222222"
		doc.Agent.ResetCommitted = false // consumer has validated a different local boot
		for i := 0; i < 5; i++ {
			doc.AppendEvent(nodestate.EventEntry{TS: time.Now()})
			doc.AppendIO(nodestate.IOEntry{TS: time.Now()})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	restarted, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Load(ctx)
	if err != nil || got.HistoryUncertain() {
		t.Fatalf("reload: %v %v", got, err)
	}
	if !reflect.DeepEqual(got.Document.Agent.Extra, want.Agent.Extra) || len(got.Document.Events) != 1 || len(got.Document.IO) != 1 || got.Document.Agent.ResetCommitted {
		t.Fatalf("lost state or failed diagnostic trim: %+v", got.Document)
	}
	clone := got.Document.Clone()
	clone.Agent.Extra["resetBudget"] = "[]"
	if !reflect.DeepEqual(got.Document.Agent.Extra, want.Agent.Extra) {
		t.Fatal("clone aliases required state")
	}
	// Reporter APIs must not fsync, rewrite, or reclassify consumer fields.
	before, err := os.ReadFile(filepath.Join(dir, nodestate.SBRFile))
	if err != nil {
		t.Fatal(err)
	}
	peer, err := restarted.LoadPeer(ctx, nodestate.ComponentSBR)
	if err != nil || peer.Status != nodestate.PeerOK || !reflect.DeepEqual(peer.Document.Agent.Extra, want.Agent.Extra) {
		t.Fatalf("reporter read: %v %v", peer, err)
	}
	after, err := os.ReadFile(filepath.Join(dir, nodestate.SBRFile))
	if err != nil || string(before) != string(after) {
		t.Fatal("reporter changed journal")
	}
	var wire struct {
		Agent struct {
			Extra map[string]string `json:"extra"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(after, &wire); err != nil || !reflect.DeepEqual(wire.Agent.Extra, want.Agent.Extra) {
		t.Fatal("SBR state was not encoded as JSON strings")
	}
	// Required state over the hard size limit must fail, not be truncated.
	limited, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir), nodestate.WithMaxDocumentBytes(len(after)+128))
	if err != nil {
		t.Fatal(err)
	}
	_, err = limited.Update(ctx, func(doc *nodestate.Document) error {
		doc.Agent.Extra["resetBudget"] = strings.Repeat("x", len(after)+256)
		return nil
	})
	if !errors.Is(err, nodestate.ErrTooLarge) {
		t.Fatalf("oversized ledger: %v", err)
	}
	after, err = os.ReadFile(filepath.Join(dir, nodestate.SBRFile))
	if err != nil || string(before) != string(after) {
		t.Fatal("failed update destroyed retained ledger")
	}
}

func TestDesignV2CorruptIsNotRecovered(t *testing.T) {
	s, dir := newStore(t, nodestate.ComponentSBR)
	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), "broken")
	for i := 0; i < 2; i++ {
		got, err := s.Load(context.Background())
		if err != nil || got.Status != nodestate.StatusCorrupt || got.Status.String() != "Corrupt" || !got.HistoryUncertain() {
			t.Fatalf("corrupt history classification: %v %v", got, err)
		}
	}
}
