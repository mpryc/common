package nodestate_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
	"github.com/medik8s/common/pkg/nodestate/nodestatetest"
)

// This file pins the on-disk contract against the design document.
//
// Every other test here asserts behaviour. These assert *names*, because the
// journal is an interface between two repositories that are released
// separately. A field renamed in Go is a compile error for this package and a
// silent data loss for the consumer: the old key stops being written, the new
// one is not read by the deployed agent, and the first symptom is a reset
// budget that reads empty on a node that has already been reset.
//
// If one of these fails, the question is not "is the new name better" but
// "has the design and the consumer changed with it".

// envelopeKeys are the top-level members design §5.6 specifies for the
// journal envelope.
var envelopeKeys = []string{
	"schemaVersion",
	"node",
	"nodeUID",
	"bootID",
	"updatedAt",
	"agent",
	"reboots",
	"rebootHistoryCompleteSince",
	"historyReconciledAt",
}

// agentKeys are the members of the agent object the design names. Only
// resetCommitted, shutdown and extra are required by SBR; component and the
// process identity are common's own.
var agentKeys = []string{
	"component",
	"startedAt",
	"resetCommitted",
	"extra",
}

func TestContract_EnvelopeFieldNames(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Node = "worker-0"
	doc.NodeUID = "node-uid"
	doc.BootID = "11111111-1111-4111-8111-111111111111"
	doc.Config = map[string]string{"uid": "config-uid", "sharedStorageVolumeMode": "BlockPerNode"}
	doc.Agent.Shutdown = nodestate.ShutdownRunning
	doc.Agent.ResetCommitted = true
	doc.Agent.Extra = map[string]string{"schemaVersion": "1"}

	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}

	for _, key := range envelopeKeys {
		if _, ok := envelope[key]; !ok {
			t.Errorf("envelope is missing %q, which design §5.6 names; encoded: %s", key, encoded)
		}
	}

	var agent map[string]json.RawMessage
	if err := json.Unmarshal(envelope["agent"], &agent); err != nil {
		t.Fatal(err)
	}
	for _, key := range agentKeys {
		if _, ok := agent[key]; !ok {
			t.Errorf("agent object is missing %q; encoded: %s", key, envelope["agent"])
		}
	}

	// The vocabulary this mode exists to escape must not come back.
	if strings.Contains(strings.ToLower(string(encoded)), "fence") {
		t.Errorf("the encoded document mentions fencing; BlockPerNode performs resets: %s", encoded)
	}
}

// TestContract_ResetCommittedIsNotFenceLatched guards the rename itself. The
// consumer reads this key by name; a silent revert would mean an agent that
// cannot tell it has already committed to a reset, and commits again.
func TestContract_ResetCommittedIsNotFenceLatched(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Agent.ResetCommitted = true

	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"resetCommitted":true`) {
		t.Errorf("resetCommitted not in the encoded form: %s", encoded)
	}
	if strings.Contains(string(encoded), "fenceLatched") {
		t.Errorf("the superseded fenceLatched key is still emitted: %s", encoded)
	}

	var back nodestate.Document
	if err := json.Unmarshal(encoded, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Agent.ResetCommitted {
		t.Error("resetCommitted did not survive a round trip")
	}
}

// TestContract_RequestIdentityIsUIDAndNodeUID pins the identity rule from
// design §5.2 and §5.6: a changed generation is not another attempt, and a
// different Node is never the same attempt.
func TestContract_RequestIdentityIsUIDAndNodeUID(t *testing.T) {
	t.Parallel()

	base := nodestate.CRRef{Kind: "StorageBasedRemediation", Name: "worker-0", UID: "request-uid", NodeUID: "node-uid"}

	laterGeneration := base
	laterGeneration.Generation = 7
	if base.Key() != laterGeneration.Key() {
		t.Errorf("generation changed the identity: %q vs %q; design §5.2 says a changed "+
			"generation cannot give that CR another attempt", base.Key(), laterGeneration.Key())
	}

	replacementNode := base
	replacementNode.NodeUID = "other-node-uid"
	if base.Key() == replacementNode.Key() {
		t.Error("a different Node UID produced the same identity; a replacement node would " +
			"inherit its predecessor's execution history")
	}

	// Neither half alone is an identity.
	for _, incomplete := range []nodestate.CRRef{
		{UID: "request-uid"},
		{NodeUID: "node-uid"},
		{Kind: "StorageBasedRemediation", Name: "worker-0"},
	} {
		if err := incomplete.Validate(); err == nil {
			t.Errorf("%+v validated as an identity", incomplete)
		}
		if incomplete.Key() != "" {
			t.Errorf("%+v produced key %q; an unusable reference must not match anything",
				incomplete, incomplete.Key())
		}
	}

	// The encoding must not collide when a UID contains the delimiter.
	odd := nodestate.CRRef{UID: `a","b`, NodeUID: "n"}
	other := nodestate.CRRef{UID: "a", NodeUID: `b","n`}
	if odd.Key() == other.Key() {
		t.Errorf("delimiter collision: %q == %q", odd.Key(), other.Key())
	}
}

// TestContract_ExtraKeysAreOpaqueAndPreserved covers the four keys design
// §5.6 reserves for the consumer. Common must carry them unchanged — it has
// no business parsing a commitState or charging a budget — and must not prune
// them as diagnostics.
func TestContract_ExtraKeysAreOpaqueAndPreserved(t *testing.T) {
	t.Parallel()

	extra := map[string]string{
		"schemaVersion": "1",
		"activeAttempt": `{"attemptID":"request-uid","requestUID":"request-uid","nodeUID":"node-uid",` +
			`"configUID":"config-uid","initialBootID":"boot-a","outageEpoch":"epoch-uuid","commitState":"Committed"}`,
		"resetBudget": `[{"attemptID":"request-uid","outageEpoch":"epoch-uuid","commitState":"Committed"},` +
			`{"attemptID":"local-uid","source":"Autonomous","outageEpoch":"epoch-uuid","commitState":"Committed"}]`,
		"outageState": `{"outageEpoch":"epoch-uuid","resolution":"Unresolved"}`,
	}

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	initialize(t, s)

	doc, err := s.Update(ctx, func(d *nodestate.Document) error {
		d.Node = "worker-0"
		d.NodeUID = "node-uid"
		d.Agent.Extra = extra
		d.Agent.ResetCommitted = true
		// Enough diagnostics to force retention to run and prune.
		for i := 0; i < nodestate.DefaultMaxIOEntries*2; i++ {
			d.AppendIO(nodestate.IOEntry{TS: time.Now(), Op: "write", OK: true})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.IO) != nodestate.DefaultMaxIOEntries {
		t.Fatalf("retention did not run: %d I/O entries", len(doc.IO))
	}

	back, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Agent.Extra, extra) {
		t.Errorf("required consumer state was altered by diagnostic pruning\n got: %v\nwant: %v",
			back.Agent.Extra, extra)
	}
	if !back.Agent.ResetCommitted {
		t.Error("resetCommitted was lost")
	}

	// The keys reach the file under exactly these names.
	raw, err := json.Marshal(back)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schemaVersion", "activeAttempt", "resetBudget", "outageState"} {
		if !strings.Contains(string(raw), `"`+key+`"`) {
			t.Errorf("consumer key %q is not in the encoded document", key)
		}
	}
	for _, superseded := range []string{"sbrStateVersion", "resetLedger", "epochID", "beforeBootID"} {
		if strings.Contains(string(raw), `"`+superseded+`"`) {
			t.Errorf("superseded name %q is still written", superseded)
		}
	}
}

// TestContract_StatusVocabulary pins the classification names a consumer
// branches on at start-up, per the recovery table in design §5.6.
func TestContract_StatusVocabulary(t *testing.T) {
	t.Parallel()

	want := map[nodestate.Status]string{
		nodestate.StatusOK:                "OK",
		nodestate.StatusAbsent:            "Absent",
		nodestate.StatusCorrupt:           "Corrupt",
		nodestate.StatusIncompatible:      "Incompatible",
		nodestate.StatusDurabilityUnknown: "DurabilityUnknown",
		nodestate.StatusRecovered:         "Recovered",
	}
	for status, name := range want {
		if got := status.String(); got != name {
			t.Errorf("Status(%d).String() = %q, want %q", int(status), got, name)
		}
	}

	// Only observed and reconstructed history may be acted on.
	trusted := map[nodestate.Status]bool{
		nodestate.StatusOK:        true,
		nodestate.StatusRecovered: true,
	}
	for status := range want {
		if got := status.Trusted(); got != trusted[status] {
			t.Errorf("Status %v: Trusted() = %v, want %v", status, got, trusted[status])
		}
		res := &nodestate.LoadResult{Status: status}
		if got := res.HistoryUncertain(); got == trusted[status] {
			t.Errorf("Status %v: HistoryUncertain() = %v", status, got)
		}
	}
}

// TestContract_PeerVocabulary pins the peer statuses, which a consumer turns
// into a Node condition.
func TestContract_PeerVocabulary(t *testing.T) {
	t.Parallel()

	want := map[nodestate.PeerStatus]string{
		nodestate.PeerOK:                "OK",
		nodestate.PeerAbsent:            "Absent",
		nodestate.PeerUnusable:          "Unusable",
		nodestate.PeerUnreadable:        "Unreadable",
		nodestate.PeerUnsupportedSchema: "UnsupportedSchema",
	}
	for status, name := range want {
		if got := status.String(); got != name {
			t.Errorf("PeerStatus(%d).String() = %q, want %q", int(status), got, name)
		}
	}
}

// TestContract_SchemaAndPathConstants pins the values a deployment is
// configured against: the mount path in the DaemonSet and the file name.
func TestContract_SchemaAndPathConstants(t *testing.T) {
	t.Parallel()

	if nodestate.Dir != "/var/lib/medik8s/state" {
		t.Errorf("Dir = %q; the design mounts /var/lib/medik8s/state into the agent", nodestate.Dir)
	}
	if nodestate.SBRFile != "sbr.json" {
		t.Errorf("SBRFile = %q, want sbr.json", nodestate.SBRFile)
	}
	if nodestate.Schema != 1 {
		t.Errorf("Schema = %d; design §5.6 pins the envelope at 1", nodestate.Schema)
	}
}

// TestContract_OwnerScopedDefault pins the V2-001 decision: a consumer reads
// its own component unless it names others.
//
// It matters because the alternative was a silent one. With a built-in peer
// set, a stale snr.json left by an unrelated deployment would make SBR's
// history permanently incomplete, and a consumer failing closed on that would
// stop resetting nodes entirely.
func TestContract_OwnerScopedDefault(t *testing.T) {
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	initialize(t, s)
	// A corrupt file belonging to a component this consumer does not use.
	mustWrite(t, dir+"/"+nodestate.SNRFile, "{ not a document")

	snap := s.Snapshot(ctx)
	if got := len(snap.Peers); got != 1 {
		t.Fatalf("default snapshot read %d components, want only the owner", got)
	}
	if !snap.HistoryComplete(time.Now().Add(-time.Hour)) {
		t.Errorf("an unrelated component's corrupt file made the owner's history incomplete: %v", snap.Missing())
	}

	// Naming it explicitly still reports it, so deliberate multi-component
	// reads keep working for a future controller.
	explicit := s.Snapshot(ctx, nodestate.ComponentSBR, nodestate.ComponentSNR)
	if explicit.HistoryComplete(time.Now().Add(-time.Hour)) {
		t.Error("an explicitly requested corrupt component did not make the history incomplete")
	}
	missing := explicit.Missing()
	if len(missing) != 1 || missing[0] != nodestate.ComponentSNR {
		t.Errorf("Missing() = %v", missing)
	}
}

// TestContract_DocumentFieldsAreStable is a tripwire on the Go struct rather
// than the JSON, so that adding a field is a deliberate act reviewed against
// the design rather than something that happens in passing.
func TestContract_DocumentFieldsAreStable(t *testing.T) {
	t.Parallel()

	want := []string{
		"Agent", "BootID", "Conditions", "Config", "Events", "HistoryReconciledAt", "IO",
		"Node", "NodeUID", "RebootHistoryCompleteSince", "RebootHistoryUnknown", "Reboots",
		"Revision", "SchemaVersion", "Unknown", "UpdatedAt", "WriterID",
	}
	assertFieldNames(t, reflect.TypeOf(nodestate.Document{}), want, "Document")

	wantAgent := []string{
		"Component", "Extra", "PID", "ResetCommitted", "Shutdown", "StartedAt", "Unknown", "Version",
	}
	assertFieldNames(t, reflect.TypeOf(nodestate.AgentState{}), wantAgent, "AgentState")

	wantRef := []string{"Generation", "Kind", "Name", "Namespace", "NodeUID", "UID", "Unknown"}
	assertFieldNames(t, reflect.TypeOf(nodestate.CRRef{}), wantRef, "CRRef")
}

func assertFieldNames(t *testing.T, typ reflect.Type, want []string, label string) {
	t.Helper()

	got := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		got = append(got, typ.Field(i).Name)
	}
	sort.Strings(got)
	sorted := append([]string(nil), want...)
	sort.Strings(sorted)

	if !reflect.DeepEqual(got, sorted) {
		t.Errorf("%s fields changed.\n got: %v\nwant: %v\n\n"+
			"Adding or renaming a field here changes an interface between two separately "+
			"released repositories. Update the design and the consumer in the same change, "+
			"then update this list.", label, got, sorted)
	}
}

// initialize performs the deliberate first write design §5.6 requires before
// ordinary updates. On a node with no journal, Update fails closed rather
// than inventing an empty history, so a consumer must initialize explicitly
// after it has checked its live identities and budget records.
func initialize(t *testing.T, s *nodestate.Store) {
	t.Helper()
	if err := s.Write(context.Background(), nodestate.NewDocument(s.Self())); err != nil {
		t.Fatalf("initializing a fresh journal: %v", err)
	}
}

// TestContract_FreshNodeRequiresDeliberateInitialization pins that boundary.
//
// It is the answer to "fail in the controller, or carry on with a fresh
// file": carry on is not available by accident. Absence is not proof of first
// installation — it is equally what a wiped host path looks like — so the
// consumer has to say it means to start from nothing.
func TestContract_FreshNodeRequiresDeliberateInitialization(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != nodestate.StatusAbsent || !res.HistoryUncertain() {
		t.Fatalf("fresh node: status=%v uncertain=%v", res.Status, res.HistoryUncertain())
	}

	if _, err := s.Update(ctx, func(d *nodestate.Document) error { return nil }); !errorsIsHistoryUnknown(err) {
		t.Errorf("Update on an uninitialized journal = %v, want ErrHistoryUnknown", err)
	}
	if _, err := s.Read(ctx); !errorsIsHistoryUnknown(err) {
		t.Errorf("Read on an uninitialized journal = %v, want ErrHistoryUnknown", err)
	}

	// Write is the deliberate act, and it works.
	initialize(t, s)
	if _, err := s.Update(ctx, func(d *nodestate.Document) error { d.Node = "worker-0"; return nil }); err != nil {
		t.Errorf("Update after initialization: %v", err)
	}
}

func errorsIsHistoryUnknown(err error) bool {
	return err != nil && errors.Is(err, nodestate.ErrHistoryUnknown)
}

// TestContract_RecoveredHistoryIsTrustedAndSelfClearing covers the state
// added for reconstructed history.
//
// It has to be usable, or reconciliation would leave the agent no better off
// than before it. It has to be distinguishable, or a reset budget assembled
// by an operator from API facts would be indistinguishable from one the agent
// observed. And it has to stop being either, once the records it describes
// have aged out — a permanent mark would brand a node reconstructed for the
// rest of its life.
func TestContract_RecoveredHistoryIsTrustedAndSelfClearing(t *testing.T) {
	t.Parallel()

	start := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := nodestatetest.NewFakeClock(start)
	s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithClock(clk))
	ctx := context.Background()

	mustWrite(t, filepath.Join(dir, nodestate.SBRFile), "{ truncated")
	reconciled, err := s.ReconcileHistory(ctx, "operator confirmed identities against the API",
		func(r *nodestate.LoadResult) (*nodestate.Document, error) {
			if r.Status != nodestate.StatusCorrupt {
				t.Errorf("reconciling from status %v", r.Status)
			}
			d := nodestate.NewDocument(nodestate.ComponentSBR)
			d.Node = "worker-0"
			return d, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.HistoryReconciledAt.IsZero() {
		t.Error("the reconstruction was not recorded in the envelope")
	}

	res, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != nodestate.StatusRecovered || !res.HistoryReconstructed() {
		t.Fatalf("status = %v", res.Status)
	}
	if res.HistoryUncertain() {
		t.Error("reconstructed history is not usable")
	}

	// It survives a restart, because it is in the envelope and not the ring.
	successor, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir), nodestate.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	if after, err := successor.Load(ctx); err != nil || after.Status != nodestate.StatusRecovered {
		t.Fatalf("after restart: status=%v err=%v", after.Status, err)
	}

	// Once the reconstruction is older than the reboot retention window,
	// every record it supplied has aged out and the history is ordinary.
	clk.Set(start.Add(nodestate.DefaultRebootRetention + time.Hour))
	later, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if later.Status != nodestate.StatusOK {
		t.Errorf("status = %v after the retention window; the mark did not clear", later.Status)
	}
}
