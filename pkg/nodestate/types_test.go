package nodestate_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestUnknownFieldsPreserved is the staggered-upgrade guarantee: a document
// written by a newer peer must survive a read-modify-write by an older one with
// every key it did not recognise intact.
func TestUnknownFieldsPreserved(t *testing.T) {
	t.Parallel()

	const raw = `{
		"schemaVersion": 1,
		"node": "worker-0",
		"futureKey": {"nested": [1, 2, 3], "deep": {"a": "b"}},
		"anotherFuture": "scalar",
		"reboots": []
	}`

	var doc nodestate.Document
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if doc.Node != "worker-0" {
		t.Errorf("Node = %q, want %q", doc.Node, "worker-0")
	}
	if len(doc.Unknown) != 2 {
		t.Fatalf("Unknown has %d keys (%v), want 2", len(doc.Unknown), keysOf(doc.Unknown))
	}

	// Modify a known field, then round-trip.
	doc.BootID = "a-new-boot-id"

	out, err := json.Marshal(&doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}

	for _, key := range []string{"futureKey", "anotherFuture"} {
		var orig map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &orig); err != nil {
			t.Fatal(err)
		}
		if !jsonEqual(t, orig[key], got[key]) {
			t.Errorf("key %q: got %s, want %s", key, got[key], orig[key])
		}
	}

	if string(got["bootID"]) != `"a-new-boot-id"` {
		t.Errorf("bootID = %s, want %q", got["bootID"], "a-new-boot-id")
	}

	// "unknown" must never leak into the wire format as its own key.
	if _, ok := got["Unknown"]; ok {
		t.Error("the Unknown map leaked into the encoded document")
	}
}

// TestUnknownFieldsPreserved_Nested covers the same guarantee one level down.
// A newer peer that adds a field to a reboot record must not have it dropped
// when an older peer rewrites the document.
func TestUnknownFieldsPreserved_Nested(t *testing.T) {
	t.Parallel()

	const raw = `{
		"schemaVersion": 1,
		"reboots": [{"ts": "2024-01-01T00:00:00Z", "source": "sbr", "completed": true, "futureReason": {"x": 1}}],
		"agent": {"component": "sbr", "futureAgentKey": "keep-me"}
	}`

	var doc nodestate.Document
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(doc.Reboots) != 1 {
		t.Fatalf("got %d reboots, want 1", len(doc.Reboots))
	}

	out, err := json.Marshal(&doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got struct {
		Reboots []map[string]json.RawMessage `json:"reboots"`
		Agent   map[string]json.RawMessage   `json:"agent"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}

	if !jsonEqual(t, got.Reboots[0]["futureReason"], json.RawMessage(`{"x":1}`)) {
		t.Errorf("reboots[0].futureReason = %s, want {\"x\":1}", got.Reboots[0]["futureReason"])
	}
	if !jsonEqual(t, got.Agent["futureAgentKey"], json.RawMessage(`"keep-me"`)) {
		t.Errorf("agent.futureAgentKey = %s, want \"keep-me\"", got.Agent["futureAgentKey"])
	}
}

// TestUnknownFieldsCannotShadowKnownFields guards the merge order: a stale
// unknown key must never override the typed field of the same name.
func TestUnknownFieldsCannotShadowKnownFields(t *testing.T) {
	t.Parallel()

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.Node = "real-node"
	doc.Unknown = map[string]json.RawMessage{
		"node":   json.RawMessage(`"forged-node"`),
		"future": json.RawMessage(`1`),
	}

	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if string(got["node"]) != `"real-node"` {
		t.Errorf("node = %s, want \"real-node\" (typed field must win)", got["node"])
	}
	if string(got["future"]) != `1` {
		t.Errorf("future = %s, want 1", got["future"])
	}
}

func TestDocument_Clone_IsDeep(t *testing.T) {
	t.Parallel()

	orig := nodestate.NewDocument(nodestate.ComponentSNR)
	orig.Node = "worker-1"
	orig.Config = map[string]string{"k": "v"}
	orig.Unknown = map[string]json.RawMessage{"f": json.RawMessage(`{"a":1}`)}
	orig.AppendIO(nodestate.IOEntry{TS: time.Unix(1, 0).UTC(), OK: true})
	orig.AppendEvent(nodestate.EventEntry{TS: time.Unix(1, 0).UTC(), Reason: "R"})
	orig.SetCondition(nodestate.ConditionEntry{Type: "T", Status: "True"})
	orig.AppendReboot(nodestate.RebootEntry{
		TS:          time.Unix(1, 0).UTC(),
		Source:      nodestate.ComponentSNR,
		Remediation: &nodestate.CRRef{UID: "u", Generation: 1},
	})

	clone := orig.Clone()
	if !reflect.DeepEqual(orig, clone) {
		t.Fatal("clone is not equal to the original")
	}

	clone.Config["k"] = "mutated"
	clone.IO[0].OK = false
	clone.Events[0].Reason = "mutated"
	clone.Conditions[0].Status = "False"
	clone.Reboots[0].Remediation.UID = "mutated"
	clone.Unknown["f"] = json.RawMessage(`2`)
	clone.Node = "mutated"

	if orig.Config["k"] != "v" {
		t.Error("Config aliased between original and clone")
	}
	if !orig.IO[0].OK {
		t.Error("IO aliased between original and clone")
	}
	if orig.Events[0].Reason != "R" {
		t.Error("Events aliased between original and clone")
	}
	if orig.Conditions[0].Status != "True" {
		t.Error("Conditions aliased between original and clone")
	}
	if orig.Reboots[0].Remediation.UID != "u" {
		t.Error("Reboots[].Remediation aliased between original and clone")
	}
	if string(orig.Unknown["f"]) != `{"a":1}` {
		t.Error("Unknown aliased between original and clone")
	}
	if orig.Node != "worker-1" {
		t.Error("scalar field aliased, which is impossible; Clone returned the same pointer")
	}

	// Clone of a nil document must not panic.
	var nilDoc *nodestate.Document
	if nilDoc.Clone() != nil {
		t.Error("(*Document)(nil).Clone() must return nil")
	}
}

func TestDocument_SetCondition_ReplacesByTypeAndKeepsTransitionTime(t *testing.T) {
	t.Parallel()

	first := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)

	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.SetCondition(nodestate.ConditionEntry{Type: "Healthy", Status: "True", LastTransitionTime: first, ObservedAt: first})
	doc.SetCondition(nodestate.ConditionEntry{Type: "Healthy", Status: "True", LastTransitionTime: second, ObservedAt: second})

	if len(doc.Conditions) != 1 {
		t.Fatalf("got %d conditions, want 1", len(doc.Conditions))
	}
	if !doc.Conditions[0].LastTransitionTime.Equal(first) {
		t.Errorf("LastTransitionTime = %v, want %v: an unchanged status must not re-stamp the transition time",
			doc.Conditions[0].LastTransitionTime, first)
	}
	if !doc.Conditions[0].ObservedAt.Equal(second) {
		t.Errorf("ObservedAt = %v, want %v", doc.Conditions[0].ObservedAt, second)
	}

	doc.SetCondition(nodestate.ConditionEntry{Type: "Healthy", Status: "False", LastTransitionTime: second, ObservedAt: second})
	if !doc.Conditions[0].LastTransitionTime.Equal(second) {
		t.Errorf("a status change must re-stamp LastTransitionTime; got %v", doc.Conditions[0].LastTransitionTime)
	}
	if got, ok := doc.Condition("Healthy"); !ok || got.Status != "False" {
		t.Errorf("Condition(Healthy) = %+v, %v", got, ok)
	}
	if _, ok := doc.Condition("Absent"); ok {
		t.Error("Condition reported an absent type as present")
	}
}

// TestDocument_TimeRoundTrip covers times written in a zone other than UTC,
// which is what an agent on a node with a local timezone produces. The
// instant must survive; the offset notation is RFC 3339's business.
func TestDocument_TimeRoundTrip(t *testing.T) {
	t.Parallel()

	loc := time.FixedZone("UTC+5", 5*60*60)
	doc := nodestate.NewDocument(nodestate.ComponentSBR)
	doc.UpdatedAt = time.Date(2024, 3, 1, 10, 0, 0, 0, loc)
	doc.AppendReboot(nodestate.RebootEntry{TS: doc.UpdatedAt.Add(-time.Hour)})

	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte(`"updatedAt":"2024-03-01T10:00:00+05:00"`)) {
		t.Errorf("encoded form is not RFC 3339 with the original offset: %s", out)
	}

	var back nodestate.Document
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if !back.UpdatedAt.Equal(doc.UpdatedAt) {
		t.Errorf("UpdatedAt round-trip: got %v, want %v", back.UpdatedAt, doc.UpdatedAt)
	}
	if !back.Reboots[0].TS.Equal(doc.Reboots[0].TS) {
		t.Errorf("nested time round-trip: got %v, want %v", back.Reboots[0].TS, doc.Reboots[0].TS)
	}

	// A zero time is written in full: encoding/json's omitempty does not
	// apply to a struct, so no field in this schema may rely on it.
	if !bytes.Contains(out, []byte(`"startedAt":"0001-01-01T00:00:00Z"`)) {
		t.Errorf("a zero time was omitted after all; revisit the struct tags: %s", out)
	}
}

func TestComponent_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      nodestate.Component
		wantErr bool
	}{
		{"sbr", nodestate.ComponentSBR, false},
		{"snr", nodestate.ComponentSNR, false},
		{"other operator", nodestate.Component("fence-agent"), false},
		{"digits", nodestate.Component("agent2"), false},
		{"empty", nodestate.Component(""), true},
		{"dot", nodestate.Component("."), true},
		{"parent traversal", nodestate.Component(".."), true},
		{"relative traversal", nodestate.Component("../../etc/passwd"), true},
		{"absolute", nodestate.Component("/etc/passwd"), true},
		{"separator", nodestate.Component("a/b"), true},
		{"backslash", nodestate.Component(`a\b`), true},
		{"nul byte", nodestate.Component("a\x00b"), true},
		{"uppercase", nodestate.Component("SBR"), true},
		{"leading dash", nodestate.Component("-sbr"), true},
		{"trailing dash", nodestate.Component("sbr-"), true},
		{"space", nodestate.Component("s br"), true},
		{"too long", nodestate.Component("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.in.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestComponent_FileName(t *testing.T) {
	t.Parallel()

	if got := nodestate.ComponentSBR.FileName(); got != nodestate.SBRFile {
		t.Errorf("ComponentSBR.FileName() = %q, want %q", got, nodestate.SBRFile)
	}
	if got := nodestate.ComponentSNR.FileName(); got != nodestate.SNRFile {
		t.Errorf("ComponentSNR.FileName() = %q, want %q", got, nodestate.SNRFile)
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func jsonEqual(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var av, bv interface{}
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
