package nodestate_test

import (
	"encoding/json"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
)

// FuzzDocumentCodec fuzzes the document parser.
//
// It is worth fuzzing because the input is attacker-influenceable: any
// workload privileged enough to write the node's state directory chooses
// these bytes, and both operators parse whatever they find there.
//
// Three properties, in increasing order of what they would cost to get
// wrong:
//
//  1. Decoding never panics. A panic in an agent that feeds a watchdog is a
//     node reset.
//
//  2. Anything that decodes must re-encode. This is the one that matters.
//     The unknown-member machinery carries forward bytes the build does not
//     understand, so a document that reads cleanly but fails to marshal
//     would leave an agent able to load its state and unable to write it —
//     wedged out of its own file for as long as that file is on disk, with
//     no way to recover short of deleting it by hand.
//
//  3. Re-decoding the re-encoded form agrees on the typed fields. A codec
//     that is not idempotent would mutate the document a little on every
//     write, which over a reboot history is data corruption that nothing
//     would notice until a budget came out wrong.
func FuzzDocumentCodec(f *testing.F) {
	seeds := []string{
		`{"schemaVersion":1}`,
		`{"schemaVersion":1,"node":"worker-0","reboots":[]}`,
		`{"schemaVersion":1,"agent":{"component":"sbr","pid":7}}`,
		// Unknown members at every level the preservation machinery covers.
		`{"schemaVersion":1,"futureKey":{"nested":[1,2,3]},"agent":{"futureAgentKey":"x"}}`,
		`{"schemaVersion":1,"reboots":[{"ts":"2024-01-01T00:00:00Z","completed":true,"futureReason":{"x":1}}]}`,
		`{"schemaVersion":1,"reboots":[{"remediation":{"uid":"u","generation":2,"futureRef":[]}}]}`,
		// Shapes the parser has to reject rather than mangle.
		`{"schemaVersion":"one"}`,
		`{"schemaVersion":1,"reboots":{}}`,
		`[]`,
		`null`,
		``,
		`{`,
		"\x00\x01\x02",
		// Case-insensitive member matching, which decides what counts as
		// unknown.
		`{"SCHEMAVERSION":1,"NODE":"worker-0"}`,
		// Duplicate members: last wins for the struct, and the map must not
		// disagree.
		`{"schemaVersion":1,"node":"a","node":"b"}`,
		// Deep nesting, to make sure nothing recurses without a bound.
		`{"schemaVersion":1,"deep":` + nest(64) + `}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var doc nodestate.Document
		if err := json.Unmarshal(data, &doc); err != nil {
			return // rejecting input is the correct outcome, not a finding
		}

		encoded, err := json.Marshal(&doc)
		if err != nil {
			t.Fatalf("decoded but will not re-encode, which wedges an agent out of its own file: %v\ninput: %q",
				err, data)
		}

		var again nodestate.Document
		if err := json.Unmarshal(encoded, &again); err != nil {
			t.Fatalf("re-encoded form does not decode: %v\ninput: %q\nencoded: %q", err, data, encoded)
		}

		// The typed fields must survive a round trip unchanged. Unknown
		// members are compared by count rather than by bytes, because
		// encoding/json compacts them and a member whose name collided with
		// a typed field is deliberately dropped.
		if again.SchemaVersion != doc.SchemaVersion ||
			again.Node != doc.Node ||
			again.NodeUID != doc.NodeUID ||
			again.BootID != doc.BootID ||
			again.Revision != doc.Revision ||
			again.WriterID != doc.WriterID {
			t.Fatalf("identity fields changed across a round trip\nfirst:  %+v\nsecond: %+v\ninput: %q",
				doc, again, data)
		}
		if !again.UpdatedAt.Equal(doc.UpdatedAt) {
			t.Fatalf("UpdatedAt changed across a round trip: %v then %v", doc.UpdatedAt, again.UpdatedAt)
		}
		if len(again.Reboots) != len(doc.Reboots) {
			t.Fatalf("reboot count changed across a round trip: %d then %d\ninput: %q",
				len(doc.Reboots), len(again.Reboots), data)
		}
		for i := range doc.Reboots {
			a, b := doc.Reboots[i], again.Reboots[i]
			if !a.TS.Equal(b.TS) || a.Completed != b.Completed || a.Source != b.Source || a.Method != b.Method {
				t.Fatalf("reboot %d changed across a round trip\nfirst:  %+v\nsecond: %+v", i, a, b)
			}
			if (a.Remediation == nil) != (b.Remediation == nil) {
				t.Fatalf("reboot %d lost or gained a remediation reference", i)
			}
			if a.Remediation != nil && a.Remediation.Key() != b.Remediation.Key() {
				t.Fatalf("reboot %d remediation identity changed: %q then %q",
					i, a.Remediation.Key(), b.Remediation.Key())
			}
		}

		// Clone must agree with the original on everything that matters, or
		// a caller handing a snapshot to another goroutine is not holding
		// what it thinks it is.
		clone := doc.Clone()
		cloned, err := json.Marshal(clone)
		if err != nil {
			t.Fatalf("a clone will not encode: %v", err)
		}
		if string(cloned) != string(encoded) {
			t.Fatalf("Clone does not round-trip identically\noriginal: %q\nclone:    %q", encoded, cloned)
		}
	})
}

// nest builds a deeply nested JSON object, to check that decoding is bounded.
func nest(depth int) string {
	if depth == 0 {
		return `1`
	}
	return `{"a":` + nest(depth-1) + `}`
}
