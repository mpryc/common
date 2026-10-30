package nodestate_test

import (
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestSchemaIsPinned makes changing the on-disk format a deliberate act.
//
// The evolution policy is additive: new optional members are preserved
// across a staggered upgrade by the unknown-member machinery, so they cost
// nothing and need no version bump. A bump is a different thing entirely.
// Every peer older than the bump stops being able to read the file at all —
// it quarantines it, starts fresh, and its reboots vanish from every budget
// merged on that node until both sides are upgraded.
//
// That is survivable but it is not a detail, and it is the sort of change
// that otherwise arrives inside a pull request about something else. If this
// test fails, the question to answer in the review is not "is the new format
// better" but "what does every node do during the window where the two
// agents disagree".
func TestSchemaIsPinned(t *testing.T) {
	t.Parallel()

	const pinned = 1

	if nodestate.Schema != pinned {
		t.Fatalf("Schema is %d, pinned at %d.\n\n"+
			"Bumping it makes every older peer treat this file as unreadable: quarantined, "+
			"started fresh, and its reboot history absent from every budget until both agents "+
			"are upgraded. Prefer adding optional members, which are preserved across a "+
			"staggered upgrade and need no bump. If a bump really is necessary, change this "+
			"test deliberately and say in the commit message what a node does during the "+
			"window where the two agents disagree.", nodestate.Schema, pinned)
	}

	if nodestate.MinSupportedSchema > nodestate.Schema {
		t.Fatalf("MinSupportedSchema %d is above Schema %d; nothing would ever read",
			nodestate.MinSupportedSchema, nodestate.Schema)
	}
}
