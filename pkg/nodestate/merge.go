package nodestate

import (
	"sort"
	"time"
)

// MergeReboots combines the reboot histories of several documents into one
// list, oldest first, counting each remediation once.
//
// This is the function the two-file model exists for. With one file per
// writer and no shared lock, "how many times has this node rebooted?" is
// answered by reading every file and merging at read time. Two agents can
// both record the same reboot — the self-node-remediation agent writes a
// pending software reboot, the storage agent records the watchdog release
// that actually carried it out — and that is one reboot, not two. Counting it
// twice would halve the node's effective budget.
//
// Identity is the remediation resource's UID and generation. An entry with no
// remediation reference is autonomous and is never collapsed with anything:
// two independent self-fences are two reboots even if they look alike.
//
// Where two entries describe the same remediation:
//
//   - the timestamp is the earliest, which is when the node committed;
//   - Completed is true if either writer observed completion;
//   - the method, source and reason come from the writer that observed
//     completion, because that is the mechanism that actually took effect;
//   - boot ids and the episode id are filled in from whichever entry has them.
//
// Nil documents are skipped. Inputs are never modified. The result is always
// non-nil.
func MergeReboots(docs ...*Document) []RebootEntry {
	merged := make([]RebootEntry, 0, 16)
	index := make(map[string]int)

	for _, d := range docs {
		if d == nil {
			continue
		}
		for _, entry := range d.Reboots {
			if entry.Remediation == nil {
				merged = append(merged, entry.clone())
				continue
			}
			key := entry.Remediation.Key()
			if at, ok := index[key]; ok {
				merged[at] = combineReboots(merged[at], entry)
				continue
			}
			index[key] = len(merged)
			merged = append(merged, entry.clone())
		}
	}

	sort.SliceStable(merged, func(i, j int) bool {
		if !merged[i].TS.Equal(merged[j].TS) {
			return merged[i].TS.Before(merged[j].TS)
		}
		return rebootSortKey(merged[i]) < rebootSortKey(merged[j])
	})
	return merged
}

// combineReboots folds b into a. See MergeReboots for the rules.
func combineReboots(a, b RebootEntry) RebootEntry {
	out := a.clone()

	// The authoritative description is the one from the writer that saw the
	// reboot complete.
	if b.Completed && !a.Completed {
		out.Source = b.Source
		out.Method = b.Method
		if b.Reason != "" {
			out.Reason = b.Reason
		}
	}
	out.Completed = a.Completed || b.Completed

	if !b.TS.IsZero() && (out.TS.IsZero() || b.TS.Before(out.TS)) {
		out.TS = b.TS
	}
	if out.EpisodeID == "" {
		out.EpisodeID = b.EpisodeID
	}
	if out.BeforeBootID == "" {
		out.BeforeBootID = b.BeforeBootID
	}
	if out.AfterBootID == "" {
		out.AfterBootID = b.AfterBootID
	}
	if out.Method == MethodUnknown {
		out.Method = b.Method
	}
	if out.Source == "" {
		out.Source = b.Source
	}
	if out.Reason == "" {
		out.Reason = b.Reason
	}
	if out.Remediation == nil && b.Remediation != nil {
		ref := *b.Remediation
		out.Remediation = &ref
	}
	return out
}

func rebootSortKey(e RebootEntry) string {
	if e.Remediation != nil {
		return e.Remediation.Key()
	}
	return string(e.Source) + "/" + string(e.Method) + "/" + e.Reason
}

// CountReboots returns the number of distinct reboots at or after since,
// across every supplied document, optionally restricted by match.
//
// It is the primitive behind both budgets the design defines. The autonomous
// budget counts entries with no remediation reference; the requested budget
// counts entries that have one:
//
//	autonomous := nodestate.CountReboots(windowStart,
//	    func(r nodestate.RebootEntry) bool { return r.Remediation == nil },
//	    mine, peer)
//
// A nil match counts everything.
//
// Check Document.RebootHistoryTruncated on the inputs before trusting a count
// to be complete.
func CountReboots(since time.Time, match func(RebootEntry) bool, docs ...*Document) int {
	n := 0
	for _, entry := range MergeReboots(docs...) {
		if entry.TS.Before(since) {
			continue
		}
		if match != nil && !match(entry) {
			continue
		}
		n++
	}
	return n
}

// FindReboot returns the merged record of the reboot that served ref, if any
// document holds one.
//
// It answers "has this remediation already been carried out on this node?",
// which is how a restarted agent avoids rebooting twice for one request. The
// answer is advisory: the state file is a node-local note, and the authority
// for whether a node rebooted is node.Status.NodeInfo.BootID read from the
// API. Use this to suppress a duplicate, never as the sole justification for
// a destructive action.
func FindReboot(ref CRRef, docs ...*Document) (RebootEntry, bool) {
	key := ref.Key()
	var (
		found RebootEntry
		ok    bool
	)
	for _, entry := range MergeReboots(docs...) {
		if entry.Remediation == nil || entry.Remediation.Key() != key {
			continue
		}
		found, ok = entry, true
	}
	return found, ok
}

// PendingReboots returns the merged records that have been committed to but
// not confirmed complete, oldest first.
//
// A pending record surviving a change of boot id is the local evidence that
// the reboot happened and the agent did not live to confirm it.
func PendingReboots(docs ...*Document) []RebootEntry {
	all := MergeReboots(docs...)
	pending := make([]RebootEntry, 0, len(all))
	for _, entry := range all {
		if !entry.Completed {
			pending = append(pending, entry)
		}
	}
	return pending
}
