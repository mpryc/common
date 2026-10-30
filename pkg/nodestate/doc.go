// Package nodestate reads and writes the node-local state files that medik8s
// remediation agents share.
//
// Several agents may run on one node — storage-based remediation and
// self-node remediation today, others later — and each needs durable local
// notes that survive its own restart and the node's reboot: what it has
// committed to, what it has already done, and what it believed the last time
// it could see the cluster. Each agent also needs to read its peers' notes,
// so that one remediation carried out by two components is counted once.
//
// # The model
//
// One directory, one file per writer, no lock:
//
//	/var/lib/medik8s/state/
//	├── sbr.json   written only by the storage agent;      read by all
//	└── snr.json   written only by the self-remediation agent; read by all
//
// Three properties follow, and they are the whole design:
//
//   - Single writer. A Store writes exactly one file — the one named after
//     its own Component — and refuses any other with ErrNotOwner. A damaged
//     peer file is never repaired, never quarantined, never touched.
//
//   - Atomic replace. A write goes to a temporary file in the same directory,
//     is flushed, renamed over the target, and the directory entry is flushed
//     in turn. A concurrent reader sees the whole previous version or the
//     whole new one. A failure anywhere leaves the previous version in place.
//
//   - No lock. Because there is one writer per file and replacement is
//     atomic, there is nothing to serialise between processes. That is not a
//     simplification for its own sake. The product exists to handle broken
//     storage, and a node whose storage stack is failing produces processes
//     stuck in uninterruptible sleep. A cross-process lock in the feeding
//     path of a hardware watchdog would be a liveness hazard in precisely the
//     state the watchdog is there for.
//
// # What a state file is for, and what it is not for
//
// It is for de-duplication, history and post-mortem.
//
// It is not authority. Any workload privileged enough to run on the node can
// write this directory, so a record here is forgeable. A forged reboot record
// could suppress a fence that should happen; forged fencing evidence could
// cause pods to be force-deleted on a node that is still running them, which
// turns a node-local write into cluster-wide data loss.
//
// The rule that follows is absolute: no state file may ever be the sole
// authority for a destructive action. Evidence that a node rebooted comes
// from node.Status.NodeInfo.BootID read from the Kubernetes API. The file
// says what this agent did; the API says what is true.
//
// # Usage
//
// Construct one Store per agent, naming the component it writes for:
//
//	store, err := nodestate.New(nodestate.ComponentSBR)
//	if err != nil {
//	    return err
//	}
//	if err := store.EnsureDirectory(ctx); err != nil {
//	    return err // a host-path fault; see below
//	}
//
// Classify what you find at start-up. The distinction between "the agent
// restarted" and "the node rebooted" is what keeps a rolling upgrade from
// consuming a node's reboot budget:
//
//	res, err := store.Load(ctx)
//	if err != nil {
//	    return err
//	}
//	switch {
//	case res.Status == nodestate.StatusAbsent:
//	    // First boot. No history, and none was lost.
//	case res.HistoryLost():
//	    // The file was unusable and has been quarantined at
//	    // res.QuarantinePath. Emit an event naming res.Cause, and treat any
//	    // budget as starting from zero.
//	case res.Document.BootID == currentBootID:
//	    // The agent restarted; the node did not. Resume.
//	default:
//	    // The node rebooted. res.Document.Agent.Shutdown says whether it was
//	    // orderly.
//	}
//
// Record through Update, which loads, mutates, trims and writes as one
// serialised step:
//
//	_, err := store.Update(ctx, func(d *nodestate.Document) error {
//	    d.BootID = currentBootID
//	    d.Agent.Shutdown = nodestate.ShutdownRunning
//	    d.AppendReboot(nodestate.RebootEntry{
//	        TS:          time.Now(),
//	        Source:      nodestate.ComponentSBR,
//	        Method:      nodestate.MethodWatchdog,
//	        Reason:      "RemediationCR",
//	        Remediation: &nodestate.CRRef{Kind: "...", UID: string(cr.UID), Generation: cr.Generation},
//	    })
//	    return nil
//	})
//
// Count across both files, so that one remediation counts once however it was
// carried out — and check that the history actually covers the window before
// treating the count as an answer:
//
//	docs := store.ReadAll(ctx)
//	sbr, snr := docs[nodestate.ComponentSBR], docs[nodestate.ComponentSNR]
//
//	autonomous := nodestate.CountReboots(windowStart,
//	    func(r nodestate.RebootEntry) bool { return r.Remediation == nil }, sbr, snr)
//
//	if !nodestate.RebootHistoryComplete(windowStart, sbr, snr) {
//	    // Records from inside the window were aged out or dropped. The count
//	    // is a lower bound, so fail toward counting: a reboot loop is the
//	    // case the budget exists for, and it is the case that produces the
//	    // most records.
//	}
//
// # Bounding the file
//
// The state directory is on the node's root filesystem and nothing prunes it
// but this library, so every collection has a bound and nothing accumulates
// without one.
//
// Store.Update applies the retention policy on every write: the I/O and event
// logs are rings, conditions are capped, and reboot records are trimmed by
// age. Reboots are not trimmed by count in normal operation, because a node
// in a reboot loop is the node with the most records and discarding them
// would hand it a fresh budget.
//
// Two bounds qualify that, in opposite directions. Below, a floor the age
// trim will not go under whatever the records' apparent age: the clock is the
// trim's only input and is least trustworthy exactly when this matters, on a
// node returning from a total outage with NTP about to step it. Above, an
// outer ceiling sized to keep a full document inside the write limit, because
// an agent that cannot write its state file is in the worst position of all.
// Whenever either bound drops a record, Document.RebootHistoryCompleteSince
// moves rather than the history pretending to still be whole.
//
// Store.Cleanup reclaims what is left in the directory rather than in the
// document: temporary files abandoned by a process that was killed mid-write,
// and quarantined copies past their count or age. It runs once per Store on
// the first write and after every quarantine, so an agent that never calls it
// still does not leak; call it from a periodic janitor to get the counts.
//
// All of this is safe to interrupt. Nothing in a sweep writes or replaces the
// live document — it only removes files that are not it — and the document
// itself is only ever replaced by the atomic sequence described above. A
// process killed in the middle of any of it leaves, at worst, a temporary
// file that the next sweep reclaims. There is no instant at which a reader
// sees anything other than a whole state file or no state file.
//
// # Failure handling
//
// Failures are split into three kinds, and the split is the point: a caller
// that treats them alike will get one of them wrong.
//
// A peer's problem is never yours. ReadPeer returns (nil, nil) for an absent,
// empty, truncated, oversized, symlinked, wrongly-owned or future-schema
// peer file. There is no error to handle, by design.
//
// One of those is not the peer's problem, though. A file that is there and
// cannot be read — permission denied, an owner the policy rejects, an
// SELinux denial — is a deployment fault on this node, and flattening it to
// "the peer has never run" hides something that will not fix itself and that
// quietly removes that peer's reboots from every budget merged here. Use
// LoadPeer where the distinction matters and raise a condition on
// PeerRead.Configured.
//
// Your own file repairs itself. Load quarantines an unusable file, preserving
// its bytes under a *.corrupt name so the shape of the damage survives for a
// post-mortem, and returns a fresh Document with StatusRecovered. The caller
// keeps working; what it must do is notice, because its history is gone.
//
// A host-path fault is yours to report, not to act on. If the directory
// cannot be created, has permissions that make its contents untrustworthy, or
// cannot be written, the operation returns a *PathError wrapping one of the
// package sentinels. Being unable to keep a note is evidence about this
// agent's bookkeeping and nothing else: it must never set a storage-health
// condition, and it must never cause a reboot. Equally, an agent that has
// already latched a fence must not disarm its watchdog merely because it
// could not record that fact.
//
// # Concurrency, and the limits of what a library can promise
//
// A Store is safe for concurrent use. Writes and Update cycles within one
// process are serialised, so goroutines cannot lose each other's changes.
//
// Across processes, the single-writer rule is a contract this library cannot
// enforce — nothing stops a second process opening the same path. What it
// does instead is detect the violation. Each write stamps an identity built
// from the kernel boot id, the process id and the process start time, plus
// an increasing revision, and Load reports LoadResult.ForeignWrite when the
// file came from someone else.
//
// It reports and carries on. It never refuses to write, and there is no
// option to make it: an agent that stops recording because it suspects a
// second writer is worse than one that overwrites a record which should not
// have existed. Refusing would also reintroduce, at start-up, exactly the
// liveness hazard the lock-free design exists to avoid — a predecessor stuck
// in uninterruptible sleep would keep its replacement from ever starting.
//
// Deployments must still hold up their end. Set maxSurge to 0 on any
// DaemonSet running an agent, so a rolling update never runs two at once.
// That is the Kubernetes default for a DaemonSet, so it holds unless someone
// changes it — which is a reason to set it explicitly rather than to rely on
// it. One window remains open regardless: a pod stuck terminating, which is
// what a storage fault produces, can have its pod object force-deleted while
// the container is still alive. ForeignWrite is how that becomes visible.
//
// None of these operations is cancellable. The context is checked before work
// begins, but a file operation in progress cannot be interrupted, and on a
// node with failing storage it can block for a long time. Do not place a
// state-file write on the path that feeds a hardware watchdog. If you need a
// bound, run the write on its own goroutine and abandon the result.
//
// # Schema evolution
//
// Add optional members. Do not bump Schema.
//
// Two operators on different release cadences read and write this format, so
// a staggered upgrade is the normal case, not an edge case. Members this
// build does not recognise are preserved verbatim through a read-modify-write
// — the thing encoding/json will not do for you — so a newer peer's data
// survives an older peer rewriting the file. That machinery is what makes
// additive change free.
//
// A Schema bump is not free: every older peer then treats the file as
// unreadable, quarantines it, and starts fresh. Reserve it for a change an
// older build would actively misinterpret, rather than merely ignore.
//
// # Dependencies
//
// This package and its nodestatetest subpackage import only the Go standard
// library. That is a hard constraint, enforced in CI. The file is JSON on
// disk and needs no Kubernetes types, and the consuming operators are on
// different Kubernetes versions; a dependency here would have to be
// reconciled across all of them.
package nodestate
