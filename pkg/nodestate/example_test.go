package nodestate_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/medik8s/common/pkg/nodestate"
)

// exampleDir returns a private directory standing in for
// /var/lib/medik8s/state, which the examples cannot use.
func exampleDir() string {
	dir, err := os.MkdirTemp("", "nodestate-example-")
	if err != nil {
		panic(err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		panic(err)
	}
	return dir
}

// Example shows the shape of an agent's use of the package: construct a
// Store, verify the host path, classify what was there, and record.
func Example() {
	dir := exampleDir()
	defer func() { _ = os.RemoveAll(dir) }()

	ctx := context.Background()

	store, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		panic(err)
	}
	if err := store.EnsureDirectory(ctx); err != nil {
		panic(err) // a host-path fault: report it, do not infer anything about storage
	}

	res, err := store.Load(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("start-up:", res.Status, "history lost:", res.HistoryLost())

	_, err = store.Update(ctx, func(d *nodestate.Document) error {
		d.Node = "worker-0"
		d.BootID = "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8"
		d.Agent.Shutdown = nodestate.ShutdownRunning
		d.AppendEvent(nodestate.EventEntry{
			TS: time.Now(), Type: "Normal", Reason: "AgentStarted",
		})
		return nil
	})
	if err != nil {
		panic(err)
	}

	doc, err := store.Read(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("node:", doc.Node, "revision:", doc.Revision)

	// Output:
	// start-up: Absent history lost: false
	// node: worker-0 revision: 1
}

// ExampleStore_Update_recordAReboot shows the ordering a reboot record
// requires: the intent is written and flushed before the reboot is issued, so
// that a node which does not come back still leaves a trace of why.
func ExampleStore_Update_recordAReboot() {
	dir := exampleDir()
	defer func() { _ = os.RemoveAll(dir) }()

	ctx := context.Background()
	store, _ := nodestate.New(nodestate.ComponentSNR, nodestate.WithDir(dir))

	cr := nodestate.CRRef{
		Kind: "SelfNodeRemediation", Namespace: "medik8s",
		Name: "worker-0", UID: "6f0c6c1e-3f3a-4f1f-9c1e-2b1d4a5e6f70", Generation: 2,
	}

	// Before the reboot: record the intent, not the outcome.
	if _, err := store.Update(ctx, func(d *nodestate.Document) error {
		d.AppendReboot(nodestate.RebootEntry{
			TS:           time.Now(),
			Source:       nodestate.ComponentSNR,
			Method:       nodestate.MethodSoftware,
			Reason:       "RemediationCR",
			EpisodeID:    "ep-4a7c",
			Remediation:  &cr,
			BeforeBootID: "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8",
			Completed:    false,
		})
		return nil
	}); err != nil {
		// The reboot must not be issued if this failed: an unrecorded reboot
		// is indistinguishable from a crash, and cannot be budgeted.
		panic(err)
	}

	// ... the node reboots and the agent starts again ...

	// After the reboot: confirm the entry whose boot id is no longer current.
	if _, err := store.Update(ctx, func(d *nodestate.Document) error {
		for i := range d.Reboots {
			if d.Reboots[i].Completed {
				continue
			}
			d.Reboots[i].Completed = true
			d.Reboots[i].AfterBootID = "c4d5e6f7-0819-2a3b-4c5d-6e7f80912a3b"
		}
		return nil
	}); err != nil {
		panic(err)
	}

	doc, _ := store.Read(ctx)
	fmt.Println("recorded:", len(doc.Reboots), "pending:", len(nodestate.PendingReboots(doc)))

	// Output:
	// recorded: 1 pending: 0
}

// ExampleMergeReboots shows why both files are read. The self-remediation
// agent records a software reboot it requested; the storage agent records the
// watchdog release that actually carried it out. That is one reboot.
func ExampleMergeReboots() {
	at := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	cr := &nodestate.CRRef{UID: "uid-1", Generation: 1}

	snr := nodestate.NewDocument(nodestate.ComponentSNR)
	snr.AppendReboot(nodestate.RebootEntry{
		TS: at, Source: nodestate.ComponentSNR, Method: nodestate.MethodSoftware,
		Remediation: cr, Completed: false,
	})

	sbr := nodestate.NewDocument(nodestate.ComponentSBR)
	sbr.AppendReboot(nodestate.RebootEntry{
		TS: at.Add(2 * time.Second), Source: nodestate.ComponentSBR, Method: nodestate.MethodWatchdog,
		Remediation: cr, Completed: true,
	})
	sbr.AppendReboot(nodestate.RebootEntry{
		TS: at.Add(time.Hour), Source: nodestate.ComponentSBR, Method: nodestate.MethodWatchdog,
		Reason: "TotalOutage", Completed: true,
	})

	for _, r := range nodestate.MergeReboots(snr, sbr) {
		fmt.Printf("%s by %s via %s\n", r.TS.Format(time.RFC3339), r.Source, r.Method)
	}

	window := at.Add(-time.Hour)
	requested := nodestate.CountReboots(window,
		func(r nodestate.RebootEntry) bool { return r.Remediation != nil }, snr, sbr)
	autonomous := nodestate.CountReboots(window,
		func(r nodestate.RebootEntry) bool { return r.Remediation == nil }, snr, sbr)
	fmt.Println("requested:", requested, "autonomous:", autonomous)

	// A count is only an answer if the history covers the window it is taken
	// over. Always ask.
	fmt.Println("history covers the window:", nodestate.RebootHistoryComplete(window, snr, sbr))

	// Output:
	// 2024-06-01T12:00:00Z by sbr via watchdog
	// 2024-06-01T13:00:00Z by sbr via watchdog
	// requested: 1 autonomous: 1
	// history covers the window: true
}

// ExampleStore_Cleanup shows the housekeeping call. A process killed between
// creating a temporary file and renaming it into place — which is what a
// watchdog reset does — leaves the temporary file behind, and nothing else
// ever looks for it.
func ExampleStore_Cleanup() {
	dir := exampleDir()
	defer func() { _ = os.RemoveAll(dir) }()

	ctx := context.Background()
	store, _ := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(dir),
		// A live agent leaves this at the default hour: a write still in
		// flight must never be mistaken for an abandoned one.
		nodestate.WithOrphanGrace(0))

	// Stand in for what a killed process leaves behind.
	_ = os.WriteFile(filepath.Join(dir, ".sbr.json.tmp-0123456789abcdef"), []byte(`{"schemaVer`), 0o600)

	report, err := store.Cleanup(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("abandoned writes reclaimed:", report.TemporaryFilesRemoved)

	// Output:
	// abandoned writes reclaimed: 1
}

// ExampleStore_ReadPeer shows the tolerance contract. A peer's bookkeeping
// problem reads as "no information", never as an error to handle.
func ExampleStore_ReadPeer() {
	dir := exampleDir()
	defer func() { _ = os.RemoveAll(dir) }()

	ctx := context.Background()
	store, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))

	// A peer that has written nothing yet.
	doc, err := store.ReadPeer(ctx, nodestate.ComponentSNR)
	fmt.Println("absent:", doc == nil, "error:", err)

	// A peer mid-crash, having left a truncated file behind.
	_ = os.WriteFile(filepath.Join(dir, nodestate.SNRFile), []byte(`{"schemaVersion":1,"nod`), 0o600)
	doc, err = store.ReadPeer(ctx, nodestate.ComponentSNR)
	fmt.Println("truncated:", doc == nil, "error:", err)

	// Output:
	// absent: true error: <nil>
	// truncated: true error: <nil>
}

// ExampleStore_WriteTo shows the ownership rule being enforced rather than
// merely documented.
func ExampleStore_WriteTo() {
	dir := exampleDir()
	defer func() { _ = os.RemoveAll(dir) }()

	store, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))

	err := store.WriteTo(context.Background(), nodestate.ComponentSNR,
		nodestate.NewDocument(nodestate.ComponentSBR))

	fmt.Println("is ErrNotOwner:", errors.Is(err, nodestate.ErrNotOwner))

	// Output:
	// is ErrNotOwner: true
}
