# SBR integration with the generic host journal

This contract follows the revised Storage-Arbitrated Remediation design, sections 5.2, 5.6 and 5.7, supplied on 2026-11-04. It describes responsibilities; it does not implement the SBR controllers.

## Component selection

SBR owns `sbr.json`. SNR does not participate in this integration. `New(ComponentSBR)` followed by `Snapshot(ctx)` or `ReadAll(ctx)` reads only SBR's file. Missing or corrupt SNR history is irrelevant to that default. Missing SBR history remains unknown.

Other controllers may use `New(Component("other-controller"))` to own their own file. The same owner-only default applies. Multi-component diagnostic reads remain available through explicit component arguments. `KnownComponents()` is a catalog of conventional names, not an installed-component registry or a required peer set.

Only the local executor writes SBR's journal, while holding the separate stable host lock. The local reporter may use `LoadPeer(ctx, ComponentSBR)` or `Snapshot(ctx)` for read-only diagnostics. It must not use `Load`, which synchronizes storage, or any mutation/reconciliation API. Remote lifecycle peers use Kubernetes API records, not host journals. Common does not acquire the executor lock or control the watchdog.

## Status and journal are different contracts

| Current SBR CR status | Common responsibility |
|---|---|
| `nodeUID`, `configUID` | `Document.NodeUID` and opaque config/attempt state retain local context. SBR validates the permanent CR binding. |
| `volumeMode` | SBR status field. Effective configuration uses `sharedStorageVolumeMode` in `Document.Config`. |
| `initialBootID` | Permanent attempt binding in `activeAttempt`; `RebootEntry.BeforeBootID` is diagnostic. |
| `executionPhase` | Derived from the authoritative Node execution record, not a common enum. |
| `observedBootID` | Ordered API observation; `RebootEntry.AfterBootID` is diagnostic and does not prove execution ordering. |
| `cleanup.phase`, `startedAt`, `deadline`, `outOfServiceApplied`, `reason` | Bounded SBR API operation. No common cleanup state machine or resource list. |
| `restrictionsReleased` | SBR Node holder release; not evidence that the Node is schedulable. |

The Node annotation owns Offered → Accepted → BootEnded or Offered → Cancelled. It also retains requested-budget charges, restriction holders and `coordinationRevision`. `Document.Revision` is only a local file revision and must not substitute for that counter or Kubernetes `resourceVersion`.

## Retained SBR payload

The common envelope in the new design is unchanged: schemaVersion 1; node, nodeUID, bootID; string-valued config; agent.shutdown, agent.resetCommitted and agent.extra; diagnostic collections. No migration or backward compatibility layer is required for this unreleased package.

SBR stores exactly these design-defined keys in `Agent.Extra`:

| Key | Encoding | Responsibility |
|---|---|---|
| `schemaVersion` | String `"1"` | SBR validates its own schema, independently of common's schema. |
| `activeAttempt` | JSON object encoded as a string | Stable attempt/request identity, Node/configuration/boot binding, outage epoch and local phase. |
| `resetBudget` | JSON array encoded as a string | Reservations and requested/autonomous accounting that diagnostics cannot replace. |
| `outageState` | JSON object encoded as a string | Retained unresolved epoch and qualified rearm state. |

Common preserves these strings, including unknown consumer fields, across encoding, cloning and diagnostic pruning. It does not parse SBR phases, charge budgets, infer acceptance, or rearm epochs. If required state exceeds the document limit, the write fails without pruning it or replacing the previous file. A consumer can explicitly mutate the map; common cannot validate whether that mutation is authorized.

Requested identity is `(CR UID, Node UID)`; generation changes do not create another request. Config UID and initial boot binding must be validated by SBR. Autonomous entries have their own local attempt ID and must never be adopted by a later CR. `CountReboots` and `FindReboot` are diagnostics, not the authoritative ledger.

## Ordering and recovery

1. The executor locks the host file's separate lock inode and loads the journal.
2. SBR validates retained state, Node identity, configuration, local boot and budgets.
3. Persist the request reservation and stable epoch with the latch false.
4. SBR conditionally accepts the exact Node execution and confirms its outcome.
5. Persist the accepted attempt, ledger charge and latch together with `Store.Update`.
6. Only a successful durable commitment permits the new reset operation.

`ErrDurabilityUnconfirmed` blocks a new reset. Resolve the returned candidate through `ConfirmDurability` without repeating the mutation. A prior durable commitment remains binding in its recorded boot. A valid different boot requires an atomic consumer update clearing only that old latch while preserving attempt and accounting records, before arming the watchdog again.

`StatusAbsent` does not establish NeverInitialized: SBR must prove first installation and known history. `StatusCorrupt` means evidence is retained, with ordinary updates blocked. Unsupported, unreadable and durability-unknown histories also require reconciliation. `ReconcileHistory` requires consumer-authorized reconstruction; an API record cannot reconstruct unreported autonomous attempts merely because it lacks entries.

To establish requested boot completion, the reporter or target first reads the exact Accepted Node record, then reads the current local kernel boot ID, then conditionally publishes matching evidence. A peer validates it before recording BootEnded. Raw Node boot status, a journal entry, or a completed diagnostic record cannot establish this ordering or continuing isolation.

SBR owns watchdog shutdown, nonblocking orchestration, one outstanding journal operation, host-lock lifetime, API conflict handling, budgets and rearm qualification. Common remains standard-library-only. No Kubernetes API mutations or cleanup operations are added here.
