package nodestate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Size and retention defaults for a Store.
const (
	// DefaultMaxDocumentBytes caps what a Store will write. A document that
	// large means a caller is accumulating something it should be trimming,
	// and a state directory on a node's root filesystem is not the place to
	// find that out.
	DefaultMaxDocumentBytes = 1 << 20 // 1 MiB

	// DefaultMaxReadBytes caps what a Store will read into memory. It bounds
	// the cost of a hostile or runaway file; anything larger is treated as
	// unreadable rather than parsed.
	DefaultMaxReadBytes = 8 << 20 // 8 MiB

	// DefaultQuarantineMaxAge is how long a quarantined copy is kept before
	// it is reclaimed. Evidence of a fault nobody looked at in a month is no
	// longer evidence; it is just a file on a node's root filesystem.
	DefaultQuarantineMaxAge = 30 * 24 * time.Hour

	// DefaultOrphanGrace is how long a temporary file must have gone
	// untouched before a sweep treats it as abandoned rather than in flight.
	// Generous on purpose: reclaiming a live write would be far worse than
	// leaving a dead one in place for an hour.
	DefaultOrphanGrace = time.Hour

	// DefaultMaxPreservedBytes caps the total size of the members this build
	// does not recognise but carries forward. Preservation exists so a newer
	// peer's few extra fields survive a downgrade, not so the file can be
	// used as storage: without a cap, anything able to write the directory
	// could stuff it past DefaultMaxDocumentBytes and leave the agent unable
	// to write its own state ever again.
	DefaultMaxPreservedBytes = 64 << 10 // 64 KiB

	// DefaultQuarantineRetention is how many quarantined copies of a file are
	// kept. More than one is useful because a boot loop produces one per boot
	// and the first is usually the informative one; keeping them all would let
	// that loop fill the host path.
	DefaultQuarantineRetention = 3

	// quarantineSuffix terminates every quarantined file name, so operators
	// and log scrapers can find them with one glob.
	quarantineSuffix = ".corrupt"

	// quarantineTimeLayout is fixed-width and lexically sortable, which is
	// what lets the pruner order quarantined files by name alone.
	quarantineTimeLayout = "20060102T150405.000000000Z"

	// EventReasonStateFileRecovered is the Reason this package writes into
	// Document.Events when it quarantines an unusable file and starts fresh.
	// A caller that surfaces state-file events to Kubernetes should give this
	// one a warning event of its own: it means the node's recorded history is
	// gone, and any budget computed from it restarts at zero.
	EventReasonStateFileRecovered = "StateFileRecovered"

	// EventReasonClockWentBackwards is the Reason recorded when the wall
	// clock reads earlier than the previous write of this file. Durations
	// derived from the history across that point are unreliable, which
	// matters to anything computing a reboot budget over a time window.
	EventReasonClockWentBackwards = "ClockWentBackwards"

	// EventReasonPreservedFieldsDropped is the Reason recorded when members
	// written by a newer peer exceeded the preservation ceiling and were
	// dropped rather than carried forward. It means a staggered upgrade will
	// lose that peer's data through this agent, which is worth an alert even
	// though the alternative was worse. See Store.capPreserved.
	EventReasonPreservedFieldsDropped = "PreservedFieldsDropped"
)

// Status classifies what Store.Load found on disk.
type Status int

const (
	// StatusOK means the file was read and understood.
	StatusOK Status = iota
	// StatusAbsent means there is no file. This is a first boot, not a fault:
	// history was never written, as opposed to lost.
	StatusAbsent
	// StatusRecovered means the file existed but could not be used, so it was
	// quarantined and a fresh document returned. The caller has lost its
	// history and should say so — emit an event, and treat any budget it
	// computes as starting from nothing.
	StatusRecovered
	// StatusIncompatible means the file declared a schema version this build
	// cannot interpret, most likely because a newer agent wrote it and the
	// node was then downgraded. The bytes are preserved in the quarantine
	// file; the running agent starts fresh so that it can keep writing.
	StatusIncompatible
)

// String implements fmt.Stringer.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusAbsent:
		return "Absent"
	case StatusRecovered:
		return "Recovered"
	case StatusIncompatible:
		return "Incompatible"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// LoadResult is what Store.Load returns. Document is always usable, whatever
// the status, so a caller that only wants to get on with its work can ignore
// everything else — but the start-up classification the design calls for
// needs all of it.
type LoadResult struct {
	// Document is the loaded state, or a fresh one when there was nothing
	// usable to load. Never nil when Load returns a nil error.
	Document *Document

	// Status says where Document came from.
	Status Status

	// Cause is why the file could not be used, when Status is StatusRecovered
	// or StatusIncompatible. Classify it with errors.Is against ErrCorrupt,
	// ErrSchemaUnsupported, ErrSymlink, ErrNotRegular, ErrInsecurePermissions,
	// ErrInsecureOwner or ErrTooLarge.
	Cause error

	// QuarantinePath is where the unusable file was moved, when there was one.
	QuarantinePath string

	// ForeignWrite reports that the file was last written by someone other
	// than this Store, which should be impossible under the single-writer
	// contract. See Store.Load for what the library does about it.
	ForeignWrite bool
}

// HistoryLost reports whether the previous contents of the file are no longer
// available to this caller. It is true for StatusRecovered and
// StatusIncompatible, and false for a first boot.
//
// A caller computing a reboot budget must treat a true result as "the budget
// restarts from zero, and that fact is worth an event", not as "this node has
// never rebooted".
func (r *LoadResult) HistoryLost() bool {
	return r != nil && (r.Status == StatusRecovered || r.Status == StatusIncompatible)
}

// Clock supplies the current time. Replace it in tests with
// nodestatetest.FakeClock.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns the Clock backed by time.Now.
func SystemClock() Clock { return systemClock{} }

// Store reads and writes node-local state files on behalf of one component.
//
// A Store owns exactly one file, the one named after its component, and will
// not write any other. That single-writer rule is what makes the atomic
// replace safe without a lock, and the lock is what the design could not
// afford: a cross-process lock in the feeding path of a hardware watchdog is
// a liveness hazard in precisely the storage failure the product exists to
// handle.
//
// A Store is safe for concurrent use. Writes and read-modify-write cycles
// from one process are serialised internally. Serialising *other* processes
// is not something a library can do, and is not attempted; see Load for how a
// foreign write is detected and reported.
//
// Construct one with New.
type Store struct {
	self      Component
	dir       string
	fs        FS
	clock     Clock
	retention RetentionPolicy
	security  SecurityPolicy

	maxDocumentBytes    int
	maxReadBytes        int
	maxPreservedBytes   int
	quarantineRetention int
	quarantineMaxAge    time.Duration
	orphanGrace         time.Duration
	strictOwnership     bool
	writerID            string

	// mu serialises read-modify-write cycles on this file. It is held across
	// file I/O, which is why none of these operations is cancellable: see the
	// package documentation on what to do about that in a watchdog path.
	//
	// It is shared by every Store in this process that targets the same path,
	// not owned by this one. A consumer that builds two Stores for its own
	// component — one in main, one in a controller — is making a mistake, but
	// it is a mistake that would otherwise show up as silently lost records
	// rather than as anything diagnosable. Sharing the lock removes the whole
	// class, and leaves ForeignWrite meaning what it says: another *process*
	// wrote this file.
	// The three fields below are per-Store rather than per-file, but every
	// access to them happens under mu, so mu covers them too.
	mu           *sync.Mutex
	lastRevision uint64
	haveWritten  bool
	swept        bool
}

// fileLocks maps a state file path to the mutex serialising access to it
// within this process. Entries are never removed: a process uses one or two
// paths, and a test suite a few hundred, so the map is bounded in practice by
// the number of distinct directories a binary touches.
var fileLocks sync.Map // string -> *sync.Mutex

func lockForPath(path string) *sync.Mutex {
	if existing, ok := fileLocks.Load(path); ok {
		return existing.(*sync.Mutex)
	}
	actual, _ := fileLocks.LoadOrStore(path, &sync.Mutex{})
	return actual.(*sync.Mutex)
}

// config is the resolved option set. It is unexported: callers configure a
// Store through Option values, which keeps the defaults in one place and
// lets new settings be added without breaking anyone.
type config struct {
	dir                 string
	fs                  FS
	clock               Clock
	retention           RetentionPolicy
	security            SecurityPolicy
	maxDocumentBytes    int
	maxReadBytes        int
	maxPreservedBytes   int
	quarantineRetention int
	quarantineMaxAge    time.Duration
	orphanGrace         time.Duration
	strictOwnership     bool
	writerID            string
}

// Option configures a Store. See the With functions in this package.
type Option func(*config) error

// WithDir overrides the state directory. The default is Dir, which is the
// path the medik8s contract fixes; override it in tests, or in a deployment
// that mounts the host path somewhere else.
//
// The path must be absolute.
func WithDir(dir string) Option {
	return func(c *config) error {
		if dir == "" {
			return errors.New("nodestate: WithDir: the directory must not be empty")
		}
		if !filepath.IsAbs(dir) {
			return fmt.Errorf("nodestate: WithDir: %q is not absolute; a relative state directory would follow the process working directory", dir)
		}
		c.dir = filepath.Clean(dir)
		return nil
	}
}

// WithFileSystem replaces the filesystem implementation. Production callers
// do not need this; it exists so that consumers can test their behaviour when
// the host path fails. See the nodestatetest subpackage.
func WithFileSystem(filesystem FS) Option {
	return func(c *config) error {
		if filesystem == nil {
			return errors.New("nodestate: WithFileSystem: the filesystem must not be nil")
		}
		c.fs = filesystem
		return nil
	}
}

// WithClock replaces the time source.
func WithClock(clock Clock) Option {
	return func(c *config) error {
		if clock == nil {
			return errors.New("nodestate: WithClock: the clock must not be nil")
		}
		c.clock = clock
		return nil
	}
}

// WithRetention replaces the retention policy applied by Store.Update.
func WithRetention(policy RetentionPolicy) Option {
	return func(c *config) error {
		if err := policy.Validate(); err != nil {
			return err
		}
		c.retention = policy
		return nil
	}
}

// WithSecurityPolicy replaces the permission and ownership requirements.
func WithSecurityPolicy(policy SecurityPolicy) Option {
	return func(c *config) error {
		if err := policy.Validate(); err != nil {
			return err
		}
		c.security = policy
		return nil
	}
}

// WithMaxDocumentBytes caps the encoded size of a document the Store will
// write. A larger document fails with ErrTooLarge and the file on disk is
// left as it was.
func WithMaxDocumentBytes(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("nodestate: WithMaxDocumentBytes: %d must be positive", n)
		}
		c.maxDocumentBytes = n
		return nil
	}
}

// WithMaxReadBytes caps how much of a file the Store will read into memory.
// A larger file reads as unusable: quarantined for the owner, absent for a
// peer.
func WithMaxReadBytes(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("nodestate: WithMaxReadBytes: %d must be positive", n)
		}
		c.maxReadBytes = n
		return nil
	}
}

// WithQuarantineRetention sets how many quarantined copies of the owned file
// are kept. It must be at least one: the evidence in the first bad file is
// usually the only record of what went wrong.
func WithQuarantineRetention(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("nodestate: WithQuarantineRetention: %d must be at least 1", n)
		}
		c.quarantineRetention = n
		return nil
	}
}

// WithQuarantineMaxAge sets how long a quarantined copy is kept. Zero means
// forever, which leaves reclaiming them to the operator.
func WithQuarantineMaxAge(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("nodestate: WithQuarantineMaxAge: %s must not be negative", d)
		}
		c.quarantineMaxAge = d
		return nil
	}
}

// WithOrphanGrace sets how long a temporary file must have gone untouched
// before a sweep reclaims it.
//
// Lower it only where no other process can possibly be writing this
// component's file, such as a test. Setting it too low on a live node risks
// deleting a write that is merely slow, which on failing storage is the
// normal case.
func WithOrphanGrace(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("nodestate: WithOrphanGrace: %s must not be negative", d)
		}
		c.orphanGrace = d
		return nil
	}
}

// WithMaxPreservedBytes caps the total size of unrecognised members carried
// forward from a newer peer. Beyond it they are dropped and the fact is
// recorded as an EventReasonPreservedFieldsDropped event.
func WithMaxPreservedBytes(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("nodestate: WithMaxPreservedBytes: %d must not be negative", n)
		}
		c.maxPreservedBytes = n
		return nil
	}
}

// WithStrictOwnership makes Store.Update fail with ErrForeignWrite when the
// file was last written by another writer, instead of proceeding.
//
// The default is to proceed, because for an agent whose job is to fence a
// node, refusing to record state is worse than recording it over someone
// else's. Turn this on where a lost record is worse than a stalled agent —
// in a test, or in a tool that audits the directory.
func WithStrictOwnership(strict bool) Option {
	return func(c *config) error {
		c.strictOwnership = strict
		return nil
	}
}

// WithWriterID overrides the identifier stamped into Document.WriterID. The
// default is random per Store, which is what makes a foreign write
// detectable. Set it to something stable only if you have a reason.
func WithWriterID(id string) Option {
	return func(c *config) error {
		if id == "" {
			return errors.New("nodestate: WithWriterID: the identifier must not be empty")
		}
		c.writerID = id
		return nil
	}
}

// New returns a Store that owns self's state file.
//
// It performs no file operations: a Store can be constructed before the host
// path is mounted, and nothing touches the disk until the first Load, Write,
// Update or EnsureDirectory. Options are applied in order and the first
// failure is returned.
func New(self Component, opts ...Option) (*Store, error) {
	if err := self.Validate(); err != nil {
		return nil, err
	}

	cfg := config{
		dir:                 Dir,
		fs:                  OSFileSystem(),
		clock:               SystemClock(),
		retention:           DefaultRetention(),
		security:            DefaultSecurityPolicy(),
		maxDocumentBytes:    DefaultMaxDocumentBytes,
		maxReadBytes:        DefaultMaxReadBytes,
		maxPreservedBytes:   DefaultMaxPreservedBytes,
		quarantineRetention: DefaultQuarantineRetention,
		quarantineMaxAge:    DefaultQuarantineMaxAge,
		orphanGrace:         DefaultOrphanGrace,
	}
	for i, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("nodestate: New: option %d is nil", i)
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	writerID := cfg.writerID
	if writerID == "" {
		id, err := randomSuffix()
		if err != nil {
			return nil, fmt.Errorf("nodestate: New: generating a writer id: %w", err)
		}
		writerID = id
	}

	return &Store{
		self:                self,
		dir:                 cfg.dir,
		mu:                  lockForPath(filepath.Join(cfg.dir, filepath.Base(self.FileName()))),
		fs:                  cfg.fs,
		clock:               cfg.clock,
		retention:           cfg.retention,
		security:            cfg.security,
		maxDocumentBytes:    cfg.maxDocumentBytes,
		maxReadBytes:        cfg.maxReadBytes,
		maxPreservedBytes:   cfg.maxPreservedBytes,
		quarantineRetention: cfg.quarantineRetention,
		quarantineMaxAge:    cfg.quarantineMaxAge,
		orphanGrace:         cfg.orphanGrace,
		strictOwnership:     cfg.strictOwnership,
		writerID:            writerID,
	}, nil
}

// Self returns the component this Store writes for.
func (s *Store) Self() Component { return s.self }

// Directory returns the state directory.
func (s *Store) Directory() string { return s.dir }

// Path returns the file path of c's state file. The component is reduced to
// its base name, so an unvalidated value cannot escape the state directory.
func (s *Store) Path(c Component) string {
	return filepath.Join(s.dir, filepath.Base(c.FileName()))
}

// WriterID returns the identifier this Store stamps into the documents it
// writes.
func (s *Store) WriterID() string { return s.writerID }

// EnsureDirectory creates the state directory if it is missing and verifies
// it against the security policy if it is not.
//
// Call it once at start-up to get a clear error early. Every write path calls
// it too, so skipping it is safe; what you lose is the chance to report a
// misconfigured host path before the first thing you need to record.
//
// It never relaxes or repairs an existing directory's mode: an unexpected
// mode means something else is managing the path, and that is worth an alert
// rather than a silent fix.
func (s *Store) EnsureDirectory(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureDirectory()
}

func (s *Store) ensureDirectory() error {
	fi, err := s.fs.Lstat(s.dir)
	switch {
	case err == nil:
		return s.security.checkDirectory(s.dir, fi)
	case !errors.Is(err, fs.ErrNotExist):
		return pathErrorf("lstat", s.dir, err)
	}

	if err := s.fs.MkdirAll(s.dir, s.security.DirMode); err != nil {
		return pathErrorf("mkdir", s.dir, err)
	}

	// Verify before adjusting the mode, not after. MkdirAll treats an existing
	// directory as success and resolves symlinks to find it, so a link planted
	// at this path between the check above and here would be followed — and
	// Chmod follows symlinks too. Checking first means the only thing this
	// code will ever chmod is something Lstat has just confirmed is a real
	// directory.
	fi, err = s.fs.Lstat(s.dir)
	if err != nil {
		return pathErrorf("lstat", s.dir, err)
	}
	if err := s.security.checkDirectory(s.dir, fi); err != nil {
		return err
	}

	// MkdirAll applies the process umask, so the directory may be more
	// restrictive than asked for but never less. Set the mode explicitly, and
	// only when it differs, so the result does not depend on how the agent was
	// launched.
	if fi.Mode().Perm() == s.security.DirMode.Perm() {
		return nil
	}
	if err := s.fs.Chmod(s.dir, s.security.DirMode); err != nil {
		return pathErrorf("chmod", s.dir, err)
	}
	fi, err = s.fs.Lstat(s.dir)
	if err != nil {
		return pathErrorf("lstat", s.dir, err)
	}
	return s.security.checkDirectory(s.dir, fi)
}

// Load reads this Store's own file and classifies what it found.
//
// Load always returns a usable Document unless it returns an error, and it
// returns an error only for a condition the caller cannot work around: a
// cancelled context, a state directory that violates the security policy, or
// a failure to move an unusable file out of the way. Everything about the
// file's *contents* is handled here:
//
//   - no file: StatusAbsent and a fresh document;
//   - unreadable or unparseable: the file is quarantined with its bytes
//     intact, and StatusRecovered is returned with a fresh document;
//   - a schema version this build does not support: quarantined likewise,
//     StatusIncompatible.
//
// Quarantining is a write, and a Store only writes its own file. A peer's
// damaged file is never touched — see ReadPeer.
//
// Load also reports a foreign write. Document.WriterID identifies the Store
// instance that last wrote the file; finding another one means a second
// process is writing a file this one owns, which the single-writer contract
// forbids. The default is to report it in LoadResult.ForeignWrite and carry
// on, because stalling a fencing agent is worse than overwriting a record
// that should not exist. WithStrictOwnership inverts that.
func (s *Store) Load(ctx context.Context) (*LoadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

// Read is Load without the classification, for a caller that only wants the
// current contents.
//
// Do not use it for the start-up path. Read cannot distinguish "this node has
// never rebooted" from "the history was unusable and has just been thrown
// away", and those lead to opposite decisions about a reboot budget. Use Load
// there and inspect LoadResult.HistoryLost. The recovery is also recorded as
// an EventReasonStateFileRecovered entry in the returned document, so it is
// not invisible — but a status code is the thing to branch on.
func (s *Store) Read(ctx context.Context) (*Document, error) {
	res, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	return res.Document, nil
}

func (s *Store) loadLocked() (*LoadResult, error) {
	// A directory that is absent is a first boot; one that exists but is
	// insecure is a fault the caller has to know about, because nothing read
	// from inside it can be trusted.
	if err := s.verifyDirectoryIfPresent(); err != nil {
		return nil, err
	}

	path := s.Path(s.self)
	doc, err := s.readDocument(path, s.self)
	switch {
	case err == nil:
		res := &LoadResult{Document: doc, Status: StatusOK}
		res.ForeignWrite = s.isForeign(doc)
		s.capPreserved(doc)
		return res, nil

	case errors.Is(err, fs.ErrNotExist):
		return &LoadResult{Document: s.newDocument(), Status: StatusAbsent}, nil
	}

	status := StatusRecovered
	if errors.Is(err, ErrSchemaUnsupported) {
		status = StatusIncompatible
	}

	quarantined, qerr := s.quarantine(path)
	if qerr != nil {
		// The file cannot be used and cannot be moved aside. Writing over it
		// would destroy evidence of whatever is wrong with this host path, so
		// the caller is told instead.
		return nil, fmt.Errorf("nodestate: %s is unusable (%v) and could not be quarantined: %w", path, err, qerr)
	}

	// Record the loss in the fresh document as well as in the result.
	//
	// The result is the structured signal, and a caller classifying its
	// start-up will act on it. But Read and Update return only the document,
	// and recovery can happen on any call, not just the first — so without
	// this, an agent that only ever calls Update would lose its history with
	// nothing anywhere to say so. The event ring is the file's own record of
	// what happened to it, which is exactly what a post-mortem reads.
	fresh := s.newDocument()
	fresh.AppendEvent(EventEntry{
		TS:     s.clock.Now(),
		Type:   "Warning",
		Reason: EventReasonStateFileRecovered,
		Message: fmt.Sprintf("the previous state file was unusable and was quarantined at %s; history before this point is lost: %v",
			filepath.Base(quarantined), err),
	})

	return &LoadResult{
		Document:       fresh,
		Status:         status,
		Cause:          err,
		QuarantinePath: quarantined,
	}, nil
}

// verifyDirectoryIfPresent checks the state directory, treating its absence
// as acceptable.
func (s *Store) verifyDirectoryIfPresent() error {
	fi, err := s.fs.Lstat(s.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return pathErrorf("lstat", s.dir, err)
	}
	return s.security.checkDirectory(s.dir, fi)
}

// capPreserved enforces the ceiling on unrecognised members carried forward
// from a newer peer.
//
// The ceiling matters because preservation is unconditional and the document
// has a write limit. A newer peer adding a handful of fields costs a few
// hundred bytes and is exactly what the mechanism is for. A file containing
// a megabyte of unrecognised members — whether a peer with a bug or a
// workload with write access to the directory — would be read, carried, and
// then refused by the write limit, leaving this agent unable to record
// anything for as long as that file sat there. Dropping is the lesser loss:
// preservation is a courtesy to a peer, and writing is this agent's job.
//
// All of them go, rather than an arbitrary subset, so the outcome does not
// depend on map iteration order.
func (s *Store) capPreserved(doc *Document) {
	if s.maxPreservedBytes <= 0 {
		return
	}
	total := doc.preservedBytes()
	if total <= s.maxPreservedBytes {
		return
	}
	doc.dropPreserved()
	doc.AppendEvent(EventEntry{
		TS:     s.clock.Now(),
		Type:   "Warning",
		Reason: EventReasonPreservedFieldsDropped,
		Message: fmt.Sprintf("dropped %d bytes of members written by a newer peer, over the %d byte limit; "+
			"a staggered upgrade will not carry that peer's data through this agent", total, s.maxPreservedBytes),
	})
}

func (s *Store) isForeign(doc *Document) bool {
	if !s.haveWritten {
		// Nothing written yet, so there is nothing to contradict. A file left
		// by a previous run of this same agent is the normal case.
		return false
	}
	return doc.WriterID != s.writerID || doc.Revision < s.lastRevision
}

func (s *Store) newDocument() *Document {
	return NewDocument(s.self)
}

// ReadPeer reads another component's file.
//
// It returns (nil, nil) for every condition that is the peer's problem rather
// than the caller's: no file, an empty or truncated one, a schema version
// this build does not understand, a symlink, a directory, a file with
// permissions that make its contents untrustworthy, or one too large to read.
// That tolerance is deliberate and is part of the contract. An agent deciding
// whether to fence a node must not be stopped by its peer's bookkeeping, and
// a peer's file carries history and de-duplication hints, never authority.
//
// An error is returned only for a programming mistake — an invalid component
// name — or a cancelled context.
//
// ReadPeer never writes. A peer's damaged file is left exactly as found, for
// its owner to quarantine.
//
// Passing this Store's own component is allowed and reads the Store's own
// file with the same tolerance — no quarantine, no classification. That is
// occasionally what a diagnostic tool wants; it is not what an agent wants at
// start-up, which is Load.
func (s *Store) ReadPeer(ctx context.Context, c Component) (*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	doc, err := s.readDocument(s.Path(c), c)
	if err != nil {
		return nil, nil
	}
	return doc, nil
}

// ReadAll reads several components' files and returns the ones that could be
// read, keyed by component. With no components named it reads
// KnownComponents.
//
// Nothing is written, including for this Store's own file: ReadAll is a
// read-only view and does not quarantine. Use it to assemble the inputs to
// MergeReboots:
//
//	docs := store.ReadAll(ctx)
//	reboots := nodestate.MergeReboots(docs[nodestate.ComponentSBR], docs[nodestate.ComponentSNR])
//
// An unreadable file is simply absent from the result. There is no error
// return by design: every failure mode here is one the caller must carry on
// through.
func (s *Store) ReadAll(ctx context.Context, components ...Component) map[Component]*Document {
	out := make(map[Component]*Document, len(components))
	if ctx.Err() != nil {
		return out
	}
	if len(components) == 0 {
		components = KnownComponents()
		if !containsComponent(components, s.self) {
			components = append(components, s.self)
		}
	}
	seen := make(map[Component]struct{}, len(components))
	for _, c := range components {
		if _, done := seen[c]; done {
			continue // a repeated component is read once, not once per mention
		}
		seen[c] = struct{}{}
		if err := c.Validate(); err != nil {
			continue
		}
		doc, err := s.readDocument(s.Path(c), c)
		if err != nil {
			continue
		}
		out[c] = doc
	}
	return out
}

func containsComponent(list []Component, want Component) bool {
	for _, c := range list {
		if c == want {
			return true
		}
	}
	return false
}

// Write replaces this Store's file with doc.
//
// The replacement is atomic for any reader: the bytes are written to a
// temporary file in the same directory, flushed, renamed over the target, and
// the directory entry is flushed in turn. A concurrent reader sees either the
// whole previous version or the whole new one, never a mixture and never an
// empty file. A failure at any step leaves the previous version in place.
//
// Write stamps SchemaVersion, Agent.Component, UpdatedAt, WriterID and
// Revision on doc, so the caller's copy reflects what was written.
//
// Write does not apply the retention policy — it writes what it is given, and
// fails with ErrTooLarge if that exceeds the configured size. Prefer Update,
// which loads, mutates, trims and writes in one serialised step.
//
// One error does not mean the write was lost. ErrDurabilityUnconfirmed says
// the replacement is already visible and only its survival across a power
// loss is unproven; treat the document as written. Every other error means
// the previous version is still in place and nothing changed. A caller that
// gates an action on the record reaching disk — recording a pending fence
// before issuing it — must distinguish the two, or it will withhold a fence
// whose record is already there.
func (s *Store) Write(ctx context.Context, doc *Document) error {
	return s.WriteTo(ctx, s.self, doc)
}

// WriteTo is Write with the target component named explicitly. It returns
// ErrNotOwner, without touching the filesystem, for any component other than
// this Store's own.
//
// It exists so the ownership rule is enforced at a call site that can express
// a violation, rather than only being documented. Code that iterates over
// components and writes each one is a bug this turns into an error.
func (s *Store) WriteTo(ctx context.Context, c Component, doc *Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return err
	}
	if c != s.self {
		return fmt.Errorf("%w: this store writes %q and may not write %q; each file has exactly one writer",
			ErrNotOwner, s.self, c)
	}
	if doc == nil {
		return errors.New("nodestate: Write: the document must not be nil")
	}
	if doc.Agent.Component != "" && doc.Agent.Component != s.self {
		return fmt.Errorf("%w: the document is stamped %q but this store writes %q",
			ErrNotOwner, doc.Agent.Component, s.self)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(doc)
}

func (s *Store) writeLocked(doc *Document) error {
	doc.SchemaVersion = Schema
	doc.Agent.Component = s.self
	doc.UpdatedAt = s.clock.Now()
	doc.WriterID = s.writerID
	doc.Revision = s.nextRevision(doc.Revision)

	encoded, err := encodeDocument(doc)
	if err != nil {
		return fmt.Errorf("nodestate: encoding the document for %s: %w", s.self, err)
	}
	if len(encoded) > s.maxDocumentBytes {
		return fmt.Errorf("%w: the encoded document is %d bytes, over the %d byte limit; trim the history you are accumulating",
			ErrTooLarge, len(encoded), s.maxDocumentBytes)
	}

	if err := s.ensureDirectory(); err != nil {
		return err
	}

	// Reclaim anything a previous process left behind before adding to it.
	// Once per Store: the cost is one directory listing per process, and
	// skipping it would let a crash loop fill the host path with temporary
	// files that nothing else ever looks for.
	if !s.swept {
		_, _ = s.sweep()
	}

	err = s.atomicReplace(s.Path(s.self), encoded)
	if err != nil && !errors.Is(err, ErrDurabilityUnconfirmed) {
		return err
	}

	// Commit the bookkeeping for an unconfirmed-durability result too. The
	// new version is on disk and visible; pretending otherwise would reissue
	// this revision on the next write, leaving two different documents
	// claiming the same one and breaking the ordering that foreign-write
	// detection reads.
	s.lastRevision = doc.Revision
	s.haveWritten = true
	return err
}

// nextRevision never lets the revision go backwards, including when the file
// on disk has a higher one than this Store last wrote. That can only happen
// after a foreign write, and continuing from the larger value keeps the
// sequence meaningful to anyone reading the history afterwards.
func (s *Store) nextRevision(onDisk uint64) uint64 {
	next := s.lastRevision
	if onDisk > next {
		next = onDisk
	}
	return next + 1
}

func encodeDocument(doc *Document) ([]byte, error) {
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	// A trailing newline costs one byte and makes the file behave for every
	// text tool an operator will reach for on a broken node.
	return append(encoded, '\n'), nil
}

// Update performs a serialised read-modify-write cycle on this Store's file.
//
// This is the method to use. It loads the current document — recovering from
// a damaged file exactly as Load does — passes it to mutate, applies the
// retention policy, and writes the result. The whole cycle holds the Store's
// lock, so concurrent callers in one process cannot lose each other's
// changes.
//
// The mutator may return ErrNoChange to skip the write; Update then returns
// the unmodified document and a nil error. Any other error from the mutator
// aborts the cycle with nothing written, so a caller that discovers mid-way
// that it should not proceed can say so without leaving a half-applied
// document behind.
//
// The returned document is the one that was written, including the stamped
// revision and timestamp. It is the Store's own copy; Clone it before handing
// it to another goroutine.
//
// The mutator runs while the Store's lock is held. Keep it short, and do not
// call back into this Store from it — that deadlocks. In particular, read a
// peer's file before entering Update, not inside it.
//
// Like Read, Update recovers a damaged file silently and returns only the
// document, so it is not the call to classify a start-up with. When it does
// recover, the mutator sees a fresh document already carrying an
// EventReasonStateFileRecovered entry, which is both the signal and the
// durable record.
//
// Update returns a non-nil document together with ErrDurabilityUnconfirmed:
// that write is visible and only its durability is in doubt. For every other
// error the document is nil and nothing was written. See Write.
func (s *Store) Update(ctx context.Context, mutate func(*Document) error) (*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if mutate == nil {
		return nil, errors.New("nodestate: Update: the mutator must not be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	res, err := s.loadLocked()
	if err != nil {
		return nil, err
	}
	if res.ForeignWrite && s.strictOwnership {
		return nil, fmt.Errorf("%w: %s was last written by %q at revision %d",
			ErrForeignWrite, s.Path(s.self), res.Document.WriterID, res.Document.Revision)
	}

	doc := res.Document
	if err := mutate(doc); err != nil {
		if errors.Is(err, ErrNoChange) {
			return doc, nil
		}
		return nil, err
	}

	s.retention.Apply(doc, s.clock.Now())
	if err := s.writeLocked(doc); err != nil {
		if errors.Is(err, ErrDurabilityUnconfirmed) {
			// Written and visible. Hand back the document as well as the
			// error so the caller can act on what it recorded.
			return doc, err
		}
		return nil, err
	}
	return doc, nil
}

// --- reading ---

// readDocument reads and validates one state file. Every failure it can
// return is classifiable with errors.Is: fs.ErrNotExist, ErrSymlink,
// ErrNotRegular, ErrInsecurePermissions, ErrInsecureOwner, ErrTooLarge,
// ErrCorrupt or ErrSchemaUnsupported.
func (s *Store) readDocument(path string, owner Component) (*Document, error) {
	stat := s.fs.Lstat
	fromLstat := true
	if s.security.AllowSymlinks {
		stat, fromLstat = s.fs.Stat, false
	}

	fi, err := stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, err // caller distinguishes absence from damage
		}
		return nil, pathErrorf("stat", path, err)
	}
	if err := s.security.checkFile(path, fi, fromLstat); err != nil {
		return nil, err
	}
	if fi.Size() > int64(s.maxReadBytes) {
		return nil, pathErrorf("read", path,
			fmt.Errorf("%w: %d bytes, over the %d byte read limit", ErrTooLarge, fi.Size(), s.maxReadBytes))
	}

	f, err := s.fs.Open(path, OpenOptions{FollowSymlinks: s.security.AllowSymlinks})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		// A refusal to open is most often O_NOFOLLOW rejecting a link that
		// appeared between the stat and the open. Report it as such rather
		// than as a generic I/O error: it is the TOCTOU case the flag exists
		// for, and a caller seeing it should look at who else writes here.
		if isSymlinkOpenRefusal(err) {
			return nil, pathErrorf("open", path, fmt.Errorf("%w: it was replaced by a link after it was checked", ErrSymlink))
		}
		return nil, pathErrorf("open", path, err)
	}
	defer func() { _ = f.Close() }()

	// Re-check on the descriptor. Everything learned from the path above
	// describes whatever was there a moment ago; this describes what is
	// actually open.
	opened, err := f.Stat()
	if err != nil {
		return nil, pathErrorf("fstat", path, err)
	}
	if err := s.security.checkFile(path, opened, false); err != nil {
		return nil, err
	}

	// The size check above used what stat reported; this one uses what was
	// actually there. A file that grows in between is refused, not truncated.
	data, err := readAtMost(f, s.maxReadBytes)
	if err != nil {
		return nil, pathErrorf("read", path, err)
	}

	return parseDocument(path, data, owner)
}

// readAtMost reads up to limit bytes and reports ErrTooLarge if there are
// more, rather than silently truncating or allocating without bound.
func readAtMost(r io.Reader, limit int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("%w: more than the %d byte read limit", ErrTooLarge, limit)
	}
	return data, nil
}

func parseDocument(path string, data []byte, owner Component) (*Document, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, pathErrorf("parse", path, fmt.Errorf("%w: the file is empty", ErrCorrupt))
	}

	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, pathErrorf("parse", path, fmt.Errorf("%w: %v", ErrCorrupt, err))
	}

	if doc.SchemaVersion < MinSupportedSchema || doc.SchemaVersion > Schema {
		return nil, pathErrorf("parse", path, &SchemaVersionError{
			Found: doc.SchemaVersion, Min: MinSupportedSchema, Max: Schema,
		})
	}

	// A file whose contents claim a different owner is not this component's
	// state, whatever the name says. An older writer that stamped nothing is
	// accepted: the name is then the only claim available.
	if doc.Agent.Component != "" && doc.Agent.Component != owner {
		return nil, pathErrorf("parse", path, fmt.Errorf(
			"%w: %s contains a document owned by %q", ErrCorrupt, filepath.Base(path), doc.Agent.Component))
	}

	return &doc, nil
}

// --- writing ---

// atomicReplace writes data to path by the only sequence that is safe for a
// concurrent reader and for a power loss:
//
//	create a temporary file in the same directory
//	write
//	fsync the file
//	close
//	rename over the target
//	fsync the directory
//
// Each step matters. The temporary file must share the directory, or the
// rename is a cross-device copy and not atomic. The file sync must precede
// the rename, or a crash can leave the directory pointing at a file with no
// contents. The directory sync must follow it, or the rename itself can be
// lost. On any failure the temporary file is removed and the previous version
// of the target is untouched.
func (s *Store) atomicReplace(path string, data []byte) error {
	dir := filepath.Dir(path)
	prefix := temporaryPrefix(filepath.Base(path))

	f, err := s.fs.CreateTemp(dir, prefix, s.security.FileMode)
	if err != nil {
		return pathErrorf("createtemp", dir, err)
	}

	tmp := f.Name()
	closed := false
	committed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		if !committed {
			// Best effort. If this fails the file is still a temporary name
			// that no reader looks for, and the next successful write will
			// not collide with it.
			_ = s.fs.Remove(tmp)
		}
	}()

	if _, err := f.Write(data); err != nil {
		return pathErrorf("write", tmp, err)
	}
	if err := f.Sync(); err != nil {
		return pathErrorf("fsync", tmp, err)
	}
	closed = true
	if err := f.Close(); err != nil {
		return pathErrorf("close", tmp, err)
	}
	if err := s.fs.Rename(tmp, path); err != nil {
		return pathErrorf("rename", tmp, err)
	}
	committed = true

	if err := s.fs.SyncDir(dir); err != nil {
		// Past the point of no return. The rename has happened, every reader
		// already sees the new version, and there is nothing to roll back
		// to. All that is unproven is whether the directory entry survives a
		// power loss.
		//
		// This has to be a distinguishable outcome rather than a plain
		// error, because the two readings lead to opposite actions. A caller
		// that reads "the write failed" from here will conclude the record
		// it was about to act on is not on disk — and for an agent that
		// records a pending fence before issuing it, that is exactly
		// backwards.
		return fmt.Errorf("%w: %s was replaced but the directory entry could not be flushed: %w",
			ErrDurabilityUnconfirmed, path, pathErrorf("fsync", dir, err))
	}
	return nil
}

// --- quarantine ---

// quarantine moves an unusable file aside, preserving its bytes exactly, and
// returns the new path.
//
// Preserving the bytes is the point. The file is the only record of what the
// agent believed before it broke, and the shape of the damage — truncated at
// a block boundary, zero-filled, half of an older version — is what tells an
// operator whether they are looking at a crash, a full disk or a filesystem
// problem.
//
// Only the owner of a file ever calls this.
func (s *Store) quarantine(path string) (string, error) {
	stamp := s.clock.Now().UTC().Format(quarantineTimeLayout)
	unique, err := randomSuffix()
	if err != nil {
		return "", err
	}
	dest := fmt.Sprintf("%s.%s-%s%s", path, stamp, unique[:8], quarantineSuffix)

	if err := s.fs.Rename(path, dest); err != nil {
		return "", pathErrorf("rename", path, err)
	}
	_, _ = s.sweep()
	return dest, nil
}

// --- housekeeping ---
//
// Everything in this section removes files. None of it ever writes the live
// document, which is replaced only by atomicReplace. That is what keeps the
// guarantee intact under a crash: the worst a process killed in the middle of
// housekeeping can leave behind is a file the next sweep reclaims, and a
// reader at any instant still sees a whole state file or none.

// CleanupReport says what a housekeeping pass removed. Log it, or feed the
// counters to a metric: a node steadily accumulating orphans is a node whose
// agent keeps dying mid-write.
type CleanupReport struct {
	// TemporaryFilesRemoved counts abandoned temporary files reclaimed. Each
	// one is a write this component started and did not finish, so a non-zero
	// count means the agent was killed between creating the file and renaming
	// it into place.
	TemporaryFilesRemoved int
	// QuarantineFilesRemoved counts quarantined copies aged or pruned out.
	QuarantineFilesRemoved int
	// BytesReclaimed is the total size of everything removed, as far as the
	// filesystem reported it.
	BytesReclaimed int64
}

// Cleanup reclaims this component's abandoned files from the state directory
// and returns what it removed.
//
// Two kinds accumulate. A temporary file is left behind whenever a process is
// killed between creating it and renaming it into place — a panic, an
// OOM kill, or the watchdog reset this product exists to cause. A quarantined
// copy is left behind deliberately, as evidence, and is kept to a fixed
// number and age.
//
// It is called for you: once per Store on the first write, and after every
// quarantine. Call it directly from a periodic janitor on a long-lived agent,
// or at start-up to get the counts into a log line.
//
// Only this component's files are touched, by the same ownership rule that
// governs writing. A temporary file younger than the orphan grace period is
// left alone, because it may belong to a write still in flight.
func (s *Store) Cleanup(ctx context.Context) (*CleanupReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sweep()
}

// sweep performs one housekeeping pass. The caller holds s.mu.
//
// Failures are reported but not fatal to the caller that triggered the sweep:
// being unable to tidy up is not a reason to fail a read or a write.
func (s *Store) sweep() (*CleanupReport, error) {
	s.swept = true
	report := &CleanupReport{}

	entries, err := s.fs.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return report, nil
		}
		return report, pathErrorf("readdir", s.dir, err)
	}

	base := filepath.Base(s.Path(s.self))
	tempPrefix := temporaryPrefix(base)
	quarantinePrefix := base + "."
	now := s.clock.Now()

	type candidate struct {
		name string
		size int64
	}
	var quarantined []candidate

	remove := func(name string, size int64, counter *int) {
		if err := s.fs.Remove(filepath.Join(s.dir, name)); err == nil {
			*counter++
			report.BytesReclaimed += size
		}
	}

	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, tempPrefix):
			info, infoErr := e.Info()
			if infoErr != nil {
				continue // it went away underneath us, which is the outcome we wanted
			}
			// A write that has been in flight this long is not in flight. The
			// grace period is what separates "a process died holding this"
			// from "a process is writing this right now", and getting it
			// wrong in the other direction would delete a live write.
			if now.Sub(info.ModTime()) < s.orphanGrace {
				continue
			}
			remove(name, info.Size(), &report.TemporaryFilesRemoved)

		case strings.HasPrefix(name, quarantinePrefix) && strings.HasSuffix(name, quarantineSuffix):
			var size int64
			if info, infoErr := e.Info(); infoErr == nil {
				if s.quarantineMaxAge > 0 && now.Sub(info.ModTime()) > s.quarantineMaxAge {
					remove(name, info.Size(), &report.QuarantineFilesRemoved)
					continue
				}
				size = info.Size()
			}
			quarantined = append(quarantined, candidate{name: name, size: size})
		}
	}

	// Names sort chronologically because the timestamp embedded in them is
	// fixed-width, so the oldest are the ones to drop.
	if len(quarantined) > s.quarantineRetention {
		sort.Slice(quarantined, func(i, j int) bool { return quarantined[i].name < quarantined[j].name })
		for _, c := range quarantined[:len(quarantined)-s.quarantineRetention] {
			remove(c.name, c.size, &report.QuarantineFilesRemoved)
		}
	}

	return report, nil
}

// temporaryPrefix returns the name prefix atomicReplace gives the temporary
// file it writes for base. The leading dot keeps it out of a casual listing
// and, more usefully, out of the quarantine prefix's way.
func temporaryPrefix(base string) string { return "." + base + ".tmp-" }
