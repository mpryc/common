package nodestate_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestDirectoryPermissions covers concern D1s: the state directory is a
// forgeable control surface unless it is private to root.
func TestDirectoryPermissions(t *testing.T) {
	t.Parallel()

	t.Run("created 0700 including parents", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		dir := filepath.Join(root, "var", "lib", "medik8s", "state")
		s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.EnsureDirectory(context.Background()); err != nil {
			t.Fatalf("EnsureDirectory: %v", err)
		}

		for p := dir; p != root; p = filepath.Dir(p) {
			fi, statErr := os.Stat(p)
			if statErr != nil {
				t.Fatalf("stat %s: %v", p, statErr)
			}
			if got := fi.Mode().Perm(); got != 0o700 {
				t.Errorf("%s mode = %#o, want 0700", p, got)
			}
		}
	})

	t.Run("pre-existing 0777 directory is a fault", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "state")
		if err := os.Mkdir(dir, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o777); err != nil { // defeat umask
			t.Fatal(err)
		}

		s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
		if err != nil {
			t.Fatal(err)
		}

		err = s.EnsureDirectory(context.Background())
		if !errors.Is(err, nodestate.ErrInsecurePermissions) {
			t.Fatalf("EnsureDirectory error = %v, want ErrInsecurePermissions", err)
		}
		// The library must not silently repair it: an unexpected mode means
		// something else manages this path, and that is the caller's problem.
		fi, _ := os.Stat(dir)
		if got := fi.Mode().Perm(); got != 0o777 {
			t.Errorf("mode was changed to %#o; the library must report, not repair", got)
		}

		// Every write path must refuse too, not just the explicit check.
		if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); !errors.Is(err, nodestate.ErrInsecurePermissions) {
			t.Errorf("Write error = %v, want ErrInsecurePermissions", err)
		}
	})

	t.Run("group writable directory is a fault", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "state")
		if err := os.Mkdir(dir, 0o770); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o770); err != nil {
			t.Fatal(err)
		}

		s, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
		if err := s.EnsureDirectory(context.Background()); !errors.Is(err, nodestate.ErrInsecurePermissions) {
			t.Fatalf("error = %v, want ErrInsecurePermissions", err)
		}
	})

	t.Run("a relaxed policy can accept group access", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "state")
		if err := os.Mkdir(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o750); err != nil {
			t.Fatal(err)
		}

		policy := nodestate.DefaultSecurityPolicy()
		policy.ForbiddenDirModeBits = 0o007 // allow group, still refuse "other"
		s, err := nodestate.New(nodestate.ComponentSBR,
			nodestate.WithDir(dir), nodestate.WithSecurityPolicy(policy))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.EnsureDirectory(context.Background()); err != nil {
			t.Fatalf("EnsureDirectory: %v", err)
		}
	})

	t.Run("a symlinked state directory is refused", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		real := filepath.Join(root, "real")
		link := filepath.Join(root, "state")
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(real, link); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}

		s, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(link))
		if err := s.EnsureDirectory(context.Background()); !errors.Is(err, nodestate.ErrSymlink) {
			t.Fatalf("error = %v, want ErrSymlink", err)
		}
	})

	t.Run("a regular file in place of the directory is refused", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "state")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}

		s, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(path))
		if err := s.EnsureDirectory(context.Background()); !errors.Is(err, nodestate.ErrNotDirectory) {
			t.Fatalf("error = %v, want ErrNotDirectory", err)
		}
	})

	t.Run("a directory owned by someone else is refused", func(t *testing.T) {
		if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
			t.Skip("ownership checks are Unix-specific")
		}
		if os.Geteuid() != 0 {
			t.Skip("needs root to create a directory owned by another user")
		}
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "state")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(dir, 65534, 65534); err != nil { // nobody
			t.Skipf("chown: %v", err)
		}

		s, _ := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
		if err := s.EnsureDirectory(context.Background()); !errors.Is(err, nodestate.ErrInsecureOwner) {
			t.Fatalf("error = %v, want ErrInsecureOwner", err)
		}
	})
}

// TestNoSymlinkFollow covers the second half of concern D1s. A symlink in the
// state directory is the cheapest way to turn a node-local write primitive into
// an arbitrary-file overwrite.
func TestNoSymlinkFollow(t *testing.T) {
	t.Parallel()

	t.Run("reading the own file through a symlink is refused", func(t *testing.T) {
		t.Parallel()

		s, dir := newStore(t, nodestate.ComponentSBR)
		target := filepath.Join(dir, "planted.json")
		mustWrite(t, target, `{"schemaVersion":1,"node":"attacker"}`)
		if err := os.Symlink(target, filepath.Join(dir, nodestate.SBRFile)); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}

		_, err := s.Load(context.Background())
		if !errors.Is(err, nodestate.ErrSymlink) {
			t.Fatalf("Load: %v", err)
		}

		// Both the symlink and its target remain untouched.
		if _, err := os.Lstat(target); err != nil {
			t.Errorf("the symlink target was disturbed: %v", err)
		}
		if got, _ := os.ReadFile(target); string(got) != `{"schemaVersion":1,"node":"attacker"}` {
			t.Errorf("the symlink target's contents changed to %q", got)
		}
	})

	t.Run("writing never follows a symlink", func(t *testing.T) {
		t.Parallel()

		s, dir := newStore(t, nodestate.ComponentSBR)
		outside := filepath.Join(t.TempDir(), "victim")
		mustWrite(t, outside, "do not overwrite me")
		if err := os.Symlink(outside, filepath.Join(dir, nodestate.SBRFile)); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}

		if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); !errors.Is(err, nodestate.ErrSymlink) {
			t.Fatalf("Write: %v", err)
		}

		got, err := os.ReadFile(outside)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "do not overwrite me" {
			t.Fatalf("the symlink target was overwritten: %q", got)
		}
		fi, err := os.Lstat(filepath.Join(dir, nodestate.SBRFile))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			t.Error("the refused write changed the symlink")
		}
	})

	t.Run("a policy may opt in to following symlinks", func(t *testing.T) {
		t.Parallel()

		dir := tempStateDir(t)
		target := filepath.Join(dir, "target.json")
		mustWrite(t, target, `{"schemaVersion":1,"node":"linked"}`)
		if err := os.Symlink(target, filepath.Join(dir, nodestate.SBRFile)); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}

		policy := nodestate.DefaultSecurityPolicy()
		policy.AllowSymlinks = true
		s, err := nodestate.New(nodestate.ComponentSBR,
			nodestate.WithDir(dir), nodestate.WithSecurityPolicy(policy))
		if err != nil {
			t.Fatal(err)
		}

		res, err := s.Load(context.Background())
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if res.Status != nodestate.StatusOK || res.Document.Node != "linked" {
			t.Fatalf("Status = %v, Node = %q", res.Status, res.Document.Node)
		}
	})
}

func TestLoad_RefusesANonRegularFile(t *testing.T) {
	s, dir := newStore(t, nodestate.ComponentSBR)
	if err := os.Mkdir(filepath.Join(dir, nodestate.SBRFile), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(context.Background()); !errors.Is(err, nodestate.ErrNotRegular) {
		t.Fatal(err)
	}
}

func TestLoad_WorldWritableOwnFileIsAFault(t *testing.T) {
	s, dir := newStore(t, nodestate.ComponentSBR)
	path := filepath.Join(dir, nodestate.SBRFile)
	mustWrite(t, path, `{"schemaVersion":1,"node":"worker-0"}`)
	if err := os.Chmod(path, 0666); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(context.Background()); !errors.Is(err, nodestate.ErrInsecurePermissions) {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("file was moved")
	}
}

func TestEnsureDirectory_IsIdempotent(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := s.EnsureDirectory(ctx); err != nil {
			t.Fatalf("EnsureDirectory call %d: %v", i, err)
		}
	}
}

// TestIntermediateSymlinkIsRefused covers the gap the per-file protections
// leave open.
//
// Lstat before open, O_NOFOLLOW, and the re-check on the descriptor all guard
// the final component. None of them notices a link one level up: with
// /var/lib/medik8s a link, every file this package writes lands wherever it
// points, and every check still passes because the leaf really is a regular
// root-owned file in a 0700 directory.
func TestIntermediateSymlinkIsRefused(t *testing.T) {
	t.Parallel()

	root := tempStateDir(t)
	elsewhere := filepath.Join(root, "elsewhere")
	if err := os.MkdirAll(filepath.Join(elsewhere, "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(root, "medik8s")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	dir := filepath.Join(root, "medik8s", "state")

	s, err := nodestate.New(nodestate.ComponentSBR, nodestate.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.EnsureDirectory(context.Background()); !errors.Is(err, nodestate.ErrSymlink) {
		t.Fatalf("EnsureDirectory error = %v, want ErrSymlink", err)
	}

	err = s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR))
	if !errors.Is(err, nodestate.ErrSymlink) {
		t.Fatalf("Write error = %v, want ErrSymlink", err)
	}
	if _, statErr := os.Stat(filepath.Join(elsewhere, "state", nodestate.SBRFile)); statErr == nil {
		t.Error("the document landed at the symlink target")
	}
}

func TestIntermediateSymlinkCanBeAllowed(t *testing.T) {
	t.Parallel()

	root := tempStateDir(t)
	elsewhere := filepath.Join(root, "elsewhere")
	if err := os.MkdirAll(filepath.Join(elsewhere, "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(root, "medik8s")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	policy := nodestate.DefaultSecurityPolicy()
	policy.AllowSymlinks = true
	s, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(filepath.Join(root, "medik8s", "state")),
		nodestate.WithSecurityPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Write(context.Background(), nodestate.NewDocument(nodestate.ComponentSBR)); err != nil {
		t.Fatalf("Write with AllowSymlinks: %v", err)
	}
}

// TestResolvedPathCheckAcceptsAnOrdinaryDirectory guards against the check
// refusing every deployment, which is the way this sort of guard usually
// fails.
func TestResolvedPathCheckAcceptsAnOrdinaryDirectory(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	if err := s.EnsureDirectory(context.Background()); err != nil {
		t.Fatalf("EnsureDirectory on a plain directory: %v", err)
	}
	// Including a directory this call has to create itself.
	nested, err := nodestate.New(nodestate.ComponentSBR,
		nodestate.WithDir(filepath.Join(s.Directory(), "a", "b", "state")))
	if err != nil {
		t.Fatal(err)
	}
	if err := nested.EnsureDirectory(context.Background()); err != nil {
		t.Fatalf("EnsureDirectory on a path it created: %v", err)
	}
}
