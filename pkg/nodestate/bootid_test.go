package nodestate_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestBootID covers the identifier the start-up classification depends on: the
// same boot id means the agent restarted, a different one means the node did.
func TestBootID(t *testing.T) {
	t.Parallel()

	t.Run("parses the kernel file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "boot_id")
		mustWrite(t, path, "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8\n")

		got, err := nodestate.ReadBootIDFrom(path)
		if err != nil {
			t.Fatalf("ReadBootIDFrom: %v", err)
		}
		if got != "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8" {
			t.Errorf("boot id = %q", got)
		}
	})

	t.Run("tolerates surrounding whitespace", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "boot_id")
		mustWrite(t, path, "  8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8  \r\n")

		got, err := nodestate.ReadBootIDFrom(path)
		if err != nil {
			t.Fatalf("ReadBootIDFrom: %v", err)
		}
		if got != "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8" {
			t.Errorf("boot id = %q", got)
		}
	})

	t.Run("rejects malformed content", func(t *testing.T) {
		t.Parallel()

		cases := map[string]string{
			"empty":           "",
			"whitespace":      "   \n",
			"not a uuid":      "hello world",
			"truncated uuid":  "8f1a2b3c-4d5e-6f70-8192",
			"bad hex":         "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7fZ",
			"multiline":       "8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8\nextra\n",
			"leading garbage": "x8f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8",
		}
		for name, content := range cases {
			name, content := name, content
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				path := filepath.Join(t.TempDir(), "boot_id")
				mustWrite(t, path, content)

				got, err := nodestate.ReadBootIDFrom(path)
				if !errors.Is(err, nodestate.ErrBootIDUnavailable) {
					t.Fatalf("error = %v, want ErrBootIDUnavailable", err)
				}
				if got != "" {
					t.Errorf("boot id = %q, want empty on error", got)
				}
			})
		}
	})

	t.Run("a missing file is a typed error, not a panic", func(t *testing.T) {
		t.Parallel()

		got, err := nodestate.ReadBootIDFrom(filepath.Join(t.TempDir(), "absent"))
		if !errors.Is(err, nodestate.ErrBootIDUnavailable) {
			t.Fatalf("error = %v, want ErrBootIDUnavailable", err)
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("error = %v, want it to wrap os.ErrNotExist so a caller can tell why", err)
		}
		if got != "" {
			t.Errorf("boot id = %q", got)
		}
	})

	t.Run("an oversized file is refused", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "boot_id")
		mustWrite(t, path, strings.Repeat("a", 1<<20))

		if _, err := nodestate.ReadBootIDFrom(path); !errors.Is(err, nodestate.ErrBootIDUnavailable) {
			t.Fatalf("error = %v, want ErrBootIDUnavailable", err)
		}
	})

	t.Run("reads the real kernel file on linux", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("the boot id file is Linux-specific")
		}
		if _, err := os.Stat(nodestate.BootIDPath); err != nil {
			t.Skipf("%s unavailable: %v", nodestate.BootIDPath, err)
		}
		t.Parallel()

		got, err := nodestate.ReadBootID()
		if err != nil {
			t.Fatalf("ReadBootID: %v", err)
		}
		if len(got) != 36 {
			t.Errorf("boot id = %q, want a 36 character UUID", got)
		}

		again, err := nodestate.ReadBootID()
		if err != nil || again != got {
			t.Errorf("ReadBootID is not stable within a boot: %q then %q (%v)", got, again, err)
		}
	})
}

func TestBootIDPathIsTheKernelFile(t *testing.T) {
	t.Parallel()

	if nodestate.BootIDPath != "/proc/sys/kernel/random/boot_id" {
		t.Errorf("BootIDPath = %q", nodestate.BootIDPath)
	}
}
