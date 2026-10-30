package nodestate_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/medik8s/common/pkg/nodestate"
)

// TestLoadPeer_UnreadableIsNotAbsent is the distinction ReadPeer cannot make.
//
// A peer file this process may not read looks exactly like a peer that has
// never run, and the consequence is not local: that peer's reboots drop out
// of every budget merged on this node, so a reboot loop it carried out is
// invisible to the brake. The cause is always a deployment fault — an agent
// not running as the owning user, a user namespace mapping, an SELinux
// denial — and it will not fix itself.
func TestLoadPeer_UnreadableIsNotAbsent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; file permissions do not bite")
	}
	t.Parallel()

	s, dir := newStore(t, nodestate.ComponentSBR)
	ctx := context.Background()

	peer := filepath.Join(dir, nodestate.SNRFile)
	mustWrite(t, peer, `{"schemaVersion":1,"agent":{"component":"snr"},"reboots":[]}`)
	if err := os.Chmod(peer, 0o000); err != nil {
		t.Fatal(err)
	}

	res, err := s.LoadPeer(ctx, nodestate.ComponentSNR)
	if err != nil {
		t.Fatalf("LoadPeer: %v", err)
	}
	if res.Status != nodestate.PeerUnreadable {
		t.Fatalf("Status = %v, want PeerUnreadable", res.Status)
	}
	if !res.Configured() {
		t.Error("Configured() = false; this is a deployment fault the caller must surface")
	}
	if !errors.Is(res.Cause, fs.ErrPermission) {
		t.Errorf("Cause = %v, want it to wrap fs.ErrPermission", res.Cause)
	}
	if res.Document != nil {
		t.Error("a document was returned for an unreadable file")
	}

	// ReadPeer keeps its tolerant contract regardless.
	doc, err := s.ReadPeer(ctx, nodestate.ComponentSNR)
	if doc != nil || err != nil {
		t.Errorf("ReadPeer = %v, %v; want nil, nil", doc, err)
	}
}

func TestLoadPeer_Statuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prepare    func(t *testing.T, dir, path string)
		want       nodestate.PeerStatus
		configured bool
	}{
		{
			name:    "absent",
			prepare: func(t *testing.T, dir, path string) {},
			want:    nodestate.PeerAbsent,
		},
		{
			name:    "healthy",
			prepare: func(t *testing.T, dir, path string) { mustWrite(t, path, healthyPeer) },
			want:    nodestate.PeerOK,
		},
		{
			name:    "truncated",
			prepare: func(t *testing.T, dir, path string) { mustWrite(t, path, `{"schemaVersion":1,"nod`) },
			want:    nodestate.PeerUnusable,
		},
		{
			name:    "empty",
			prepare: func(t *testing.T, dir, path string) { mustWrite(t, path, "") },
			want:    nodestate.PeerUnusable,
		},
		{
			name: "claims another owner",
			prepare: func(t *testing.T, dir, path string) {
				mustWrite(t, path, `{"schemaVersion":1,"agent":{"component":"sbr"}}`)
			},
			want: nodestate.PeerUnusable,
		},
		{
			name:    "newer schema",
			prepare: func(t *testing.T, dir, path string) { mustWrite(t, path, `{"schemaVersion":99}`) },
			want:    nodestate.PeerUnsupportedSchema,
		},
		{
			name: "symlink",
			prepare: func(t *testing.T, dir, path string) {
				target := filepath.Join(dir, "target.json")
				mustWrite(t, target, healthyPeer)
				if err := os.Symlink(target, path); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			},
			want:       nodestate.PeerUnreadable,
			configured: true,
		},
		{
			name: "world writable",
			prepare: func(t *testing.T, dir, path string) {
				mustWrite(t, path, healthyPeer)
				if err := os.Chmod(path, 0o666); err != nil {
					t.Fatal(err)
				}
			},
			want:       nodestate.PeerUnreadable,
			configured: true,
		},
		{
			name: "oversized",
			prepare: func(t *testing.T, dir, path string) {
				mustWrite(t, path, strings.Repeat("x", 32<<10))
			},
			want:       nodestate.PeerUnreadable,
			configured: true,
		},
		{
			name: "directory",
			prepare: func(t *testing.T, dir, path string) {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			},
			want:       nodestate.PeerUnreadable,
			configured: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, dir := newStore(t, nodestate.ComponentSBR, nodestate.WithMaxReadBytes(8<<10))
			tt.prepare(t, dir, filepath.Join(dir, nodestate.SNRFile))

			res, err := s.LoadPeer(context.Background(), nodestate.ComponentSNR)
			if err != nil {
				t.Fatalf("LoadPeer: %v", err)
			}
			if res.Status != tt.want {
				t.Errorf("Status = %v, want %v (cause: %v)", res.Status, tt.want, res.Cause)
			}
			if res.Configured() != tt.configured {
				t.Errorf("Configured() = %v, want %v", res.Configured(), tt.configured)
			}
			if tt.want == nodestate.PeerOK && res.Document == nil {
				t.Error("no document for a healthy peer")
			}
			if tt.want != nodestate.PeerOK && res.Document != nil {
				t.Error("a document was returned for an unusable peer")
			}
			if tt.want != nodestate.PeerOK && tt.want != nodestate.PeerAbsent && res.Cause == nil {
				t.Error("Cause is nil; the caller has nothing to report")
			}

			// Whatever the status, nothing was written: a peer's file is not
			// ours to repair.
			if matches, _ := filepath.Glob(filepath.Join(dir, "*.corrupt")); len(matches) != 0 {
				t.Errorf("LoadPeer quarantined the peer's file: %v", matches)
			}
		})
	}
}

func TestLoadPeer_RejectsAnInvalidComponent(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	if _, err := s.LoadPeer(context.Background(), nodestate.Component("../escape")); !errors.Is(err, nodestate.ErrInvalidComponent) {
		t.Fatalf("error = %v, want ErrInvalidComponent", err)
	}
}

func TestLoadPeer_RespectsACancelledContext(t *testing.T) {
	t.Parallel()

	s, _ := newStore(t, nodestate.ComponentSBR)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.LoadPeer(ctx, nodestate.ComponentSNR); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestPeerStatusString(t *testing.T) {
	t.Parallel()

	for _, st := range []nodestate.PeerStatus{
		nodestate.PeerOK, nodestate.PeerAbsent, nodestate.PeerUnusable,
		nodestate.PeerUnreadable, nodestate.PeerUnsupportedSchema,
	} {
		if s := st.String(); s == "" || strings.HasPrefix(s, "PeerStatus(") {
			t.Errorf("PeerStatus(%d).String() = %q", int(st), s)
		}
	}
	if got := nodestate.PeerStatus(42).String(); !strings.Contains(got, "42") {
		t.Errorf("unknown status string = %q", got)
	}
}

const healthyPeer = `{"schemaVersion":1,"node":"worker-0","agent":{"component":"snr"},"reboots":[]}`
