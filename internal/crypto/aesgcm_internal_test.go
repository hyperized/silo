package crypto

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"
)

func newTestCipher(t *testing.T) *Cipher {
	t.Helper()
	key := make([]byte, ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	c, err := NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	return c
}

func TestNewCipher_WrongKeyLength(t *testing.T) {
	for _, n := range []int{0, 16, 24, 31, 33, 64} {
		_, err := NewCipher(make([]byte, n))
		if err == nil {
			t.Errorf("NewCipher(len=%d): expected error", n)
			continue
		}
		if !strings.Contains(err.Error(), "32 bytes") || !strings.Contains(err.Error(), "openssl rand") {
			t.Errorf("NewCipher(len=%d) error should name the length and the fix command, got %v", n, err)
		}
	}
}

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	c := newTestCipher(t)
	cases := []struct {
		name      string
		plaintext []byte
	}{
		{"empty", nil},
		{"small", []byte("hello, silo")},
		{"binary", []byte{0x00, 0xff, 0x7f, 0x80, 0x00}},
		{"4 KiB", bytes.Repeat([]byte{'A'}, 4096)},
		{"4 MiB", bytes.Repeat([]byte{'Z'}, 4*1024*1024)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, err := c.EncryptChunk(tc.plaintext)
			if err != nil {
				t.Fatalf("EncryptChunk: %v", err)
			}
			if len(env) != len(tc.plaintext)+OverheadBytes {
				t.Errorf("envelope size: got %d, want %d (overhead = %d)", len(env), len(tc.plaintext)+OverheadBytes, OverheadBytes)
			}
			out, err := c.DecryptChunk(env)
			if err != nil {
				t.Fatalf("DecryptChunk: %v", err)
			}
			if !bytes.Equal(out, tc.plaintext) {
				t.Errorf("round-trip mismatch (lengths %d vs %d)", len(out), len(tc.plaintext))
			}
		})
	}
}

func TestEncrypt_NondeterministicCiphertext(t *testing.T) {
	c := newTestCipher(t)
	plaintext := []byte("same plaintext twice")
	a, err := c.EncryptChunk(plaintext)
	if err != nil {
		t.Fatalf("EncryptChunk a: %v", err)
	}
	b, err := c.EncryptChunk(plaintext)
	if err != nil {
		t.Fatalf("EncryptChunk b: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two encryptions of the same plaintext produced identical ciphertext; this would allow correlation")
	}
}

func TestDecrypt_DetectsTamper(t *testing.T) {
	c := newTestCipher(t)
	env, err := c.EncryptChunk([]byte("important data"))
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}

	cases := []struct {
		name  string
		mut   func([]byte) []byte
		want  error
	}{
		{
			name: "flip a ciphertext bit",
			mut:  func(b []byte) []byte { out := append([]byte(nil), b...); out[len(out)-1] ^= 0x01; return out },
			want: ErrTampered,
		},
		{
			name: "flip a wrapped-dek bit",
			mut:  func(b []byte) []byte { out := append([]byte(nil), b...); out[HeaderBytes-15] ^= 0x01; return out },
			want: ErrTampered,
		},
		{
			name: "truncate the tag",
			mut:  func(b []byte) []byte { return append([]byte(nil), b[:len(b)-1]...) },
			want: ErrTampered,
		},
		{
			name: "empty input",
			mut:  func(_ []byte) []byte { return nil },
			want: ErrTooShort,
		},
		{
			name: "header-sized but no payload tag",
			mut:  func(_ []byte) []byte { return make([]byte, HeaderBytes+tagBytes-1) },
			want: ErrTooShort,
		},
		{
			name: "wrong magic",
			mut:  func(b []byte) []byte { out := append([]byte(nil), b...); out[0] = 'X'; return out },
			want: ErrBadMagic,
		},
		{
			name: "unsupported version",
			mut:  func(b []byte) []byte { out := append([]byte(nil), b...); out[magicSize] = 99; return out },
			want: ErrBadVersion,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.DecryptChunk(tc.mut(env))
			if !errors.Is(err, tc.want) {
				t.Errorf("got error %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecrypt_WrongClusterKeyFails(t *testing.T) {
	keyA := make([]byte, ClusterKeyBytes)
	keyB := make([]byte, ClusterKeyBytes)
	if _, err := rand.Read(keyA); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := rand.Read(keyB); err != nil {
		t.Fatalf("rand: %v", err)
	}
	cipherA, _ := NewCipher(keyA)
	cipherB, _ := NewCipher(keyB)

	env, err := cipherA.EncryptChunk([]byte("encrypted under key A"))
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}
	if _, err := cipherB.DecryptChunk(env); !errors.Is(err, ErrTampered) {
		t.Errorf("decrypt with wrong key: got %v, want ErrTampered", err)
	}
}

// failingReader returns a fixed error from Read; used to simulate exhausted
// entropy in tests of the random-source error paths.
type failingReader struct{ err error }

func (r failingReader) Read(_ []byte) (int, error) { return 0, r.err }

// budgetReader serves zeros until `remaining` bytes have been delivered,
// then returns io.ErrUnexpectedEOF. Used to fail the Nth rand.Read inside
// EncryptChunk.
type budgetReader struct{ remaining int }

func (r *budgetReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	r.remaining -= n
	return n, nil
}

func TestEncrypt_RandReadErrors(t *testing.T) {
	// Build the cipher first — newTestCipher itself reads random bytes
	// to mint a key, so we must not swap rand.Reader yet.
	c := newTestCipher(t)

	prev := rand.Reader
	t.Cleanup(func() { rand.Reader = prev })
	rand.Reader = failingReader{err: io.ErrUnexpectedEOF}

	_, err := c.EncryptChunk([]byte("x"))
	if err == nil {
		t.Fatal("expected encryption to fail with a broken rand source")
	}
	if !strings.Contains(err.Error(), "random") || !strings.Contains(err.Error(), "entropy") {
		t.Errorf("error should mention randomness/entropy, got: %v", err)
	}
}

func TestEncrypt_RandFailsAtEachStage(t *testing.T) {
	// EncryptChunk reads random three times: data key, wrap nonce, chunk
	// nonce. Each failure should produce an error that names the purpose
	// so an operator reading the log can tell which read failed.
	c := newTestCipher(t)
	prev := rand.Reader
	t.Cleanup(func() { rand.Reader = prev })

	cases := []struct {
		name   string
		budget int
		want   string
	}{
		{"data key fails first", 0, "per-chunk data key"},
		{"wrap nonce fails after data key", dataKeyBytes, "wrap nonce"},
		{"chunk nonce fails after wrap nonce", dataKeyBytes + nonceBytes, "chunk nonce"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rand.Reader = &budgetReader{remaining: tc.budget}
			_, err := c.EncryptChunk([]byte("x"))
			if err == nil {
				t.Fatal("expected an error when the random source runs out")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should mention %q, got: %v", tc.want, err)
			}
			if !strings.Contains(err.Error(), "entropy") {
				t.Errorf("error should mention entropy, got: %v", err)
			}
		})
	}
}

func TestErrSentinels_AreInstructive(t *testing.T) {
	// Each user-facing sentinel must read as instruction, per
	// silo-errors-are-instructions.
	cases := []struct {
		err  error
		want []string
	}{
		{ErrTampered, []string{"authentication", "modified", "restore"}},
		{ErrBadMagic, []string{"silo chunk envelope", "recreate"}},
		{ErrBadVersion, []string{"version", "upgrade"}},
		{ErrTooShort, []string{"truncated", "restore"}},
	}
	for _, tc := range cases {
		msg := tc.err.Error()
		for _, sub := range tc.want {
			if !strings.Contains(msg, sub) {
				t.Errorf("%v: missing %q in message %q", tc.err, sub, msg)
			}
		}
	}
}
