package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestObjectKey(t *testing.T) {
	cases := []struct{ name, prefix, obj, want string }{
		{"empty prefix", "", "chunks/a", "chunks/a"},
		{"with prefix", "p/q", "chunks/a", "p/q/chunks/a"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := objectKey(c.prefix, c.obj); got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestListKey(t *testing.T) {
	cases := []struct{ name, prefix, list, want string }{
		{"both empty", "", "", ""},
		{"empty target prefix", "", "chunks/", "chunks/"},
		{"empty list prefix", "p", "", "p/"},
		{"both set", "p", "chunks/", "p/chunks/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := listKey(c.prefix, c.list); got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestRelativeName(t *testing.T) {
	cases := []struct {
		name, prefix, key string
		want              string
		ok                bool
	}{
		{"empty prefix", "", "chunks/a", "chunks/a", true},
		{"empty prefix empty key", "", "", "", false},
		{"inside prefix", "p", "p/chunks/a", "chunks/a", true},
		{"outside prefix", "p", "q/chunks/a", "", false},
		{"sibling sharing the prefix text", "p", "px/a", "", false},
		{"bare prefix", "p", "p", "", false},
		{"prefix with slash only", "p", "p/", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := relativeName(c.prefix, c.key)
			// The returned name is only meaningful when ok is true.
			if ok != c.ok || (ok && got != c.want) {
				t.Errorf("got (%q, %v), want (%q, %v)", got, ok, c.want, c.ok)
			}
		})
	}
}

func TestValidatePrefix(t *testing.T) {
	cases := []struct {
		prefix  string
		wantErr bool
	}{
		{"", false},
		{"chunks/", false},
		{"chunks", false},
		{"/x", true},
		{"a/../b/", true},
	}
	for _, c := range cases {
		t.Run(c.prefix, func(t *testing.T) {
			err := validatePrefix(c.prefix)
			if (err != nil) != c.wantErr {
				t.Errorf("validatePrefix(%q) = %v, wantErr %v", c.prefix, err, c.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidName) {
				t.Errorf("error should wrap ErrInvalidName: %v", err)
			}
		})
	}
}

func TestS3NotFound(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"NoSuchKey type", &types.NoSuchKey{}, true},
		{"NotFound type", &types.NotFound{}, true},
		{"generic NoSuchKey", &smithy.GenericAPIError{Code: "NoSuchKey"}, true},
		{"generic NotFound", &smithy.GenericAPIError{Code: "NotFound"}, true},
		{"wrapped NoSuchKey type", fmt.Errorf("get: %w", &types.NoSuchKey{}), true},
		{"wrapped generic", fmt.Errorf("get: %w", &smithy.GenericAPIError{Code: "NoSuchKey"}), true},
		{"other code", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"plain error", errors.New("boom"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := s3NotFound(c.err); got != c.want {
				t.Errorf("s3NotFound = %v, want %v", got, c.want)
			}
		})
	}
}

func TestTargetNames(t *testing.T) {
	if got := newS3("b", "p", options{}).Name(); got != "s3://b/p" {
		t.Errorf("s3 = %q", got)
	}
	if got := newGCS("b", "p").Name(); got != "gs://b/p" {
		t.Errorf("gcs = %q", got)
	}
	if got := newAzure("acct", "c", "p").Name(); got != "az://acct/c/p" {
		t.Errorf("azure = %q", got)
	}
}

func TestS3Hint(t *testing.T) {
	plain := newS3("b", "", options{}).hint()
	if strings.Contains(plain, "SILO_BACKUP_S3_ENDPOINT") {
		t.Errorf("hint without endpoint mentions the endpoint variable: %q", plain)
	}
	custom := newS3("b", "", options{s3Endpoint: "https://minio:9000"}).hint()
	if !strings.Contains(custom, "SILO_BACKUP_S3_ENDPOINT") {
		t.Errorf("hint with endpoint should mention the endpoint variable: %q", custom)
	}
}

// The cloud targets must reject a bad name before building a client, so these
// calls never reach the network.
func TestCloudTargets_RejectInvalidNames(t *testing.T) {
	ctx := context.Background()
	targets := map[string]Target{
		"s3":    newS3("b", "p", options{}),
		"gcs":   newGCS("b", "p"),
		"azure": newAzure("acct", "c", "p"),
	}
	for label, tgt := range targets {
		t.Run(label, func(t *testing.T) {
			if err := tgt.Put(ctx, "../x", []byte("x")); !errors.Is(err, ErrInvalidName) {
				t.Errorf("Put = %v", err)
			}
			if _, err := tgt.Get(ctx, "/abs", 1); !errors.Is(err, ErrInvalidName) {
				t.Errorf("Get = %v", err)
			}
			if _, err := tgt.PutIfAbsent(ctx, "../x", []byte("x")); !errors.Is(err, ErrInvalidName) {
				t.Errorf("PutIfAbsent = %v", err)
			}
			if _, err := tgt.List(ctx, "a//b/"); !errors.Is(err, ErrInvalidName) {
				t.Errorf("List = %v", err)
			}
			if err := tgt.Delete(ctx, "a\\b"); !errors.Is(err, ErrInvalidName) {
				t.Errorf("Delete = %v", err)
			}
		})
	}
}

func TestS3PreconditionFailed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"precondition failed", &smithy.GenericAPIError{Code: "PreconditionFailed"}, true},
		{"wrapped", fmt.Errorf("put: %w", &smithy.GenericAPIError{Code: "PreconditionFailed"}), true},
		{"other code", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"plain error", errors.New("boom"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := s3PreconditionFailed(c.err); got != c.want {
				t.Errorf("s3PreconditionFailed = %v, want %v", got, c.want)
			}
		})
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

func TestReadCapped(t *testing.T) {
	cases := []struct {
		name    string
		r       io.Reader
		max     int64
		want    string
		wantErr error
		errText string
	}{
		{"under the limit", strings.NewReader("abc"), 5, "abc", nil, ""},
		{"exactly at the limit", strings.NewReader("abcde"), 5, "abcde", nil, ""},
		{"one byte over", strings.NewReader("abcdef"), 5, "", ErrTooLarge, ""},
		{"read error", failingReader{}, 5, "", nil, "interrupted"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readCapped(c.r, c.max, "obj", "tgt")
			switch {
			case c.wantErr != nil:
				if !errors.Is(err, c.wantErr) {
					t.Errorf("err = %v, want %v", err, c.wantErr)
				}
			case c.errText != "":
				if err == nil || !strings.Contains(err.Error(), c.errText) {
					t.Errorf("err = %v, want text %q", err, c.errText)
				}
			case err != nil || string(got) != c.want:
				t.Errorf("= (%q, %v), want %q", got, err, c.want)
			}
		})
	}
}
