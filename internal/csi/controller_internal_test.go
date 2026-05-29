package csi

import (
	"testing"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
)

func TestParseByteSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		bad  bool
	}{
		{"", 0, false},
		{"4096", 4096, false},
		{"64K", 64 << 10, false},
		{"64Ki", 64 << 10, false},
		{"4Mi", 4 << 20, false},
		{"1g", 1 << 30, false},
		{"2Ti", 2 << 40, false},
		{"nope", 0, true},
		{"-5", 0, true},
		{"Mi", 0, true},
		{"99999999999999T", 0, true}, // overflows int64
	}
	for _, tc := range cases {
		got, err := parseByteSize(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("parseByteSize(%q) = %d, want error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseByteSize(%q) = (%d, %v), want (%d, nil)", tc.in, got, err, tc.want)
		}
	}
}

func TestCapacityBytes(t *testing.T) {
	if _, err := capacityBytes(nil); err == nil {
		t.Error("nil capacity range should error")
	}
	if got, err := capacityBytes(&csiv1.CapacityRange{RequiredBytes: 1 << 20}); err != nil || got != 1<<20 {
		t.Errorf("required = (%d, %v), want (1Mi, nil)", got, err)
	}
	if got, err := capacityBytes(&csiv1.CapacityRange{LimitBytes: 2 << 20}); err != nil || got != 2<<20 {
		t.Errorf("limit fallback = (%d, %v), want (2Mi, nil)", got, err)
	}
}

func TestAccessModeSupported(t *testing.T) {
	ok := []csiv1.VolumeCapability_AccessMode_Mode{
		csiv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csiv1.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csiv1.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
		csiv1.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
	}
	for _, m := range ok {
		if !accessModeSupported(m) {
			t.Errorf("mode %s should be supported", m)
		}
	}
	bad := []csiv1.VolumeCapability_AccessMode_Mode{
		csiv1.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		csiv1.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csiv1.VolumeCapability_AccessMode_UNKNOWN,
	}
	for _, m := range bad {
		if accessModeSupported(m) {
			t.Errorf("mode %s should not be supported", m)
		}
	}
}
