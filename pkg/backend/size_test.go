package backend

import (
	"testing"
	"unsafe"
)

// Backend is copied into per-request filtered lists; keep it two words of
// interface plus one pointer.
func TestBackendStaysSmall(t *testing.T) {
	if s := unsafe.Sizeof(Backend{}); s > 24 {
		t.Fatalf("Backend is %d bytes; keep per-backend state behind the pointer", s)
	}
}
