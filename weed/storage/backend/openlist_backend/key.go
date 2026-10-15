package openlist_backend

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// The .vif key carries the eight-byte volume superblock. Mounting thousands of
// cold volumes after restart must not download thousands of packs just to read
// their headers. This is format metadata, never a file body or encryption key.
func parseKey(key string) (digest string, header []byte, err error) {
	parts := strings.Split(key, ".")
	if len(parts) != 3 || parts[0] != "v1" || len(parts[1]) != 64 || len(parts[2]) != 16 || strings.ToLower(key) != key {
		return "", nil, fmt.Errorf("invalid OpenList volume key")
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return "", nil, err
	}
	header, err = hex.DecodeString(parts[2])
	return parts[1], header, err
}
