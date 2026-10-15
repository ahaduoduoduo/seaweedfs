// Package vault provides a single-authority NAS object API on SeaweedFS needle
// packs and the OpenList cold tier. It does not need a separate filer/master.
package vault

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
)

const ChunkSize = 4 << 20

var spaceIdentifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

type SpaceConfig struct {
	Name             string `json:"name"`
	TokenFile        string `json:"token_file"`
	GatewayTokenFile string `json:"gateway_token_file"`
}
type Config struct {
	Listen                   string        `json:"listen"`
	PublicURL                string        `json:"public_url"`
	Dir                      string        `json:"dir"`
	Gateway                  string        `json:"gateway"`
	AdminTokenFile           string        `json:"admin_token_file"`
	RecoveryKeyFile          string        `json:"recovery_key_file"`
	GatewayRecoveryTokenFile string        `json:"gateway_recovery_token_file"`
	Origins                  []string      `json:"origins"`
	Spaces                   []SpaceConfig `json:"spaces"`
	PackMiB                  int64         `json:"pack_mib"`
	StagingMiB               int64         `json:"staging_mib"`
	SealSeconds              int64         `json:"seal_seconds"`
	CheckpointSeconds        int64         `json:"checkpoint_seconds"`
	UploadTTLSeconds         int64         `json:"upload_ttl_seconds"`
	LeaseSeconds             int64         `json:"lease_seconds"`
}

func ReadConfig(path string) (Config, error) {
	var c Config
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	if e != nil {
		return c, e
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:9400"
	}
	if c.PackMiB == 0 {
		c.PackMiB = 64
	}
	if c.StagingMiB == 0 {
		c.StagingMiB = 8192
	}
	if c.CheckpointSeconds == 0 {
		c.CheckpointSeconds = 3600
	}
	if c.UploadTTLSeconds == 0 {
		c.UploadTTLSeconds = 86400
	}
	if c.LeaseSeconds == 0 {
		c.LeaseSeconds = 120
	}
	if c.StagingMiB > 1<<30 || c.Dir == "" || len(c.Spaces) == 0 || c.PackMiB < 8 || c.PackMiB > 64 || c.StagingMiB < 2*c.PackMiB || c.SealSeconds < 0 || c.CheckpointSeconds < 1 || c.UploadTTLSeconds < 60 || c.LeaseSeconds < 10 || c.LeaseSeconds > 600 {
		return c, fmt.Errorf("invalid vault size, timing or directory configuration")
	}
	for _, raw := range []string{c.Gateway, c.PublicURL} {
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, fmt.Errorf("invalid gateway or public_url")
		}
	}
	public, _ := url.Parse(c.PublicURL)
	if public.Path != "" && public.Path != "/" {
		return c, fmt.Errorf("public_url must be an origin without a path")
	}
	c.Gateway = strings.TrimRight(c.Gateway, "/")
	c.PublicURL = strings.TrimRight(c.PublicURL, "/")
	seen := map[string]bool{}
	for _, s := range c.Spaces {
		if !spaceIdentifier.MatchString(s.Name) || seen[s.Name] {
			return c, fmt.Errorf("invalid or duplicate space")
		}
		seen[s.Name] = true
	}
	return c, nil
}
func secret(path string, min int) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	s := strings.TrimSpace(string(b))
	if len(s) < min {
		return "", fmt.Errorf("secret in %s is too short", path)
	}
	return s, nil
}
func randomID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func recoveryKey(path string) ([]byte, error) {
	s, e := secret(path, 40)
	if e != nil {
		return nil, e
	}
	b, e := base64.StdEncoding.DecodeString(s)
	if e != nil || len(b) != 32 {
		return nil, fmt.Errorf("recovery key must be base64 encoded 32 random bytes")
	}
	return b, nil
}
