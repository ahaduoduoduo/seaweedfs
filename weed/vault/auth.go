package vault

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type claims struct {
	Space   string `json:"space"`
	Subject string `json:"subject"`
	Scope   string `json:"scope"`
	Expires int64  `json:"expires"`
}

func signature(key []byte, b []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(b)
	return h.Sum(nil)
}
func (s *Server) authorize(w http.ResponseWriter, r *http.Request, scope string) (string, bool) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	sp := s.spaces[r.PathValue("space")]
	if sp == nil || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		fail(w, 401, "valid space token required")
		return "", false
	}
	sum := sha256.Sum256([]byte(token))
	owner := hex.EncodeToString(sum[:])
	if subtle.ConstantTimeCompare([]byte(token), []byte(sp.token)) == 1 {
		return owner, true
	}
	parts := strings.Split(token, ".")
	if len(parts) == 2 && scope != "admin" {
		b, e := base64.RawURLEncoding.DecodeString(parts[0])
		sig, e2 := base64.RawURLEncoding.DecodeString(parts[1])
		var c claims
		if e == nil && e2 == nil && hmac.Equal(sig, signature([]byte(sp.token), b)) && json.Unmarshal(b, &c) == nil && c.Space == r.PathValue("space") && c.Expires > time.Now().Unix() && (c.Scope == scope || (scope == "read" && c.Scope == "write")) {
			return owner, true
		}
	}
	fail(w, 403, "token lacks permission or has expired")
	return "", false
}
func (s *Server) admin(w http.ResponseWriter, r *http.Request) bool {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare([]byte(token), []byte(s.adminToken)) != 1 {
		fail(w, 403, "server administrator token required")
		return false
	}
	return true
}
func (s *Server) issueToken(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "admin"); !ok {
		return
	}
	var in struct {
		Subject string `json:"subject"`
		Scope   string `json:"scope"`
		TTL     int64  `json:"ttl_seconds"`
	}
	if !decode(w, r, &in) {
		return
	}
	if len(in.Subject) < 1 || len(in.Subject) > 128 || (in.Scope != "read" && in.Scope != "write") || in.TTL < 1 || in.TTL > 86400 {
		fail(w, 400, "subject, read/write scope and ttl_seconds 1..86400 required")
		return
	}
	c := claims{r.PathValue("space"), in.Subject, in.Scope, time.Now().Unix() + in.TTL}
	b, _ := json.Marshal(c)
	sig := signature([]byte(s.spaces[c.Space].token), b)
	reply(w, 201, map[string]any{"token": base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(sig), "expires": c.Expires})
}
