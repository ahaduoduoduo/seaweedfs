package vault

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend/openlist_backend"
)

func (s *Server) gatewayRequest(r *http.Request, space, method, path string, body io.Reader) (*http.Response, error) {
	req, e := http.NewRequestWithContext(r.Context(), method, s.config.Gateway+"/"+space+path, body)
	if e != nil {
		return nil, e
	}
	req.Header.Set("Authorization", "Bearer "+s.spaces[space].gatewayToken)
	req.Header.Set("Content-Type", "application/json")
	return s.client.Do(req)
}
func (s *Server) acquireLease(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.authorize(w, r, "read")
	if !ok {
		return
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	p, e := s.pack(r.PathValue("pack"), r.PathValue("space"))
	if e != nil {
		fail(w, 404, "pack not found")
		return
	}
	source := ""
	var urlExpires int64
	var downloadUserAgent *string
	if p.State == "remote" {
		res, e := s.gatewayRequest(r, p.Space, "GET", "/objects/"+digest(p.Key), nil)
		if e != nil {
			fail(w, 502, "gateway unavailable")
			return
		}
		res.Body.Close()
		if res.StatusCode != 302 {
			if retry := res.Header.Get("Retry-After"); retry != "" {
				w.Header().Set("Retry-After", retry)
			}
			fail(w, res.StatusCode, "gateway declined new link request")
			return
		}
		source = res.Header.Get("Location")
		if value := res.Header.Get("X-SeaweedFS-Download-User-Agent"); value != "" {
			if e := json.Unmarshal([]byte(value), &downloadUserAgent); e != nil {
				fail(w, 502, "invalid gateway download user agent")
				return
			}
		}
		urlExpires, _ = strconv.ParseInt(res.Header.Get("X-SeaweedFS-Link-Expires-At"), 10, 64)
		u, e := url.Parse(source)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
			fail(w, 502, "invalid gateway redirect")
			return
		}
	}
	id := randomID()
	expires := time.Now().Unix() + s.config.LeaseSeconds
	_, e = s.db.Exec("DELETE FROM read_sessions WHERE expires<=?", time.Now().Unix())
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	_, e = s.db.Exec("INSERT INTO read_sessions(lease_id,space,pack,owner,url,expires,url_expires,download_ua) VALUES(?,?,?,?,?,?,?,?)", id, p.Space, p.ID, owner, source, expires, urlExpires, downloadUserAgent)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	proxy := "/v1/spaces/" + p.Space + "/leases/" + id + "/bytes"
	reply(w, 201, map[string]any{"id": id, "url": source, "url_cached_until": urlExpires, "download_user_agent": downloadUserAgent, "proxy_url": proxy, "expires": expires, "renew_after_seconds": max(int64(2), s.config.LeaseSeconds/3), "pack": p})
}
func (s *Server) validLease(r *http.Request, owner string) (string, string, error) {
	var pack, source string
	e := s.db.QueryRow("SELECT pack,url FROM read_sessions WHERE lease_id=? AND space=? AND owner=? AND expires>?", r.PathValue("lease"), r.PathValue("space"), owner, time.Now().Unix()).Scan(&pack, &source)
	return pack, source, e
}
func (s *Server) renewLease(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.authorize(w, r, "read")
	if !ok {
		return
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if _, _, e := s.validLease(r, owner); e != nil {
		fail(w, 409, "read session expired")
		return
	}
	e := requireAffected(s.db.Exec("UPDATE read_sessions SET expires=? WHERE lease_id=?", time.Now().Unix()+s.config.LeaseSeconds, r.PathValue("lease")))
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	w.WriteHeader(204)
}
func (s *Server) completeLease(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.authorize(w, r, "read")
	if !ok {
		return
	}
	var in struct {
		Status int `json:"status"`
	}
	if !decode(w, r, &in) {
		return
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	pack, source, e := s.validLease(r, owner)
	if e != nil {
		fail(w, 409, "read session expired")
		return
	}
	if source != "" && (rejectedLink(in.Status) || in.Status == 200 || in.Status == 206) {
		p, e := s.pack(pack, r.PathValue("space"))
		if e == nil && p.Key != "" {
			b, _ := json.Marshal(map[string]any{"status": in.Status, "url": source})
			res, e := s.gatewayRequest(r, p.Space, "POST", "/objects/"+digest(p.Key)+"/failure", bytes.NewReader(b))
			if e != nil {
				fail(w, 502, "could not report failed provider link")
				return
			}
			res.Body.Close()
			if res.StatusCode >= 400 && !(res.StatusCode == http.StatusServiceUnavailable && res.Header.Get("Retry-After") != "") {
				fail(w, 502, "gateway rejected provider cooldown")
				return
			}
		}
	}
	_, e = s.db.Exec("DELETE FROM read_sessions WHERE lease_id=? AND owner=?", r.PathValue("lease"), owner)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	w.WriteHeader(204)
}
func (s *Server) proxyBytes(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.authorize(w, r, "read")
	if !ok {
		return
	}
	s.leaseMu.Lock()
	id, source, e := s.validLease(r, owner)
	s.leaseMu.Unlock()
	if e != nil {
		fail(w, 409, "read session expired")
		return
	}
	p, e := s.pack(id, r.PathValue("space"))
	if e != nil {
		fail(w, 404, "pack not found")
		return
	}
	if source == "" {
		f, e := os.Open(s.packPath(id))
		if e != nil {
			fail(w, 409, "pack moved to cloud; obtain a new lease")
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(p.Size, 10))
		io.CopyN(w, f, p.Size)
		return
	}
	req, e := http.NewRequestWithContext(r.Context(), "GET", source, nil)
	if e != nil {
		fail(w, 502, "invalid source")
		return
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", p.Size-1))
	var downloadUserAgent sql.NullString
	if e := s.db.QueryRow("SELECT download_ua FROM read_sessions WHERE lease_id=? AND owner=?", r.PathValue("lease"), owner).Scan(&downloadUserAgent); e != nil {
		fail(w, 409, "read session expired")
		return
	}
	if downloadUserAgent.Valid {
		req.Header.Set("User-Agent", downloadUserAgent.String)
	}
	res, e := s.client.Do(req)
	if e != nil {
		fail(w, 502, "cloud download failed")
		return
	}
	defer res.Body.Close()
	if openlist_backend.IsTransientDownloadBusy(res) {
		w.Header().Set("Retry-After", "5")
		w.Header().Set("X-Vault-CDN-Busy", "1")
		fail(w, 503, "provider download busy; reuse this URL after waiting")
		return
	}
	if res.StatusCode != 206 || res.Header.Get("Content-Range") != fmt.Sprintf("bytes 0-%d/%d", p.Size-1, p.Size) {
		if rejectedLink(res.StatusCode) {
			fail(w, res.StatusCode, "cloud download link rejected")
		} else {
			fail(w, 502, "invalid cloud range response")
		}
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(p.Size, 10))
	io.CopyN(w, res.Body, p.Size)
}

func rejectedLink(status int) bool {
	switch status {
	case 401, 403, 404, 405, 410, 429:
		return true
	}
	return false
}
