package vault

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
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
	var expires int64
	s.db.QueryRow("SELECT expires FROM lease WHERE id=1").Scan(&expires)
	if expires > time.Now().Unix() {
		w.Header().Set("Retry-After", "1")
		fail(w, 429, "another client holds the download lease")
		return
	}
	p, e := s.pack(r.PathValue("pack"), r.PathValue("space"))
	if e != nil {
		fail(w, 404, "pack not found")
		return
	}
	source := ""
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
			fail(w, res.StatusCode, "gateway declined download permit")
			return
		}
		source = res.Header.Get("Location")
		u, e := url.Parse(source)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
			fail(w, 502, "invalid gateway redirect")
			return
		}
	}
	id := randomID()
	expires = time.Now().Unix() + s.config.LeaseSeconds
	_, e = s.db.Exec("INSERT INTO lease(id,lease_id,space,pack,owner,url,expires) VALUES(1,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET lease_id=excluded.lease_id,space=excluded.space,pack=excluded.pack,owner=excluded.owner,url=excluded.url,expires=excluded.expires,started=0", id, p.Space, p.ID, owner, source, expires)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	proxy := "/v1/spaces/" + p.Space + "/leases/" + id + "/bytes"
	reply(w, 201, map[string]any{"id": id, "url": source, "proxy_url": proxy, "expires": expires, "renew_after_seconds": max(int64(2), s.config.LeaseSeconds/3), "pack": p})
}
func (s *Server) validLease(r *http.Request, owner string) (string, string, error) {
	var pack, source string
	e := s.db.QueryRow("SELECT pack,url FROM lease WHERE lease_id=? AND space=? AND owner=? AND expires>?", r.PathValue("lease"), r.PathValue("space"), owner, time.Now().Unix()).Scan(&pack, &source)
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
		fail(w, 409, "download lease expired")
		return
	}
	e := requireAffected(s.db.Exec("UPDATE lease SET expires=? WHERE lease_id=?", time.Now().Unix()+s.config.LeaseSeconds, r.PathValue("lease")))
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
	pack, _, e := s.validLease(r, owner)
	if e != nil {
		fail(w, 409, "download lease expired")
		return
	}
	if in.Status == 403 || in.Status == 405 || in.Status == 429 {
		p, e := s.pack(pack, r.PathValue("space"))
		if e == nil && p.Key != "" {
			b, _ := json.Marshal(in)
			res, e := s.gatewayRequest(r, p.Space, "POST", "/objects/"+digest(p.Key)+"/failure", bytes.NewReader(b))
			if e != nil {
				fail(w, 502, "could not report provider cooldown")
				return
			}
			res.Body.Close()
			if res.StatusCode >= 400 && !(res.StatusCode == http.StatusServiceUnavailable && res.Header.Get("Retry-After") != "") {
				fail(w, 502, "gateway rejected provider cooldown")
				return
			}
		}
	}
	_, e = s.db.Exec("DELETE FROM lease WHERE lease_id=? AND owner=?", r.PathValue("lease"), owner)
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
	if e == nil {
		e = requireAffected(s.db.Exec("UPDATE lease SET started=1 WHERE lease_id=? AND started=0", r.PathValue("lease")))
	}
	s.leaseMu.Unlock()
	if e != nil {
		fail(w, 409, "lease expired or proxy transfer already started")
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
	res, e := s.client.Do(req)
	if e != nil {
		fail(w, 502, "cloud download failed")
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 206 || res.Header.Get("Content-Range") != fmt.Sprintf("bytes 0-%d/%d", p.Size-1, p.Size) {
		if res.StatusCode == 403 || res.StatusCode == 405 || res.StatusCode == 429 {
			fail(w, res.StatusCode, "cloud download blocked")
		} else {
			fail(w, 502, "invalid cloud range response")
		}
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(p.Size, 10))
	io.CopyN(w, res.Body, p.Size)
}
