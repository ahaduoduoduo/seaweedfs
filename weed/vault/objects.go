package vault

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Object struct {
	Checkpoint string          `json:"checkpoint"`
	ID         string          `json:"id"`
	Version    string          `json:"version"`
	Revision   int64           `json:"revision"`
	Size       int64           `json:"size"`
	SHA        string          `json:"sha256"`
	MIME       string          `json:"content_type"`
	Metadata   json.RawMessage `json:"metadata"`
	State      string          `json:"state"`
	Created    int64           `json:"created"`
	Batch      string          `json:"-"`
	Deleted    bool            `json:"deleted"`
}

func (s *Server) object(space, id, version string) (Object, error) {
	var o Object
	var meta string
	q := "SELECT v.object_id,v.vid,v.revision,v.size,v.sha,v.mime,v.metadata,v.state,v.created,v.batch,v.deleted,v.checkpoint FROM versions v "
	args := []any{space, id}
	if version == "" {
		q += "JOIN objects o ON o.vid=v.vid WHERE o.space=? AND o.object_id=?"
	} else {
		q += "WHERE v.space=? AND v.object_id=? AND v.vid=?"
		args = append(args, version)
	}
	e := s.db.QueryRow(q, args...).Scan(&o.ID, &o.Version, &o.Revision, &o.Size, &o.SHA, &o.MIME, &meta, &o.State, &o.Created, &o.Batch, &o.Deleted, &o.Checkpoint)
	o.Metadata = json.RawMessage(meta)
	return o, e
}
func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "read"); !ok {
		return
	}
	o, e := s.object(r.PathValue("space"), r.PathValue("object"), r.URL.Query().Get("version"))
	if e != nil || o.Deleted {
		fail(w, 404, "object not found")
		return
	}
	w.Header().Set("ETag", `"`+o.Version+`"`)
	reply(w, 200, o)
}
func (s *Server) listObjects(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "read"); !ok {
		return
	}
	rows, e := s.db.Query("SELECT o.object_id FROM objects o JOIN versions v ON o.vid=v.vid WHERE o.space=? AND o.object_id>? AND v.deleted=0 ORDER BY o.object_id LIMIT 101", r.PathValue("space"), r.URL.Query().Get("after"))
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			break
		}
		ids = append(ids, id)
	}
	e = errors.Join(e, rows.Err(), rows.Close())
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	next := ""
	if len(ids) > 100 {
		ids = ids[:100]
		next = ids[99]
	}
	items := []Object{}
	for _, id := range ids {
		o, e := s.object(r.PathValue("space"), id, "")
		if e != nil {
			fail(w, 500, e.Error())
			return
		}
		if !o.Deleted {
			items = append(items, o)
		}
	}
	reply(w, 200, map[string]any{"objects": items, "next": next})
}
func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "write"); !ok {
		return
	}
	rev, e := strconv.ParseInt(r.Header.Get("If-Match"), 10, 64)
	if e != nil {
		fail(w, 400, "If-Match must be current revision")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mutation(w) {
		return
	}
	o, e := s.object(r.PathValue("space"), r.PathValue("object"), "")
	if e != nil {
		fail(w, 404, "object not found")
		return
	}
	if o.Revision != rev {
		fail(w, 409, "version conflict")
		return
	}
	id := randomID()
	tx, e := s.db.Begin()
	if e == nil {
		_, e = tx.Exec("INSERT INTO versions(vid,space,object_id,revision,batch,size,sha,mime,metadata,created,state,deleted) VALUES(?,?,?,?,?,0,'','','{}',?,'remote',1)", id, r.PathValue("space"), o.ID, rev+1, "", time.Now().Unix())
		if e == nil {
			_, e = tx.Exec("UPDATE objects SET vid=? WHERE space=? AND object_id=?", id, r.PathValue("space"), o.ID)
		}
		e = commit(tx, e)
	}
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	reply(w, 200, map[string]any{"revision": rev + 1, "version": id})
}

type Chunk struct {
	Index        int    `json:"index"`
	Pack         string `json:"pack"`
	PlainOffset  int64  `json:"plain_offset"`
	PlainSize    int64  `json:"plain_size"`
	CipherOffset int64  `json:"cipher_offset"`
	CipherSize   int64  `json:"cipher_size"`
	Key          string `json:"key"`
	AAD          string `json:"aad"`
}
type Pack struct {
	ID    string `json:"id"`
	Size  int64  `json:"size"`
	Key   string `json:"-"`
	SHA   string `json:"sha256"`
	State string `json:"state"`
	Space string `json:"-"`
}

func (s *Server) readPlan(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "read"); !ok {
		return
	}
	o, e := s.object(r.PathValue("space"), r.PathValue("object"), r.URL.Query().Get("version"))
	if e != nil || o.Deleted {
		fail(w, 404, "object not found")
		return
	}
	out := map[string]any{"protocol": "vault/v1", "cipher": "AES-256-GCM", "nonce_bytes": 12, "tag_bits": 128, "object": o}
	if o.State == "accepted" {
		out["pending_url"] = "/v1/spaces/" + r.PathValue("space") + "/objects/" + o.ID + "/pending/" + o.Version
		reply(w, 200, out)
		return
	}
	rows, e := s.db.Query("SELECT idx,pack,plain_offset,plain_size,cipher_offset,cipher_size,key,aad FROM chunks WHERE vid=? ORDER BY idx", o.Version)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	chunks := []Chunk{}
	for rows.Next() {
		var c Chunk
		if e = rows.Scan(&c.Index, &c.Pack, &c.PlainOffset, &c.PlainSize, &c.CipherOffset, &c.CipherSize, &c.Key, &c.AAD); e != nil {
			break
		}
		chunks = append(chunks, c)
	}
	e = errors.Join(e, rows.Err(), rows.Close())
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	packs := map[string]Pack{}
	for _, c := range chunks {
		if _, ok := packs[c.Pack]; ok {
			continue
		}
		p, e := s.pack(c.Pack, r.PathValue("space"))
		if e != nil {
			fail(w, 500, e.Error())
			return
		}
		packs[c.Pack] = p
	}
	out["chunks"] = chunks
	out["packs"] = packs
	reply(w, 200, out)
}
func (s *Server) pendingBytes(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "read"); !ok {
		return
	}
	o, e := s.object(r.PathValue("space"), r.PathValue("object"), r.PathValue("vid"))
	if e != nil || o.Deleted {
		fail(w, 404, "object not found")
		return
	}
	f, e := os.Open(s.stagePath(o.Batch, o.ID))
	if e != nil {
		if o.Size == 0 {
			w.WriteHeader(200)
			return
		}
		fail(w, 409, "staging moved to packs; refresh read plan")
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", o.MIME)
	http.ServeContent(w, r, o.ID, time.Unix(o.Created, 0), f)
}
func (s *Server) pack(id, space string) (Pack, error) {
	var p Pack
	e := s.db.QueryRow("SELECT id,space,size,sha,remote_key,state FROM packs WHERE id=? AND space=?", id, space).Scan(&p.ID, &p.Space, &p.Size, &p.SHA, &p.Key, &p.State)
	return p, e
}
