package vault

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type ObjectInput struct {
	ID        string          `json:"id"`
	Size      int64           `json:"size"`
	SHA       string          `json:"sha256"`
	MIME      string          `json:"content_type"`
	IfVersion int64           `json:"if_version"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}
type BatchInput struct {
	IdempotencyKey string        `json:"idempotency_key"`
	Group          string        `json:"group"`
	SortKey        string        `json:"sort_key"`
	Objects        []ObjectInput `json:"objects"`
}

func (s *Server) createBatch(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "write"); !ok {
		return
	}
	var in BatchInput
	if !decode(w, r, &in) {
		return
	}
	if !identifier.MatchString(in.IdempotencyKey) || len(in.Group) > 256 || len(in.SortKey) > 256 || len(in.Objects) < 1 || len(in.Objects) > 1000 {
		fail(w, 400, "invalid batch manifest")
		return
	}
	var total int64
	seen := map[string]bool{}
	for i := range in.Objects {
		o := &in.Objects[i]
		hash, e := hex.DecodeString(o.SHA)
		if !identifier.MatchString(o.ID) || seen[o.ID] || o.Size < 0 || o.Size > s.config.StagingMiB<<20 || e != nil || len(hash) != 32 || hex.EncodeToString(hash) != o.SHA || o.IfVersion < 0 || len(o.MIME) > 256 || len(o.Metadata) > 65536 {
			fail(w, 400, "invalid object manifest")
			return
		}
		seen[o.ID] = true
		total += o.Size
		if len(o.Metadata) == 0 {
			o.Metadata = json.RawMessage(`{}`)
		}
	}
	if total > s.config.StagingMiB<<20 {
		fail(w, 413, "batch exceeds staging quota")
		return
	}
	raw, _ := json.Marshal(in)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mutation(w) {
		return
	}
	var id, existing string
	e := s.db.QueryRow("SELECT id,manifest FROM batches WHERE space=? AND idem=?", r.PathValue("space"), in.IdempotencyKey).Scan(&id, &existing)
	if e == nil {
		if existing != string(raw) {
			fail(w, 409, "idempotency key belongs to another manifest")
			return
		}
		reply(w, 200, map[string]string{"id": id})
		return
	}
	if !errors.Is(e, sql.ErrNoRows) {
		fail(w, 500, e.Error())
		return
	}
	var reserved int64
	if e = s.db.QueryRow("SELECT coalesce(sum(u.size),0) FROM uploads u JOIN batches b ON b.id=u.batch WHERE b.state NOT IN ('remote','aborted')").Scan(&reserved); e != nil {
		fail(w, 500, e.Error())
		return
	}
	if reserved+total > s.config.StagingMiB<<20 {
		w.Header().Set("Retry-After", "30")
		fail(w, 507, "staging quota exhausted")
		return
	}
	id = randomID()
	tx, e := s.db.Begin()
	if e == nil {
		_, e = tx.Exec("INSERT INTO batches(id,space,idem,group_key,sort_key,state,created,manifest) VALUES(?,?,?,?,?,'uploading',?,?)", id, r.PathValue("space"), in.IdempotencyKey, in.Group, in.SortKey, time.Now().Unix(), string(raw))
		for _, o := range in.Objects {
			if e != nil {
				break
			}
			_, e = tx.Exec("INSERT INTO uploads(batch,object_id,vid,size,sha,mime,base_version,metadata) VALUES(?,?,?,?,?,?,?,?)", id, o.ID, randomID(), o.Size, o.SHA, o.MIME, o.IfVersion, string(o.Metadata))
		}
		e = commit(tx, e)
	}
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	reply(w, 201, map[string]string{"id": id})
}
func (s *Server) batchState(space, id string) (string, error) {
	var state string
	e := s.db.QueryRow("SELECT state FROM batches WHERE id=? AND space=?", id, space).Scan(&state)
	return state, e
}
func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "read"); !ok {
		return
	}
	state, e := s.batchState(r.PathValue("space"), r.PathValue("batch"))
	if e != nil {
		fail(w, 404, "batch not found")
		return
	}
	rows, e := s.db.Query("SELECT object_id,size,uploaded FROM uploads WHERE batch=? ORDER BY object_id", r.PathValue("batch"))
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id string
		var size, uploaded int64
		if e = rows.Scan(&id, &size, &uploaded); e != nil {
			break
		}
		items = append(items, map[string]any{"id": id, "size": size, "uploaded": uploaded})
	}
	if e = rows.Err(); e != nil {
		fail(w, 500, e.Error())
		return
	}
	reply(w, 200, map[string]any{"id": r.PathValue("batch"), "state": state, "objects": items})
}
func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "write"); !ok {
		return
	}
	offset, e := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if e != nil || offset < 0 {
		fail(w, 400, "Upload-Offset required")
		return
	}
	data, e := io.ReadAll(http.MaxBytesReader(w, r.Body, ChunkSize))
	if e != nil {
		fail(w, 413, "upload chunk exceeds 4 MiB")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mutation(w) {
		return
	}
	batch, object := r.PathValue("batch"), r.PathValue("object")
	state, e := s.batchState(r.PathValue("space"), batch)
	if e != nil {
		fail(w, 404, "batch not found")
		return
	}
	if state != "uploading" {
		fail(w, 409, "batch is not accepting bytes")
		return
	}
	var size, uploaded int64
	e = s.db.QueryRow("SELECT size,uploaded FROM uploads WHERE batch=? AND object_id=?", batch, object).Scan(&size, &uploaded)
	if e != nil {
		fail(w, 404, "object not in batch")
		return
	}
	if offset != uploaded {
		w.Header().Set("Upload-Offset", strconv.FormatInt(uploaded, 10))
		fail(w, 409, "resume at returned Upload-Offset")
		return
	}
	if uploaded+int64(len(data)) > size {
		fail(w, 400, "upload exceeds declared size")
		return
	}
	f, e := os.OpenFile(s.stagePath(batch, object), os.O_CREATE|os.O_RDWR, 0600)
	if e == nil {
		e = f.Truncate(uploaded)
		if e == nil {
			_, e = f.WriteAt(data, uploaded)
		}
		if e == nil {
			e = f.Sync()
		}
		e = errors.Join(e, f.Close())
		if e == nil {
			e = util.FsyncDir(filepath.Join(s.config.Dir, "staging"))
		}
	}
	if e == nil {
		_, e = s.db.Exec("UPDATE uploads SET uploaded=? WHERE batch=? AND object_id=?", uploaded+int64(len(data)), batch, object)
	}
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(uploaded+int64(len(data)), 10))
	w.WriteHeader(204)
}

type uploadRecord struct {
	ID, VID, SHA, MIME, Metadata string
	Size, Base, Uploaded         int64
}

func (s *Server) batchUploads(id string) ([]uploadRecord, error) {
	rows, e := s.db.Query("SELECT object_id,vid,size,sha,mime,base_version,uploaded,metadata FROM uploads WHERE batch=? ORDER BY object_id", id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []uploadRecord
	for rows.Next() {
		var u uploadRecord
		if e = rows.Scan(&u.ID, &u.VID, &u.Size, &u.SHA, &u.MIME, &u.Base, &u.Uploaded, &u.Metadata); e != nil {
			return nil, e
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
func (s *Server) commitBatch(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "write"); !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mutation(w) {
		return
	}
	batch, space := r.PathValue("batch"), r.PathValue("space")
	state, e := s.batchState(space, batch)
	if e != nil {
		fail(w, 404, "batch not found")
		return
	}
	if state == "accepted" || state == "packed" || state == "remote" {
		reply(w, 200, map[string]string{"state": state})
		return
	}
	if state != "uploading" {
		fail(w, 409, "batch aborted")
		return
	}
	items, e := s.batchUploads(batch)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	for _, u := range items {
		if u.Uploaded != u.Size {
			fail(w, 409, "upload incomplete")
			return
		}
		h := sha256.New()
		f, e := os.Open(s.stagePath(batch, u.ID))
		if e == nil {
			_, e = io.Copy(h, f)
			f.Close()
		} else if u.Size == 0 && os.IsNotExist(e) {
			e = nil
		}
		if e != nil {
			fail(w, 500, e.Error())
			return
		}
		if hex.EncodeToString(h.Sum(nil)) != u.SHA {
			fail(w, 422, "sha256 mismatch: "+u.ID)
			return
		}
	}
	tx, e := s.db.Begin()
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	defer tx.Rollback()
	for _, u := range items {
		var rev int64
		e = tx.QueryRow("SELECT v.revision FROM objects o JOIN versions v ON o.vid=v.vid WHERE o.space=? AND o.object_id=?", space, u.ID).Scan(&rev)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			fail(w, 500, e.Error())
			return
		}
		if rev != u.Base {
			fail(w, 409, fmt.Sprintf("version conflict for %s: current %d", u.ID, rev))
			return
		}
		_, e = tx.Exec("INSERT INTO versions(vid,space,object_id,revision,batch,size,sha,mime,metadata,created,state) VALUES(?,?,?,?,?,?,?,?,?,?,'accepted')", u.VID, space, u.ID, rev+1, batch, u.Size, u.SHA, u.MIME, u.Metadata, time.Now().Unix())
		if e == nil {
			_, e = tx.Exec("INSERT INTO objects(space,object_id,vid) VALUES(?,?,?) ON CONFLICT(space,object_id) DO UPDATE SET vid=excluded.vid", space, u.ID, u.VID)
		}
		if e != nil {
			fail(w, 500, e.Error())
			return
		}
	}
	_, e = tx.Exec("UPDATE batches SET state='accepted' WHERE id=?", batch)
	e = commit(tx, e)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	s.signal()
	reply(w, 200, map[string]string{"state": "accepted"})
}
func (s *Server) abortBatch(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "write"); !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mutation(w) {
		return
	}
	id := r.PathValue("batch")
	state, e := s.batchState(r.PathValue("space"), id)
	if e != nil {
		fail(w, 404, "batch not found")
		return
	}
	if state != "uploading" && state != "aborted" {
		fail(w, 409, "committed batches cannot be aborted")
		return
	}
	_, e = s.db.Exec("UPDATE batches SET state='aborted' WHERE id=?", id)
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	s.removeStages(id)
	w.WriteHeader(204)
}
