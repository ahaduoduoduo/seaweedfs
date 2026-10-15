package vault

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type Receipt struct {
	Space  string          `json:"space"`
	Object json.RawMessage `json:"object"`
}
type CheckPart struct {
	Key     string  `json:"key"`
	Size    int64   `json:"size"`
	Offset  int64   `json:"offset"`
	Length  int64   `json:"length"`
	Receipt Receipt `json:"receipt"`
}
type TicketData struct {
	Protocol string      `json:"protocol"`
	ID       string      `json:"id"`
	Created  int64       `json:"created"`
	Parts    []CheckPart `json:"parts"`
}
type Ticket struct {
	Data      []byte `json:"data"`
	Signature []byte `json:"signature"`
}

func (s *Server) listCheckpoints(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	rows, e := s.db.Query("SELECT id,state,created,error FROM checkpoints ORDER BY created DESC,id DESC LIMIT 20")
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, state, detail string
		var created int64
		if e = rows.Scan(&id, &state, &created, &detail); e != nil {
			fail(w, 500, e.Error())
			return
		}
		items = append(items, map[string]any{"id": id, "state": state, "created": created, "error": detail})
	}
	if e = rows.Err(); e != nil {
		fail(w, 500, e.Error())
		return
	}
	reply(w, 200, map[string]any{"checkpoints": items})
}
func (s *Server) createCheckpoint(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var id string
	e := s.db.QueryRow("SELECT id FROM checkpoints WHERE state IN ('queued','running') LIMIT 1").Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		id = randomID()
		_, e = s.db.Exec("INSERT INTO checkpoints(id,state,created) VALUES(?,'queued',?)", id, time.Now().Unix())
	}
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	s.signal()
	reply(w, 202, map[string]string{"id": id})
}
func (s *Server) getCheckpoint(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	var state, err string
	var created int64
	e := s.db.QueryRow("SELECT state,error,created FROM checkpoints WHERE id=?", r.PathValue("checkpoint")).Scan(&state, &err, &created)
	if e != nil {
		fail(w, 404, "checkpoint not found")
		return
	}
	reply(w, 200, map[string]any{"id": r.PathValue("checkpoint"), "state": state, "error": err, "created": created})
}
func (s *Server) getTicket(w http.ResponseWriter, r *http.Request) {
	if !s.admin(w, r) {
		return
	}
	var ticket string
	e := s.db.QueryRow("SELECT ticket FROM checkpoints WHERE id=? AND state='complete'", r.PathValue("checkpoint")).Scan(&ticket)
	if e != nil {
		fail(w, 404, "completed checkpoint not found")
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="vault-`+r.PathValue("checkpoint")+`.json"`)
	reply(w, 200, json.RawMessage(ticket))
}
func (s *Server) autoCheckpoint(ctx context.Context) {
	var id string
	e := s.db.QueryRow("SELECT id FROM checkpoints WHERE state='queued' ORDER BY created LIMIT 1").Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		var last int64
		s.db.QueryRow("SELECT coalesce(max(created),0) FROM checkpoints").Scan(&last)
		var n int
		s.db.QueryRow("SELECT count(*) FROM versions").Scan(&n)
		if n == 0 || time.Now().Unix()-max(last, s.started) < s.config.CheckpointSeconds {
			return
		}
		id = randomID()
		_, e = s.db.Exec("INSERT INTO checkpoints(id,state,created) VALUES(?,'queued',?)", id, time.Now().Unix())
	}
	if e != nil {
		return
	}
	_, e = s.db.Exec("UPDATE checkpoints SET state='running' WHERE id=?", id)
	if e != nil {
		return
	}
	ticket, e := s.checkpoint(ctx, id)
	if e != nil {
		s.db.Exec("UPDATE checkpoints SET state='failed',error=? WHERE id=?", e.Error(), id)
		s.lastError.Store(e.Error())
		return
	}
	_, e = s.db.Exec("UPDATE checkpoints SET state='complete',ticket=? WHERE id=?", string(ticket), id)
	if e != nil {
		s.lastError.Store(e.Error())
	}
}
func (s *Server) exactReceipt(ctx context.Context, space, key string) (Receipt, error) {
	req, e := http.NewRequestWithContext(ctx, "GET", s.config.Gateway+"/"+space+"/receipts/"+digest(key), nil)
	if e != nil {
		return Receipt{}, e
	}
	req.Header.Set("Authorization", "Bearer "+s.spaces[space].gatewayToken)
	res, e := s.client.Do(req)
	if e != nil {
		return Receipt{}, e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return Receipt{}, fmt.Errorf("receipt HTTP %d", res.StatusCode)
	}
	b, e := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if e == nil && !json.Valid(b) {
		e = fmt.Errorf("invalid receipt")
	}
	return Receipt{space, b}, e
}
func (s *Server) checkpoint(ctx context.Context, id string) ([]byte, error) {
	s.work.Lock()
	defer s.work.Unlock()
	s.mu.Lock()
	s.maintenance.Store(true)
	s.mu.Unlock()
	defer s.maintenance.Store(false)
	if e := s.process(ctx, true); e != nil {
		return nil, e
	}
	var pending int
	s.db.QueryRow("SELECT count(*) FROM batches WHERE state IN ('accepted','packed')").Scan(&pending)
	if pending != 0 {
		return nil, fmt.Errorf("committed data still awaiting cloud receipts")
	}
	dir, e := os.MkdirTemp(filepath.Join(s.config.Dir, "checkpoints"), "work-")
	if e != nil {
		return nil, e
	}
	defer os.RemoveAll(dir)
	snapshot := filepath.Join(dir, "vault.db")
	if _, e = s.db.Exec("VACUUM INTO ?", snapshot); e != nil {
		return nil, e
	}
	db, e := openDB(snapshot)
	if e != nil {
		return nil, e
	}
	_, e = db.Exec("UPDATE versions SET checkpoint=?; DELETE FROM uploads WHERE batch IN (SELECT id FROM batches WHERE state IN ('uploading','aborted')); DELETE FROM batches WHERE state IN ('uploading','aborted'); DELETE FROM read_sessions; DELETE FROM checkpoints WHERE state!='complete'; PRAGMA wal_checkpoint(TRUNCATE)", id)
	e = errors.Join(e, db.Close())
	if e != nil {
		return nil, e
	}
	receipts, e := os.OpenFile(filepath.Join(dir, "receipts.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	enc := json.NewEncoder(receipts)
	// Keyset pagination keeps memory bounded and reads only local SQL rows.
	after := ""
	for {
		rows, e := s.db.Query("SELECT id,space,remote_key FROM packs WHERE id>? ORDER BY id LIMIT 256", after)
		if e != nil {
			receipts.Close()
			return nil, e
		}
		type row struct{ id, space, key string }
		var batch []row
		for rows.Next() {
			var v row
			if e = rows.Scan(&v.id, &v.space, &v.key); e != nil {
				break
			}
			batch = append(batch, v)
		}
		e = errors.Join(e, rows.Err(), rows.Close())
		if e != nil {
			receipts.Close()
			return nil, e
		}
		if len(batch) == 0 {
			break
		}
		for _, v := range batch {
			receipt, e := s.exactReceipt(ctx, v.space, v.key)
			if e == nil {
				e = enc.Encode(receipt)
			}
			if e != nil {
				receipts.Close()
				return nil, e
			}
			after = v.id
		}
	}
	if e = receipts.Close(); e != nil {
		return nil, e
	}
	archive := filepath.Join(dir, "snapshot.tar.gz")
	if e = makeArchive(archive, dir, []string{"vault.db", "receipts.jsonl"}); e != nil {
		return nil, e
	}
	names := []string{}
	for name := range s.spaces {
		names = append(names, name)
	}
	sort.Strings(names)
	space := names[0]
	f, e := os.Open(archive)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	data := TicketData{Protocol: "vault/checkpoint/v1", ID: id, Created: time.Now().Unix()}
	buffer := make([]byte, (s.config.PackMiB<<20)-128)
	for index := 0; ; index++ {
		n, e := io.ReadFull(f, buffer)
		if e == io.EOF {
			break
		}
		if e != nil && e != io.ErrUnexpectedEOF {
			return nil, e
		}
		aad := []byte(fmt.Sprintf("vault/checkpoint/v1/%s/%d", id, index))
		key := signature(s.recovery, aad)
		encrypted, e := encrypt(buffer[:n], key, aad)
		if e != nil {
			return nil, e
		}
		p, e := newPack(dir)
		if e != nil {
			return nil, e
		}
		offset, e := p.append(encrypted)
		if e == nil {
			e = p.close()
		} else {
			p.f.Close()
		}
		if e != nil {
			return nil, e
		}
		pf, e := os.Open(filepath.Join(dir, p.id+".dat"))
		if e != nil {
			return nil, e
		}
		remote, size, e := s.spaces[space].backend.CopyFile(pf, nil, 0)
		pf.Close()
		if e != nil {
			return nil, e
		}
		receipt, e := s.exactReceipt(ctx, space, remote)
		if e != nil {
			return nil, e
		}
		data.Parts = append(data.Parts, CheckPart{remote, size, offset, int64(len(encrypted)), receipt})
		os.Remove(filepath.Join(dir, p.id+".dat"))
	}
	raw, _ := json.Marshal(data)
	ticket, e := json.MarshalIndent(Ticket{raw, signature(s.recovery, raw)}, "", "  ")
	if e != nil {
		return nil, e
	}
	path := filepath.Join(s.config.Dir, "checkpoints", id+".json")
	if e = writeSynced(path, ticket); e != nil {
		return nil, e
	}
	tx, e := s.db.Begin()
	if e != nil {
		return nil, e
	}
	_, e = tx.Exec("UPDATE versions SET checkpoint=?", id)
	if e == nil {
		_, e = tx.Exec("UPDATE checkpoints SET state='complete',ticket=? WHERE id=?", string(ticket), id)
	}
	if e = commit(tx, e); e != nil {
		return nil, e
	}
	return ticket, nil
}
func makeArchive(path, dir string, names []string) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	for _, name := range names {
		input, e := os.Open(filepath.Join(dir, name))
		if e != nil {
			f.Close()
			return e
		}
		stat, e := input.Stat()
		if e == nil {
			e = tw.WriteHeader(&tar.Header{Name: name, Size: stat.Size(), Mode: 0600})
		}
		if e == nil {
			_, e = io.Copy(tw, input)
		}
		input.Close()
		if e != nil {
			f.Close()
			return e
		}
	}
	return errors.Join(tw.Close(), gz.Close(), f.Sync(), f.Close())
}
func writeSynced(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	return errors.Join(e, f.Sync(), f.Close())
}
