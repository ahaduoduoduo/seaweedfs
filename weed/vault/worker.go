package vault

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

func (s *Server) worker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		}
		s.work.Lock()
		e := s.process(ctx, false)
		s.work.Unlock()
		if e != nil {
			if previous := s.lastError.Load(); previous != e.Error() {
				log.Printf("vault worker: %v", e)
			}
			s.lastError.Store(e.Error())
		} else {
			s.lastError.Store("")
		}
		s.expireUploads()
		if ctx.Err() != nil {
			return
		}
		s.autoCheckpoint(ctx)
	}
}
func (s *Server) process(ctx context.Context, force bool) error {
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		var space, group string
		var oldest, total int64
		e := s.db.QueryRow("SELECT b.space,b.group_key,min(b.created),sum(u.size) FROM batches b JOIN uploads u ON u.batch=b.id WHERE b.state='accepted' GROUP BY b.space,b.group_key ORDER BY min(b.created) LIMIT 1").Scan(&space, &group, &oldest, &total)
		if errors.Is(e, sql.ErrNoRows) {
			break
		}
		if e != nil {
			return e
		}
		if !force && time.Now().Unix()-oldest < s.config.SealSeconds && total < (s.config.PackMiB<<20)-ChunkSize {
			break
		}
		rows, e := s.db.Query("SELECT id FROM batches WHERE state='accepted' AND space=? AND group_key=? ORDER BY sort_key,created,id", space, group)
		if e != nil {
			return e
		}
		var ids []string
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				break
			}
			ids = append(ids, id)
		}
		e = errors.Join(e, rows.Err(), rows.Close())
		if e != nil {
			return e
		}
		if e = s.packGroup(space, group, ids); e != nil {
			return e
		}
	}
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		var id, space string
		var retry int64
		e := s.db.QueryRow("SELECT id,space,retry_at FROM packs WHERE state='sealed' ORDER BY created,id LIMIT 1").Scan(&id, &space, &retry)
		if errors.Is(e, sql.ErrNoRows) {
			break
		}
		if e != nil {
			return e
		}
		if time.Now().Unix() < retry {
			return fmt.Errorf("pack upload retry scheduled at %d", retry)
		}
		f, e := os.Open(s.packPath(id))
		if e != nil {
			return e
		}
		key, _, e := s.spaces[space].backend.CopyFile(f, nil, 0)
		f.Close()
		if e != nil {
			s.db.Exec("UPDATE packs SET retry_at=?,error=? WHERE id=?", time.Now().Unix()+30, e.Error(), id)
			return e
		}
		if _, e = s.db.Exec("UPDATE packs SET state='remote',remote_key=?,error='' WHERE id=?", key, id); e != nil {
			return e
		}
		os.Remove(s.packPath(id))
	}
	// A batch becomes remote only when every one of its chunks has a receipt.
	rows, e := s.db.Query("SELECT b.id FROM batches b WHERE b.state='packed' AND NOT EXISTS(SELECT 1 FROM versions v JOIN chunks c ON c.vid=v.vid JOIN packs p ON p.id=c.pack WHERE v.batch=b.id AND p.state!='remote')")
	if e != nil {
		return e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			break
		}
		ids = append(ids, id)
	}
	e = errors.Join(e, rows.Err(), rows.Close())
	if e != nil {
		return e
	}
	for _, id := range ids {
		tx, e := s.db.Begin()
		if e != nil {
			return e
		}
		_, e = tx.Exec("UPDATE versions SET state='remote' WHERE batch=?", id)
		if e == nil {
			_, e = tx.Exec("UPDATE batches SET state='remote' WHERE id=?", id)
		}
		if e = commit(tx, e); e != nil {
			return e
		}
		s.removeStages(id)
	}
	return nil
}
func (s *Server) expireUploads() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maintenance.Load() {
		return
	}
	rows, e := s.db.Query("SELECT id FROM batches WHERE state='uploading' AND created<?", time.Now().Unix()-s.config.UploadTTLSeconds)
	if e != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		if _, e = s.db.Exec("UPDATE batches SET state='aborted' WHERE id=?", id); e == nil {
			s.removeStages(id)
		}
	}
}
