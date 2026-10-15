package vault

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY,space TEXT NOT NULL,idem TEXT NOT NULL,group_key TEXT NOT NULL,sort_key TEXT NOT NULL,state TEXT NOT NULL,created INTEGER NOT NULL,manifest TEXT NOT NULL,error TEXT NOT NULL DEFAULT '',UNIQUE(space,idem));
CREATE TABLE IF NOT EXISTS uploads(batch TEXT NOT NULL,object_id TEXT NOT NULL,vid TEXT NOT NULL,size INTEGER NOT NULL,sha TEXT NOT NULL,mime TEXT NOT NULL,base_version INTEGER NOT NULL,uploaded INTEGER NOT NULL DEFAULT 0,metadata TEXT NOT NULL,PRIMARY KEY(batch,object_id));
CREATE TABLE IF NOT EXISTS versions(vid TEXT PRIMARY KEY,space TEXT NOT NULL,object_id TEXT NOT NULL,revision INTEGER NOT NULL,batch TEXT NOT NULL,size INTEGER NOT NULL,sha TEXT NOT NULL,mime TEXT NOT NULL,metadata TEXT NOT NULL,created INTEGER NOT NULL,state TEXT NOT NULL,deleted INTEGER NOT NULL DEFAULT 0,checkpoint TEXT NOT NULL DEFAULT '',UNIQUE(space,object_id,revision));
CREATE TABLE IF NOT EXISTS objects(space TEXT NOT NULL,object_id TEXT NOT NULL,vid TEXT NOT NULL,PRIMARY KEY(space,object_id));
CREATE TABLE IF NOT EXISTS packs(id TEXT PRIMARY KEY,space TEXT NOT NULL,size INTEGER NOT NULL,sha TEXT NOT NULL,remote_key TEXT NOT NULL DEFAULT '',state TEXT NOT NULL,created INTEGER NOT NULL,retry_at INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS chunks(vid TEXT NOT NULL,idx INTEGER NOT NULL,pack TEXT NOT NULL,plain_offset INTEGER NOT NULL,plain_size INTEGER NOT NULL,cipher_offset INTEGER NOT NULL,cipher_size INTEGER NOT NULL,key TEXT NOT NULL,aad TEXT NOT NULL,PRIMARY KEY(vid,idx));
CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY,state TEXT NOT NULL,created INTEGER NOT NULL,error TEXT NOT NULL DEFAULT '',ticket TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS read_sessions(lease_id TEXT PRIMARY KEY,space TEXT NOT NULL,pack TEXT NOT NULL,owner TEXT NOT NULL,url TEXT NOT NULL,expires INTEGER NOT NULL,url_expires INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS read_sessions_expiry ON read_sessions(expires);
CREATE INDEX IF NOT EXISTS batches_pending ON batches(state,created);
CREATE INDEX IF NOT EXISTS versions_batch ON versions(batch);
CREATE INDEX IF NOT EXISTS chunks_pack ON chunks(pack);
CREATE INDEX IF NOT EXISTS versions_space ON versions(space,object_id);
`

func openDB(path string) (*sql.DB, error) {
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA busy_timeout=5000", schema} {
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, e
		}
	}
	if e = migrateReadSessions(db); e != nil {
		db.Close()
		return nil, e
	}
	return db, nil
}
func (s *Server) stagePath(batch, object string) string {
	return filepath.Join(s.config.Dir, "staging", batch+"-"+object)
}
func (s *Server) packPath(id string) string { return filepath.Join(s.config.Dir, "packs", id+".dat") }
func (s *Server) cleanup() error {
	// Files are committed before their DB references. A crash before SQL commit
	// leaves only unreferenced files, never a committed reference to partial data.
	files, e := os.ReadDir(filepath.Join(s.config.Dir, "packs"))
	if e != nil {
		return e
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		var count int
		if filepath.Ext(name) == ".dat" {
			id := name[:len(name)-4]
			e = s.db.QueryRow("SELECT count(*) FROM packs WHERE id=? AND state!='remote'", id).Scan(&count)
			if e != nil {
				return e
			}
		}
		if count == 0 {
			if e = os.Remove(filepath.Join(s.config.Dir, "packs", name)); e != nil {
				return e
			}
		}
	}
	rows, e := s.db.Query("SELECT id FROM batches WHERE state='remote' OR state='aborted'")
	if e != nil {
		return e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		matches, _ := filepath.Glob(filepath.Join(s.config.Dir, "staging", id+"-*"))
		for _, p := range matches {
			os.Remove(p)
		}
	}
	_, e = s.db.Exec("UPDATE checkpoints SET state='failed',error='interrupted; create another checkpoint' WHERE state='running'")
	return e
}
func commit(tx *sql.Tx, err error) error {
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
func requireAffected(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return fmt.Errorf("record changed concurrently")
	}
	return nil
}

// Preserve existing read handles while removing the old singleton restriction.
// Old cloud checkpoints pass through this migration during restore as well.
func migrateReadSessions(db *sql.DB) error {
	var old int
	if e := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='lease'").Scan(&old); e != nil {
		return e
	}
	if old == 0 {
		return nil
	}
	tx, e := db.Begin()
	if e != nil {
		return e
	}
	_, e = tx.Exec("INSERT OR IGNORE INTO read_sessions(lease_id,space,pack,owner,url,expires) SELECT lease_id,space,pack,owner,url,expires FROM lease; DROP TABLE lease;")
	return commit(tx, e)
}
