package vault

import "database/sql"

// Automatic checkpoints cover complete remote revisions. An object with a
// newer local-only edit restores to its last remotely committed revision.
// The live catalog and staging files are never modified by this projection.
func cloudSnapshot(db *sql.DB, id string) error {
	_, e := db.Exec(`
DELETE FROM chunks WHERE vid IN (SELECT vid FROM versions WHERE state!='remote');
DELETE FROM versions WHERE state!='remote';
DELETE FROM objects WHERE NOT EXISTS(SELECT 1 FROM versions v WHERE v.space=objects.space AND v.object_id=objects.object_id);
UPDATE objects SET vid=(SELECT v.vid FROM versions v WHERE v.space=objects.space AND v.object_id=objects.object_id ORDER BY v.revision DESC LIMIT 1);
DELETE FROM uploads WHERE batch IN (SELECT id FROM batches WHERE state!='remote');
DELETE FROM batches WHERE state!='remote';
DELETE FROM packs WHERE state!='remote' OR NOT EXISTS(SELECT 1 FROM chunks WHERE chunks.pack=packs.id);
UPDATE versions SET checkpoint=?;
DELETE FROM read_sessions;
DELETE FROM checkpoints WHERE state!='complete';
PRAGMA wal_checkpoint(TRUNCATE)`, id)
	return e
}
