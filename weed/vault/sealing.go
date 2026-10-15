package vault

import (
	"errors"
	"net/http"
	"time"
)

// Groups are locality hints inside one business, not separate tiny-pack queues.
// Eligibility is tested in SQL so an old small business cannot stall a full one.
func (s *Server) packingPlan(force bool) (string, []string, bool, error) {
	var space string
	var oldest, requested, remaining int64
	e := s.db.QueryRow(`SELECT b.space,min(v.created),max(b.flush_requested),
sum(u.size-(SELECT coalesce(sum(c.plain_size),0) FROM chunks c WHERE c.vid=u.vid)) AS remaining
FROM batches b JOIN uploads u ON u.batch=b.id JOIN versions v ON v.vid=u.vid
WHERE b.state='accepted' GROUP BY b.space
HAVING ? OR max(b.flush_requested)>0 OR remaining>=?
OR remaining=0 OR (? > 0 AND min(v.created)<=?)
ORDER BY min(v.created),b.space LIMIT 1`, force, (s.config.PackMiB<<20)-ChunkSize, s.config.SealSeconds, time.Now().Unix()-s.config.SealSeconds).Scan(&space, &oldest, &requested, &remaining)
	if e != nil {
		return "", nil, false, e
	}
	sealTail := force || requested > 0 || (s.config.SealSeconds > 0 && time.Now().Unix()-oldest >= s.config.SealSeconds)
	rows, e := s.db.Query("SELECT id FROM batches WHERE state='accepted' AND space=? ORDER BY group_key,sort_key,created,id", space)
	if e != nil {
		return "", nil, false, e
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
	return space, ids, sealTail, e
}

// A manual request marks the current accepted batches durably; it does not
// perform cloud work in the HTTP request or force another business to seal.
func (s *Server) flush(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "write"); !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mutation(w) {
		return
	}
	rows, e := s.db.Query("SELECT id FROM batches WHERE space=? AND state IN ('accepted','packed') ORDER BY created,id", r.PathValue("space"))
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
	if e == nil {
		_, e = s.db.Exec("UPDATE batches SET flush_requested=1 WHERE space=? AND state='accepted'", r.PathValue("space"))
	}
	if e != nil {
		fail(w, 500, e.Error())
		return
	}
	s.signal()
	reply(w, http.StatusAccepted, map[string]any{"batches": ids})
}
