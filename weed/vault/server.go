package vault

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/openlist_backend"
)

type properties map[string]string

func (p properties) GetString(k string) string { return p[k] }

type space struct {
	config              SpaceConfig
	token, gatewayToken string
	backend             backend.BackendStorage
}
type Server struct {
	config      Config
	db          *sql.DB
	spaces      map[string]*space
	lock        *flock.Flock
	client      *http.Client
	recovery    []byte
	adminToken  string
	started     int64
	mu          sync.Mutex
	work        sync.Mutex
	leaseMu     sync.Mutex
	maintenance atomic.Bool
	wake        chan struct{}
	lastError   atomic.Value
}

func Open(c Config) (*Server, error) {
	for _, name := range []string{"", "staging", "packs", "checkpoints"} {
		if e := os.MkdirAll(filepath.Join(c.Dir, name), 0700); e != nil {
			return nil, e
		}
	}
	lock := flock.New(filepath.Join(c.Dir, "vault.lock"))
	ok, e := lock.TryLock()
	if e != nil || !ok {
		return nil, fmt.Errorf("vault directory already in use: %v", e)
	}
	good := false
	defer func() {
		if !good {
			lock.Unlock()
		}
	}()
	db, e := openDB(filepath.Join(c.Dir, "vault.db"))
	if e != nil {
		return nil, e
	}
	s := &Server{started: time.Now().Unix(), config: c, db: db, spaces: map[string]*space{}, lock: lock, wake: make(chan struct{}, 1), client: &http.Client{Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	defer func() {
		if !good {
			db.Close()
		}
	}()
	s.recovery, e = recoveryKey(c.RecoveryKeyFile)
	if e != nil {
		return nil, e
	}
	s.adminToken, e = secret(c.AdminTokenFile, 32)
	if e != nil {
		return nil, e
	}
	for _, sc := range c.Spaces {
		t, e := secret(sc.TokenFile, 32)
		if e != nil {
			return nil, e
		}
		gt, e := secret(sc.GatewayTokenFile, 16)
		if e != nil {
			return nil, e
		}
		b, e := backend.BackendStorageFactories["openlist"].BuildStorage(properties{"endpoint": c.Gateway, "space": sc.Name, "token_file": sc.GatewayTokenFile, "max_object_mib": "64"}, "", sc.Name)
		if e != nil {
			return nil, e
		}
		s.spaces[sc.Name] = &space{config: sc, token: t, gatewayToken: gt, backend: b}
	}
	if e = s.cleanup(); e != nil {
		return nil, e
	}
	good = true
	return s, nil
}
func (s *Server) Close() error { e := s.db.Close(); return errors.Join(e, s.lock.Unlock()) }
func (s *Server) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mux := http.NewServeMux()
	s.routes(mux)
	server := &http.Server{Addr: s.config.Listen, Handler: s.middleware(mux), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	workers := make(chan struct{})
	go func() { defer close(workers); s.worker(ctx) }()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	log.Printf("vault API: %s", s.config.PublicURL)
	e := server.ListenAndServe()
	if e == http.ErrServerClosed {
		e = nil
	}
	cancel()
	<-workers
	return e
}
func (s *Server) routes(m *http.ServeMux) {
	m.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		reply(w, 200, map[string]any{"ok": true, "protocol": "vault/v1"})
	})
	m.HandleFunc("GET /v1/spaces/{space}/status", s.status)
	m.HandleFunc("POST /v1/spaces/{space}/flush", s.flush)
	m.HandleFunc("POST /v1/spaces/{space}/tokens", s.issueToken)
	m.HandleFunc("POST /v1/spaces/{space}/batches", s.createBatch)
	m.HandleFunc("GET /v1/spaces/{space}/batches/{batch}", s.getBatch)
	m.HandleFunc("PUT /v1/spaces/{space}/batches/{batch}/objects/{object}", s.upload)
	m.HandleFunc("POST /v1/spaces/{space}/batches/{batch}/commit", s.commitBatch)
	m.HandleFunc("DELETE /v1/spaces/{space}/batches/{batch}", s.abortBatch)
	m.HandleFunc("GET /v1/spaces/{space}/objects", s.listObjects)
	m.HandleFunc("GET /v1/spaces/{space}/objects/{object}", s.getObject)
	m.HandleFunc("DELETE /v1/spaces/{space}/objects/{object}", s.deleteObject)
	m.HandleFunc("GET /v1/spaces/{space}/objects/{object}/plan", s.readPlan)
	m.HandleFunc("GET /v1/spaces/{space}/objects/{object}/pending/{vid}", s.pendingBytes)
	m.HandleFunc("POST /v1/spaces/{space}/packs/{pack}/lease", s.acquireLease)
	m.HandleFunc("POST /v1/spaces/{space}/leases/{lease}/renew", s.renewLease)
	m.HandleFunc("POST /v1/spaces/{space}/leases/{lease}/complete", s.completeLease)
	m.HandleFunc("GET /v1/spaces/{space}/leases/{lease}/bytes", s.proxyBytes)
	m.HandleFunc("POST /v1/checkpoints", s.createCheckpoint)
	m.HandleFunc("GET /v1/checkpoints", s.listCheckpoints)
	m.HandleFunc("GET /v1/checkpoints/{checkpoint}", s.getCheckpoint)
	m.HandleFunc("GET /v1/checkpoints/{checkpoint}/ticket", s.getTicket)
	m.HandleFunc("GET /sdk/{asset}", s.sdk)
	m.HandleFunc("GET /", s.demo)
}
func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, msg string) {
	reply(w, status, map[string]string{"error": msg})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		fail(w, 400, "invalid JSON body")
		return false
	}
	return true
}
func (s *Server) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *Server) mutation(w http.ResponseWriter) bool {
	if s.maintenance.Load() {
		w.Header().Set("Retry-After", "3")
		fail(w, 503, "checkpoint in progress; retry mutation")
		return false
	}
	return true
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authorize(w, r, "read"); !ok {
		return
	}
	out := map[string]any{"chunk_bytes": ChunkSize, "pack_bytes": s.config.PackMiB << 20, "seal_seconds": s.config.SealSeconds, "maintenance": s.maintenance.Load()}
	for _, state := range []string{"accepted", "packed", "remote"} {
		var n int
		s.db.QueryRow("SELECT count(*) FROM batches WHERE space=? AND state=?", r.PathValue("space"), state).Scan(&n)
		out[state] = n
	}
	if e := s.lastError.Load(); e != nil {
		out["worker_error"] = e
	}
	reply(w, 200, out)
}
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if origin := r.Header.Get("Origin"); origin != "" {
			allowed := false
			for _, o := range s.config.Origins {
				if o == origin {
					allowed = true
				}
			}
			if origin == s.config.PublicURL {
				allowed = true
			}
			if !allowed {
				fail(w, 403, "origin not allowed")
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Upload-Offset, If-Match")
			w.Header().Set("Access-Control-Expose-Headers", "Upload-Offset, Retry-After, ETag, X-Vault-CDN-Busy")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			w.Header().Set("Pragma", "no-cache")
		}
		next.ServeHTTP(w, r)
	})
}
