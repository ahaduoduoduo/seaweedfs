package vault

import (
	"embed"
	"net/http"
	"path"
	"strings"
)

//go:embed assets/*
var assets embed.FS

func (s *Server) sdk(w http.ResponseWriter, r *http.Request) {
	name := path.Base(r.URL.Path)
	if !strings.HasSuffix(name, ".mjs") {
		http.NotFound(w, r)
		return
	}
	b, e := assets.ReadFile("assets/" + name)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Write(b)
}
func (s *Server) demo(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, _ := assets.ReadFile("assets/index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src http: https:; img-src 'self' blob:; media-src 'self' blob:; frame-ancestors 'none'")
	w.Write(b)
}
