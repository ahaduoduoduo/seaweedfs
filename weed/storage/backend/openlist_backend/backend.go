// Package openlist_backend implements cloud tiering through OpenList's
// exact-object gateway. Remote directory enumeration is never required.
package openlist_backend

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

type factory struct{}

type Storage struct {
	properties map[string]string
	endpoint   string
	space      string
	token      string
	client     *http.Client
	timeout    time.Duration
	maxObject  int64
	window     int64
	cacheLimit int64
	mu         sync.Mutex
	cache      map[string]*list.Element
	lru        *list.List
	cacheBytes int64
}

func init()                                      { backend.BackendStorageFactories["openlist"] = factory{} }
func (factory) StorageType() backend.StorageType { return "openlist" }

func (factory) BuildStorage(config backend.StringProperties, prefix, id string) (backend.BackendStorage, error) {
	properties := map[string]string{}
	for _, name := range []string{"endpoint", "space", "token_file", "read_window_mib", "cache_mib", "max_object_mib", "timeout_seconds"} {
		properties[name] = config.GetString(prefix + name)
	}
	endpoint, err := url.Parse(strings.TrimRight(properties["endpoint"], "/"))
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("invalid OpenList endpoint")
	}
	space := properties["space"]
	if space == "" || strings.ContainsAny(space, "/\\?#%") {
		return nil, fmt.Errorf("invalid OpenList space")
	}
	token, err := os.ReadFile(properties["token_file"])
	if err != nil {
		return nil, fmt.Errorf("read OpenList token file: %w", err)
	}
	if len(strings.TrimSpace(string(token))) < 16 {
		return nil, fmt.Errorf("OpenList token must contain at least 16 characters")
	}
	readInt := func(name string, def, min, max int64) (int64, error) {
		if properties[name] == "" {
			return def, nil
		}
		n, err := strconv.ParseInt(properties[name], 10, 64)
		if err != nil || n < min || n > max {
			return 0, fmt.Errorf("invalid OpenList %s", name)
		}
		return n, nil
	}
	window, err := readInt("read_window_mib", 64, 1, 256)
	if err != nil {
		return nil, err
	}
	cache, err := readInt("cache_mib", 256, 1, 4096)
	if err != nil {
		return nil, err
	}
	maxObject, err := readInt("max_object_mib", 64, 1, 4096)
	if err != nil {
		return nil, err
	}
	timeout, err := readInt("timeout_seconds", 600, 1, 3600)
	if err != nil {
		return nil, err
	}
	if window > cache {
		return nil, fmt.Errorf("OpenList cache_mib must cover a read window")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 2
	return &Storage{properties: properties, endpoint: endpoint.String(), space: space, token: strings.TrimSpace(string(token)), timeout: time.Duration(timeout) * time.Second, maxObject: maxObject << 20, window: window << 20, cacheLimit: cache << 20, cache: map[string]*list.Element{}, lru: list.New(), client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (s *Storage) ToProperties() map[string]string { return s.properties }

func (s *Storage) NewStorageFile(key string, info *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	f := &File{storage: s, key: key}
	_, f.header, f.initErr = parseKey(key)
	for _, r := range info.GetFiles() {
		if r.Key == key {
			f.size = int64(r.FileSize)
			f.modified = time.Unix(int64(r.ModifiedTime), 0)
			break
		}
	}
	return f
}

func (s *Storage) CopyFile(f *os.File, progress func(int64, float32) error, _ int) (string, int64, error) {
	stat, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	if stat.Size() > s.maxObject {
		return "", 0, fmt.Errorf("volume is %d bytes, exceeds OpenList max_object_mib; use smaller volumes", stat.Size())
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, io.NewSectionReader(f, 0, stat.Size())); err != nil {
		return "", 0, err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	header := make([]byte, 8)
	if _, err := f.ReadAt(header, 0); err != nil {
		return "", 0, fmt.Errorf("read volume superblock: %w", err)
	}
	key := "v1." + digest + "." + hex.EncodeToString(header)
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	r := &progressReader{reader: io.NewSectionReader(f, 0, stat.Size()), size: stat.Size(), progress: progress}
	req, err := s.request(ctx, http.MethodPut, key, r)
	if err != nil {
		return "", 0, err
	}
	req.ContentLength = stat.Size()
	response, err := s.client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("OpenList upload: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return "", 0, responseError(response)
	}
	var receipt struct {
		Key   string `json:"key"`
		Size  int64  `json:"size"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&receipt); err != nil || receipt.Key != digest || receipt.Size != stat.Size() || receipt.State != "ready" {
		return "", 0, fmt.Errorf("OpenList returned an invalid upload receipt")
	}
	return key, stat.Size(), nil
}

func (s *Storage) DownloadFile(filename, key string, progress func(int64, float32) error, _ int) (int64, error) {
	digest, _, err := parseKey(key)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	req, err := s.request(ctx, http.MethodHead, key, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, responseError(resp)
	}
	size := resp.ContentLength
	if size < 0 || size > s.maxObject {
		return 0, fmt.Errorf("invalid OpenList object length")
	}
	f, err := os.CreateTemp(filepath.Dir(filename), ".openlist-download-*")
	if err != nil {
		return 0, err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(f.Name())
		}
	}()
	if size > 0 {
		data, err := s.fetch(ctx, key, 0, size, size)
		if err != nil {
			return 0, err
		}
		if _, err = f.Write(data); err != nil {
			return 0, err
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != digest {
			return 0, fmt.Errorf("OpenList download SHA-256 mismatch")
		}
	}
	if err = f.Sync(); err != nil {
		return 0, err
	}
	if err = f.Close(); err != nil {
		return 0, err
	}
	if progress != nil {
		if err = progress(size, 100); err != nil {
			return 0, err
		}
	}
	if err = os.Rename(f.Name(), filename); err != nil {
		return 0, err
	}
	ok = true
	return size, nil
}

func (s *Storage) DeleteFile(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	req, err := s.request(ctx, http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	response, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return responseError(response)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.cache {
		if strings.HasPrefix(k, key+":") {
			s.cacheBytes -= int64(len(e.Value.(*entry).data))
			s.lru.Remove(e)
			delete(s.cache, k)
		}
	}
	return nil
}

type progressReader struct {
	reader   io.Reader
	size, n  int64
	progress func(int64, float32) error
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.n += int64(n)
	if n > 0 && r.progress != nil {
		if e := r.progress(r.n, float32(r.n)*100/float32(max(1, r.size))); e != nil {
			return n, e
		}
	}
	return n, err
}
