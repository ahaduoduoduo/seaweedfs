package openlist_backend

import (
	"context"
	"fmt"
	"io"
	"time"
)

type File struct {
	storage  *Storage
	key      string
	size     int64
	modified time.Time
	header   []byte
	initErr  error
}
type entry struct {
	key  string
	data []byte
}

func (f *File) ReadAt(p []byte, offset int64) (int, error) {
	if f.initErr != nil {
		return 0, f.initErr
	}
	if offset < 0 {
		return 0, fmt.Errorf("negative read offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if offset <= int64(len(f.header)) && int64(len(p)) <= int64(len(f.header))-offset {
		return copy(p, f.header[offset:]), nil
	}
	if offset >= f.size {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && offset+int64(n) < f.size {
		at := offset + int64(n)
		start := at / f.storage.window * f.storage.window
		data, err := f.storage.readWindow(f.key, start, min(f.storage.window, f.size-start), f.size)
		if err != nil {
			return n, err
		}
		n += copy(p[n:], data[at-start:])
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Coalesce only the same window. Different packs can download concurrently;
// OpenList limits new URL resolutions independently of these transfers.
func (s *Storage) readWindow(key string, start, length, size int64) ([]byte, error) {
	cacheKey := fmt.Sprintf("%s:%d", key, start)
	if data := s.cachedWindow(cacheKey); data != nil {
		return data, nil
	}
	result, err, _ := s.pending.Do(cacheKey, func() (any, error) {
		if data := s.cachedWindow(cacheKey); data != nil {
			return data, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel()
		data, err := s.fetch(ctx, key, start, length, size)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		for s.cacheBytes+int64(len(data)) > s.cacheLimit {
			e := s.lru.Back()
			if e == nil {
				break
			}
			item := e.Value.(*entry)
			delete(s.cache, item.key)
			s.cacheBytes -= int64(len(item.data))
			s.lru.Remove(e)
		}
		s.cache[cacheKey] = s.lru.PushFront(&entry{key: cacheKey, data: data})
		s.cacheBytes += int64(len(data))
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]byte), nil
}
func (s *Storage) cachedWindow(key string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.cache[key]; e != nil {
		s.lru.MoveToFront(e)
		return e.Value.(*entry).data
	}
	return nil
}

func (f *File) GetStat() (int64, time.Time, error) { return f.size, f.modified, nil }
func (f *File) Name() string                       { return "openlist:" + f.storage.space + "/" + f.key }
func (f *File) Close() error                       { return nil }
func (f *File) Sync() error                        { return nil }
func (f *File) WriteAt([]byte, int64) (int, error) {
	return 0, fmt.Errorf("OpenList tier files are immutable")
}
func (f *File) Truncate(int64) error { return fmt.Errorf("OpenList tier files are immutable") }
