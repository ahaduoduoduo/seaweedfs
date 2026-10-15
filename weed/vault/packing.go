package vault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func digest(key string) string {
	p := strings.Split(key, ".")
	if len(p) == 3 {
		return p[1]
	}
	return ""
}
func hashFile(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	_, e = io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), e
}
func encrypt(data, key, aad []byte) ([]byte, error) {
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	g, e := cipher.NewGCM(b)
	if e != nil {
		return nil, e
	}
	nonce := make([]byte, g.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return nil, e
	}
	return g.Seal(nonce, nonce, data, aad), nil
}
func decrypt(data, key, aad []byte) ([]byte, error) {
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	g, e := cipher.NewGCM(b)
	if e != nil {
		return nil, e
	}
	if len(data) < g.NonceSize()+g.Overhead() {
		return nil, io.ErrUnexpectedEOF
	}
	return g.Open(nil, data[:g.NonceSize()], data[g.NonceSize():], aad)
}

type packWriter struct {
	id     string
	f      *os.File
	size   int64
	count  uint64
	closed bool
	sha    string
}

func newPack(dir string) (*packWriter, error) {
	p := &packWriter{id: randomID(), size: 8}
	f, e := os.OpenFile(filepath.Join(dir, p.id+".dat"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	p.f = f
	sb := super_block.SuperBlock{Version: needle.Version3, ReplicaPlacement: &super_block.ReplicaPlacement{}, Ttl: &needle.TTL{}}
	_, e = f.Write(sb.Bytes())
	if e != nil {
		f.Close()
		return nil, e
	}
	return p, nil
}
func (p *packWriter) append(data []byte) (int64, error) {
	p.count++
	n := needle.Needle{Id: types.NeedleId(p.count), Cookie: 1, Data: data, Checksum: needle.NewCRC(data), AppendAtNs: uint64(time.Now().UnixNano())}
	off, _, actual, e := n.Append(backend.NewDiskFile(p.f), needle.Version3)
	if e != nil {
		return 0, e
	}
	p.size = int64(off) + actual
	return int64(off) + 20, nil
}
func (p *packWriter) close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	e := errors.Join(p.f.Sync(), p.f.Close())
	if e != nil {
		return e
	}
	p.sha, e = hashFile(p.f.Name())
	return e
}

type packedChunk struct {
	vid string
	Chunk
}

// A whole relation batch stays in one pack whenever it fits. Oversized batches
// use consecutive packs. Group + sort_key clusters neighboring album entries.
func (s *Server) packGroup(space, group string, ids []string) error {
	var writers []*packWriter
	var chunks []packedChunk
	var current *packWriter
	good := false
	defer func() {
		if !good {
			for _, p := range writers {
				p.f.Close()
				os.Remove(s.packPath(p.id))
			}
		}
	}()
	start := func() error {
		if current != nil {
			if e := current.close(); e != nil {
				return e
			}
		}
		p, e := newPack(filepath.Join(s.config.Dir, "packs"))
		if e == nil {
			writers = append(writers, p)
			current = p
		}
		return e
	}
	cap := s.config.PackMiB << 20
	for _, id := range ids {
		items, e := s.batchUploads(id)
		if e != nil {
			return e
		}
		var estimated int64
		for _, u := range items {
			estimated += u.Size + ((u.Size+ChunkSize-1)/ChunkSize)*64
		}
		if current != nil && estimated <= cap-8 && current.size+estimated > cap {
			if e := current.close(); e != nil {
				return e
			}
			current = nil
		}
		for _, u := range items {
			if u.Size == 0 {
				continue
			}
			f, e := os.Open(s.stagePath(id, u.ID))
			if e != nil {
				return e
			}
			for off, idx := int64(0), 0; off < u.Size; idx++ {
				length := min(int64(ChunkSize), u.Size-off)
				data := make([]byte, length)
				_, e = io.ReadFull(f, data)
				if e != nil {
					f.Close()
					return e
				}
				key := make([]byte, 32)
				if _, e = rand.Read(key); e != nil {
					f.Close()
					return e
				}
				aad := []byte(fmt.Sprintf("vault/v1/%s/%s/%d/%d", space, u.VID, off, length))
				ciphertext, e := encrypt(data, key, aad)
				if e != nil {
					f.Close()
					return e
				}
				if current == nil || current.size+int64(len(ciphertext))+40 > cap {
					if e = start(); e != nil {
						f.Close()
						return e
					}
				}
				cipherOffset, e := current.append(ciphertext)
				if e != nil {
					f.Close()
					return e
				}
				chunks = append(chunks, packedChunk{u.VID, Chunk{idx, current.id, off, length, cipherOffset, int64(len(ciphertext)), base64.StdEncoding.EncodeToString(key), base64.StdEncoding.EncodeToString(aad)}})
				off += length
			}
			f.Close()
		}
	}
	for _, p := range writers {
		if p.size > cap {
			return fmt.Errorf("pack exceeds configured maximum")
		}
		if e := p.close(); e != nil {
			return e
		}
	}
	if e := util.FsyncDir(filepath.Join(s.config.Dir, "packs")); e != nil {
		return e
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, p := range writers {
		_, e = tx.Exec("INSERT INTO packs(id,space,size,sha,state,created) VALUES(?,?,?,?,'sealed',?)", p.id, space, p.size, p.sha, time.Now().Unix())
		if e != nil {
			return e
		}
	}
	for _, c := range chunks {
		_, e = tx.Exec("INSERT INTO chunks(vid,idx,pack,plain_offset,plain_size,cipher_offset,cipher_size,key,aad) VALUES(?,?,?,?,?,?,?,?,?)", c.vid, c.Index, c.Pack, c.PlainOffset, c.PlainSize, c.CipherOffset, c.CipherSize, c.Key, c.AAD)
		if e != nil {
			return e
		}
	}
	for _, id := range ids {
		_, e = tx.Exec("UPDATE versions SET state='packed' WHERE batch=?", id)
		if e == nil {
			_, e = tx.Exec("UPDATE batches SET state='packed' WHERE id=?", id)
		}
		if e != nil {
			return e
		}
	}
	e = tx.Commit()
	good = e == nil
	return e
}
func (s *Server) removeStages(id string) {
	matches, _ := filepath.Glob(filepath.Join(s.config.Dir, "staging", id+"-*"))
	for _, p := range matches {
		os.Remove(p)
	}
}
