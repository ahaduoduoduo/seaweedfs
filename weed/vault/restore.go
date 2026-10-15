package vault

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Restore requires an empty target directory and the original recovery key.
// Only authenticated ticket locators are imported; remote listing is unnecessary.
func Restore(ctx context.Context, c Config, ticketPath string) error {
	if _, e := os.Stat(filepath.Join(c.Dir, "vault.db")); !os.IsNotExist(e) {
		return fmt.Errorf("restore target already contains vault.db")
	}
	key, e := recoveryKey(c.RecoveryKeyFile)
	if e != nil {
		return e
	}
	f, e := os.Open(ticketPath)
	if e != nil {
		return e
	}
	b, e := io.ReadAll(io.LimitReader(f, 16<<20))
	f.Close()
	if e != nil {
		return e
	}
	var ticket Ticket
	if e = json.Unmarshal(b, &ticket); e != nil || !hmac.Equal(ticket.Signature, signature(key, ticket.Data)) {
		return fmt.Errorf("checkpoint ticket signature invalid")
	}
	var data TicketData
	if e = json.Unmarshal(ticket.Data, &data); e != nil || data.Protocol != "vault/checkpoint/v1" || !identifier.MatchString(data.ID) || len(data.Parts) == 0 {
		return fmt.Errorf("invalid checkpoint ticket")
	}
	// Hold the same process lock as the server throughout restoration.
	s, e := Open(c)
	if e != nil {
		return e
	}
	restoredOK := false
	defer func() {
		s.Close()
		if !restoredOK {
			for _, suffix := range []string{"", "-wal", "-shm"} {
				os.Remove(filepath.Join(c.Dir, "vault.db"+suffix))
			}
		}
	}()
	recoveryToken, e := secret(c.GatewayRecoveryTokenFile, 32)
	if e != nil {
		return e
	}
	dir, e := os.MkdirTemp(c.Dir, "restore-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	archive, e := os.OpenFile(filepath.Join(dir, "snapshot.tar.gz"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	for index, part := range data.Parts {
		if s.spaces[part.Receipt.Space] == nil || part.Size < 8 || part.Size > 64<<20 || part.Offset < 8 || part.Length < 28 || part.Length > part.Size || part.Offset > part.Size-part.Length {
			archive.Close()
			return fmt.Errorf("invalid checkpoint part")
		}
		if e = s.importReceipt(ctx, part.Receipt, recoveryToken); e != nil {
			archive.Close()
			return e
		}
		path := filepath.Join(dir, "part.dat")
		_, e = s.spaces[part.Receipt.Space].backend.DownloadFile(path, part.Key, nil, 0)
		if e != nil {
			archive.Close()
			return e
		}
		pf, e := os.Open(path)
		if e != nil {
			archive.Close()
			return e
		}
		ciphertext := make([]byte, part.Length)
		_, e = pf.ReadAt(ciphertext, part.Offset)
		pf.Close()
		if e != nil {
			archive.Close()
			return e
		}
		aad := []byte(fmt.Sprintf("vault/checkpoint/v1/%s/%d", data.ID, index))
		plain, e := decrypt(ciphertext, signature(key, aad), aad)
		if e == nil {
			_, e = archive.Write(plain)
		}
		if e != nil {
			archive.Close()
			return e
		}
		os.Remove(path)
	}
	if e = errors.Join(archive.Sync(), archive.Close()); e != nil {
		return e
	}
	if e = extractArchive(filepath.Join(dir, "snapshot.tar.gz"), dir); e != nil {
		return e
	}
	receipts, e := os.Open(filepath.Join(dir, "receipts.jsonl"))
	if e != nil {
		return e
	}
	scanner := bufio.NewScanner(receipts)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var receipt Receipt
		if e = json.Unmarshal(scanner.Bytes(), &receipt); e == nil {
			e = s.importReceipt(ctx, receipt, recoveryToken)
		}
		if e != nil {
			receipts.Close()
			return e
		}
	}
	e = errors.Join(scanner.Err(), receipts.Close())
	if e != nil {
		return e
	}
	restored, e := openDB(filepath.Join(dir, "vault.db"))
	if e != nil {
		return e
	}
	var integrity string
	e = restored.QueryRow("PRAGMA integrity_check").Scan(&integrity)
	if e == nil && integrity != "ok" {
		e = fmt.Errorf("checkpoint database integrity: %s", integrity)
	}
	if e == nil {
		var pending int
		e = restored.QueryRow("SELECT count(*) FROM packs WHERE state!='remote'").Scan(&pending)
		if e == nil && pending != 0 {
			e = fmt.Errorf("checkpoint contains uncommitted packs")
		}
	}
	e = errors.Join(e, restored.Close())
	if e != nil {
		return e
	}
	if e = s.db.Close(); e != nil {
		return e
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		os.Remove(filepath.Join(c.Dir, "vault.db"+suffix))
	}
	if e = os.Rename(filepath.Join(dir, "vault.db"), filepath.Join(c.Dir, "vault.db")); e != nil {
		return e
	}
	restoredOK = true
	return util.FsyncDir(c.Dir)
}
func (s *Server) importReceipt(ctx context.Context, receipt Receipt, token string) error {
	sp := s.spaces[receipt.Space]
	if sp == nil {
		return fmt.Errorf("checkpoint space %q missing from config", receipt.Space)
	}
	body, _ := json.Marshal(map[string]any{"version": 1, "space": receipt.Space, "objects": []json.RawMessage{receipt.Object}})
	req, e := http.NewRequestWithContext(ctx, "POST", s.config.Gateway+"/"+receipt.Space+"/inventory", bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+sp.gatewayToken)
	req.Header.Set("X-SeaweedFS-Recovery-Token", token)
	req.Header.Set("Content-Type", "application/json")
	res, e := s.client.Do(req)
	if e != nil {
		return e
	}
	res.Body.Close()
	if res.StatusCode != 201 {
		return fmt.Errorf("receipt import HTTP %d", res.StatusCode)
	}
	return nil
}
func extractArchive(path, dir string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		return e
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	seen := map[string]bool{}
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if (h.Name != "vault.db" && h.Name != "receipts.jsonl") || seen[h.Name] || h.Typeflag != tar.TypeReg || h.Size < 0 || h.Size > 64<<30 {
			return fmt.Errorf("unexpected checkpoint archive entry")
		}
		seen[h.Name] = true
		out, e := os.OpenFile(filepath.Join(dir, h.Name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = io.CopyN(out, tr, h.Size)
		e = errors.Join(e, out.Sync(), out.Close())
		if e != nil {
			return e
		}
	}
	if !seen["vault.db"] || !seen["receipts.jsonl"] {
		return fmt.Errorf("incomplete checkpoint archive")
	}
	return nil
}
