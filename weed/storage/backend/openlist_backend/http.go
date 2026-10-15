package openlist_backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func (s *Storage) request(ctx context.Context, method, key string, body io.Reader) (*http.Request, error) {
	digest, _, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, s.endpoint+"/"+url.PathEscape(s.space)+"/objects/"+digest, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/octet-stream")
	return req, nil
}

func responseError(resp *http.Response) error {
	return fmt.Errorf("OpenList returned HTTP %d (Retry-After: %s)", resp.StatusCode, resp.Header.Get("Retry-After"))
}

// fetch follows a 302 explicitly: credentials are never sent to the CDN, even
// when the simulator or provider uses the same hostname as the gateway.
func (s *Storage) fetch(ctx context.Context, key string, offset, length, size int64) ([]byte, error) {
	refreshes, busyRetries := 0, 0
	for {
		data, err := s.fetchAttempt(ctx, key, offset, length, size, refreshes > 0)
		if errors.Is(err, errDownloadBusy) && busyRetries < 3 {
			busyRetries++
			timer := time.NewTimer(time.Duration(busyRetries*5) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if errors.Is(err, errExpiredLink) && refreshes < 2 {
			refreshes++
			continue
		}
		return data, err
	}
}

var errExpiredLink = errors.New("provider rejected cached download URL")
var errDownloadBusy = errors.New("provider download busy; cached URL remains valid")

// IsTransientDownloadBusy consumes only a small 403 error body. Real 115 can
// reject one overlapping transfer with this code while the same URL still works.
// It is neither evidence of expiry nor a reason to spend another link request.
func IsTransientDownloadBusy(response *http.Response) bool {
	if response.StatusCode != http.StatusForbidden {
		return false
	}
	var body struct {
		Message string `json:"message"`
	}
	return json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&body) == nil && body.Message == "115 pmt 3-2"
}

func (s *Storage) fetchAttempt(ctx context.Context, key string, offset, length, size int64, waitCooldown bool) ([]byte, error) {
	var location string
	var downloadUserAgent *string
	for {
		req, err := s.request(ctx, http.MethodGet, key, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		resp, err := s.client.Do(req)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests || (waitCooldown && resp.StatusCode == http.StatusServiceUnavailable && resp.Header.Get("Retry-After") != "") {
			seconds, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64)
			if err != nil || seconds < 1 || seconds > 3600 {
				return nil, responseError(resp)
			}
			timer := time.NewTimer(time.Duration(seconds) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode != http.StatusFound {
			return nil, responseError(resp)
		}
		location = resp.Header.Get("Location")
		if value := resp.Header.Get("X-SeaweedFS-Download-User-Agent"); value != "" {
			if err := json.Unmarshal([]byte(value), &downloadUserAgent); err != nil {
				return nil, fmt.Errorf("invalid gateway download user agent")
			}
		}
		break
	}
	u, err := url.Parse(location)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, fmt.Errorf("invalid OpenList redirect")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	if downloadUserAgent != nil {
		req.Header.Set("User-Agent", *downloadUserAgent)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if IsTransientDownloadBusy(resp) {
		return nil, errDownloadBusy
	}
	switch resp.StatusCode {
	case 401, 403, 404, 410:
		s.reportResult(ctx, key, location, resp.StatusCode)
		return nil, fmt.Errorf("%w (HTTP %d)", errExpiredLink, resp.StatusCode)
	case 405, 429:
		s.reportResult(ctx, key, location, resp.StatusCode)
		return nil, fmt.Errorf("OpenList CDN blocked download (HTTP %d); account cooldown requested", resp.StatusCode)
	}
	wantRange := fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, size)
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != wantRange {
		return nil, fmt.Errorf("OpenList CDN returned invalid range response (HTTP %d)", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, length+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, io.ErrUnexpectedEOF
	}
	s.reportResult(ctx, key, location, resp.StatusCode)
	return data, nil
}

func (s *Storage) reportResult(ctx context.Context, key, location string, status int) {
	data, _ := json.Marshal(map[string]any{"status": status, "url": location})
	body := bytes.NewReader(data)
	req, err := s.request(ctx, http.MethodPost, key, body)
	if err != nil {
		return
	}
	req.URL.Path += "/failure"
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}
