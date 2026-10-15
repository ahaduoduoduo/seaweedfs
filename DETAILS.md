# OpenList fork module map

Updated: 2026-10-15

- `weed/storage/backend/openlist_backend/backend.go`: configuration, upload,
  download and logical removal; Token contents stay out of distributed config.
- `http.go`: exact-object gateway protocol, explicit 302, Range validation,
  pacing responses and CDN failure reporting.
- `file.go`: immutable ReaderAt implementation and bounded shared LRU cache.
- `key.go`: versioned volume key with local superblock metadata.
- `weed/storage/backend/all/all.go`: backend registration.
- `weed/storage/volume_tier.go`: retryable atomic tier metadata commitment.
- `weed/server/volume_grpc_tier_upload.go`: tier upload ordering and OpenList
  read-only/sync requirements.
- `test/openlist/integration.py`: actual process integration and fault injection,
  paired with OpenList's `cmd/seaweedfs-lab`.
- `.github/workflows/openlist-tier.yml`: linux/amd64 integration/build artifacts.
- `.github/upstream-workflows/`: inactive upstream workflows retained for comparison.
- [Configuration, protocol and limitations](docs/openlist-tier.md).
