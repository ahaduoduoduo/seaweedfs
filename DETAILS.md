# OpenList fork module map

Updated: 2026-10-15

- `weed/storage/backend/openlist_backend/backend.go`: configuration, upload,
  download and logical removal; Token contents stay out of distributed config.
- `http.go`: exact-object gateway protocol, explicit 302, Range validation,
  paced URL refresh and CDN failure reporting.
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

## Vault integration

- `weed/command/vault.go`: service and cloud restore command entry points.
- `weed/vault/config.go`, `server.go`, `auth.go`: configuration, HTTP lifecycle, per-space tokens and CORS.
- `database.go`, `batches.go`, `objects.go`: authoritative catalog, resumable uploads, atomic revisions and read plans.
- `packing.go`, `worker.go`: AES-GCM needle packs, relation ordering, capacity-driven sealing, resumable full-pack prefixes and serial remote upload.
- `sealing.go`: business-level packing eligibility and durable manual sync requests.
- `checkpoint_snapshot.go`: project complete cloud revisions into a recoverable catalog without flushing local edits.
- `leases.go`: independent persistent read sessions and optional ciphertext proxy.
- `checkpoints.go`, `restore.go`: coordinated encrypted catalog/receipt checkpoints and offline restoration.
- `weed/vault/assets/`: embedded browser demo and bounded streaming WebCrypto SDK.
- `examples/vault/`: configuration, local launcher, Node CLI and independent Python protocol example.
- `docker/Dockerfile.vault`, `examples/vault/compose.yaml`: non-root amd64 runtime image and persistent DSM service.
- [DSM deployment and real-provider verification](docs/vault-dsm.md).
- `test/vault/`: multi-business large-object integration, browser interactions and failure scenarios.
- [Language-independent API, deployment and recovery](docs/vault.md).
