# OpenList tier development

Updated: 2026-10-16

- [x] Fork SeaweedFS and add a Go OpenList exact-object storage backend.
- [x] Add content-addressed upload receipts, bounded range cache and CDN failure reporting.
- [x] Persist volume headers locally to avoid cloud reads during restart.
- [x] Keep local data and retry safely when remote upload or local .vif commit fails.
- [x] Run real SeaweedFS against the OpenList gateway and a loopback 115 simulator.
- [x] Restrict this fork's GitHub validation/builds to linux/amd64.
- [x] Add business-level attachment transactions and remote durability status.
- [x] Add explicit relation/time-aware placement and automatic sealing.
- [x] Add client read plans and direct-download/decryption SDK.
- [x] Add coordinated encrypted checkpoints and restore drills from cloud-only metadata.
- [x] Permit concurrent cached-URL downloads, recover unknown URL expiry with bounded refreshes, and migrate old singleton sessions.
- [x] 2026-10-15: Accumulate sparse writes until packs fill, add per-business manual sync, and keep automatic checkpoints from sealing local tails.
- [x] 2026-10-16: Preserve 115 download signature User-Agent requirements in native clients, browser ciphertext proxy and cloud restore.
- [x] 2026-10-16: Retry explicit transient CDN busy responses with the same cached URL and bounded waits across native, browser and restore clients.
- [ ] Add retention-aware orphan and deleted-pack physical reclamation.
- [x] 2026-10-16: Deploy immutable linux/amd64 images on the authorized DSM and validate real 115 uploads, concurrent native/browser reads, cached-link reuse, 1080p playback and checkpoint restore.
