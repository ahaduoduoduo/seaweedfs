# OpenList tier development

Updated: 2026-10-15

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
- [ ] Add retention-aware orphan and deleted-pack physical reclamation.
- [ ] Validate against a separately authorized staging 115 account before production deployment.
