# DSM deployment

Updated: 2026-10-15.

The `openlist-tier.yml` workflow publishes `ghcr.io/ahaduoduoduo/seaweedfs-vault:sha-<commit>`
only after the native tier, multi-client and sparse-write scenarios pass. It packages
the same tested linux/amd64 binary, with Ubuntu 24.04 matching its CGO runtime.
Publishing an image does not deploy it automatically.

Use `/volume1/docker/seaweedfs` for the Compose project, `config/` for configuration
and secret files, and `data/` for SQLite, staging and encrypted recovery tickets.
The container runs as UID/GID 1001. Both directories must be readable by that user,
and `data/` writable. Configuration stays read-only in the container.

Start from [compose.yaml](../examples/vault/compose.yaml) and
[config.json](../examples/vault/config.json). Set `dir` to `/data`, use the existing
`autofilm` network, and point the gateway at `http://openlist:5244/seaweedfs/v1`.
Pin `VAULT_IMAGE` to the exact commit. Bind the HTTP port to the NAS LAN address;
browser use needs a trusted HTTPS reverse proxy for WebCrypto.

Create three dedicated 115 directories for `notes`, `photos` and `files` under one
Vault parent. Record their IDs once, then configure OpenList's exact-object gateway.
The running service never needs recurring remote directory listings.

Before updating OpenList, save its Compose environment, config and an SQLite backup.
Preserve the existing project, data bind mount, port, network and environment secrets.
Pull the immutable image before replacing only the OpenList service with `--no-deps`.
Keep the previous image/config available for rollback. Do not run two OpenList
instances against the same live database or 115 account.

Keep `seal_seconds: 0`, `pack_mib: 64`, and a 5-second minimum interval for new
download-link requests. Valid cached URLs are reusable and permit concurrent data
transfers. Manual business sync uploads small outstanding tails; a manual checkpoint
saves all committed data and the catalog together.

Back up the recovery key and a completed checkpoint ticket outside the NAS. Client
applications receive only their business token, never the OpenList gateway tokens
or global administrator token. The HTTP API and Node/Python examples are documented
in [vault.md](vault.md).
