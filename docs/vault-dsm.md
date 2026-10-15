# DSM deployment

Updated: 2026-10-16.

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

## Connecting applications

The browser entry point is `/`; it accepts a business name and that business token.
This page supports browsing metadata, uploading, manual cloud sync and small-file
previews. It is an integration console, not a full account/configuration dashboard.

Native apps and services use the language-independent `/v1/spaces/{space}` HTTP
API. Node and Python reference clients handle upload transactions, read plans,
authenticated decryption, cached URLs and bounded retry behavior. For the CLI:

```bash
export VAULT_URL=https://your-lan-vault.example.com
export VAULT_SPACE=notes
export VAULT_TOKEN_FILE=/private/notes.token
node examples/vault/cli.mjs list
node examples/vault/cli.mjs put article-001 article.json note-001
node examples/vault/cli.mjs sync
node examples/vault/cli.mjs get article-001 downloaded.json
```

An independently hosted web app also needs its HTTPS origin in `origins`; restart
the Vault container after changing the configuration. Issue scoped, expiring tokens
from the application's backend instead of embedding business-owner tokens in a
public frontend. New businesses need a dedicated 115 directory and matching space
entries in both Vault and OpenList. Existing data is unaffected by adding a space.

The tested 115 link signature requires an empty User-Agent. Native clients send it
exactly and download directly from 115. Browser JavaScript cannot set that header,
so `transport: "auto"` uses the NAS ciphertext proxy and decrypts in the browser.
This constraint is carried in each lease and does not apply unconditionally to
other providers. See the protocol document for the optional header contract.

## Real DSM / 115 acceptance — 2026-10-16

The authorized DSM deployment used the modified OpenList 115 driver, real cloud
storage, and a LAN HTTPS endpoint. Six producers submitted 114 objects in 90
batches across notes, photos and files: JSON with related JPEG attachments, a
64-photo album, a 1080p MP4 and a 512 MiB incompressible file. All 740.3 MiB reached
115 in 14 encrypted packs; the browser then added and manually synced one note.

| Measurement | Result |
| --- | --- |
| Approximately 60 MiB pack upload, including provider operations | 9–12 seconds |
| Single cached CDN pack download | 60.5 MiB in 2.63 seconds (23.0 MiB/s) |
| Four different cached URLs downloaded concurrently | 58.9 MiB/s aggregate; zero new link requests |
| Full 512 MiB CLI download and authenticated decryption | 52.4 seconds; SHA-256 matched |
| First album photo with empty local pack cache | 2.63 seconds |
| Seven neighboring photos from the cached pack | 20–26 ms each; zero cloud transfers |
| LAN metadata / list / read-plan median | 23 / 32 / 25 ms |
| A 256-byte read crossing a 60 MiB segment boundary | 8.24 seconds; two complete packs fetched |
| New encrypted checkpoint | 4.12 seconds |
| Restore checkpoint catalog into a new empty directory | 0.76 seconds; 115 objects, 91 batches, SQLite integrity OK |

The full-file result includes paced first-time link acquisition and concurrent
Python/browser activity. The four-URL result measures aggregate cached downloads,
not a single-file speed. Whole-upload wall time was interrupted while correcting
provider responses, so per-pack receipt durations are reported instead.

Browser checks covered desktop and 390-pixel mobile layouts, note/image previews,
two tabs sharing one transfer, upload/manual sync, and SDK decoding/seeking/playing
the 1080p MP4. Page errors were empty. The restored catalog's note was downloaded
from real 115, decrypted and checked against the original hash. Checkpoint tickets
and recovery configuration were saved outside the NAS.

Real-provider evidence is kept locally in `build/vault-dsm-results/`: JSON reports
and desktop/mobile screenshots. Regenerate fixtures and use a new prefix for a
new run; credentials belong in a private config file, never in the repository:

```bash
python3 test/vault/real_115.py --config /private/vault-client.json \
  --output build/vault-dsm-results --prefix unique-run-id --phase all
```

This explicit acceptance command writes to the configured spaces and waits for
remote confirmation. It does not list provider directories or remove cloud data.
