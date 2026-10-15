import { SHA256, hashBlob } from "./sha256.mjs";
import { cached, cache, exclusive, clearCache } from "./cache.mjs";
export { hashBlob, clearCache };
const chunkSize = 4 * 1024 * 1024;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const from64 = (text) => Uint8Array.from(atob(text), (c) => c.charCodeAt(0));
export class VaultError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}
export class VaultClient {
  constructor({
    url,
    space,
    token,
    transport = "auto",
    onEvent = () => {},
    maxWaitMs = 600000,
  }) {
    this.url = url.replace(/\/$/, "");
    this.space = space;
    this.token = token;
    this.transport = transport;
    this.onEvent = onEvent;
    this.maxWaitMs = maxWaitMs;
    this.base = "/v1/spaces/" + encodeURIComponent(space);
  }
  async request(
    path,
    {
      method = "GET",
      body,
      headers = {},
      raw = false,
      retry = true,
      waitCooldown = false,
    } = {},
  ) {
    const start = Date.now();
    while (true) {
      const response = await fetch(this.url + path, {
        method,
        headers: {
          Authorization: "Bearer " + this.token,
          ...(body !== undefined &&
          !(body instanceof Blob) &&
          !(body instanceof Uint8Array)
            ? { "Content-Type": "application/json" }
            : {}),
          ...headers,
        },
        body:
          body === undefined
            ? undefined
            : body instanceof Blob || body instanceof Uint8Array
              ? body
              : JSON.stringify(body),
      });
      if (
        (response.status === 429 ||
          (response.status === 503 &&
            Number(response.headers.get("Retry-After")) > 0 &&
            (waitCooldown ||
              Number(response.headers.get("Retry-After")) <= 5))) &&
        retry &&
        Date.now() -
          start +
          Math.max(1, Number(response.headers.get("Retry-After")) || 1) * 1000 <
          this.maxWaitMs
      ) {
        const seconds = Math.max(
          1,
          Number(response.headers.get("Retry-After")) || 1,
        );
        await response.body?.cancel();
        await sleep(seconds * 1000);
        continue;
      }
      if (!response.ok) {
        let detail;
        try {
          detail = (await response.json()).error;
        } catch {}
        throw new VaultError(
          response.status,
          detail || "HTTP " + response.status,
        );
      }
      if (raw) return response;
      return response.status === 204 ? null : response.json();
    }
  }
  async putBatch({
    objects,
    group = "",
    sortKey = "",
    idempotencyKey = crypto.randomUUID(),
  }) {
    const manifest = [];
    for (const object of objects) {
      this.onEvent({ type: "hashing", id: object.id });
      manifest.push({
        id: object.id,
        size: object.blob.size,
        sha256: object.sha256 || (await hashBlob(object.blob)),
        content_type:
          object.contentType || object.blob.type || "application/octet-stream",
        if_version: object.ifVersion || 0,
        metadata: object.metadata || {},
      });
    }
    const batch = await this.request(this.base + "/batches", {
      method: "POST",
      body: {
        idempotency_key: idempotencyKey,
        group,
        sort_key: sortKey,
        objects: manifest,
      },
    });
    let state = await this.batch(batch.id);
    if (state.state !== "uploading") return state;
    for (const object of objects) {
      let offset = state.objects.find((x) => x.id === object.id).uploaded,
        failures = 0;
      while (offset < object.blob.size) {
        const slice = object.blob.slice(offset, offset + chunkSize);
        try {
          const result = await this.request(
            this.base +
              "/batches/" +
              batch.id +
              "/objects/" +
              encodeURIComponent(object.id),
            {
              method: "PUT",
              body: slice,
              headers: { "Upload-Offset": String(offset) },
              raw: true,
            },
          );
          offset = Number(result.headers.get("Upload-Offset"));
          failures = 0;
          this.onEvent({
            type: "upload",
            id: object.id,
            offset,
            size: object.blob.size,
          });
        } catch (error) {
          if (++failures > 3 || (error.status && error.status !== 409))
            throw error;
          state = await this.batch(batch.id);
          offset = state.objects.find((x) => x.id === object.id).uploaded;
        }
      }
    }
    await this.request(this.base + "/batches/" + batch.id + "/commit", {
      method: "POST",
    });
    return this.batch(batch.id);
  }
  batch(id) {
    return this.request(this.base + "/batches/" + id);
  }
  flush() {
    return this.request(this.base + "/flush", { method: "POST" });
  }
  async waitRemote(id) {
    const start = Date.now();
    while (Date.now() - start < this.maxWaitMs) {
      const result = await this.batch(id);
      if (result.state === "remote") return result;
      if (result.state === "aborted") throw new Error("batch aborted");
      await sleep(1000);
    }
    throw new Error(
      "remote upload still pending; inspect batch/status and retry later",
    );
  }
  list(after = "") {
    return this.request(
      this.base + "/objects?after=" + encodeURIComponent(after),
    );
  }
  object(id) {
    return this.request(this.base + "/objects/" + encodeURIComponent(id));
  }
  plan(id, version = "") {
    return this.request(
      this.base +
        "/objects/" +
        encodeURIComponent(id) +
        "/plan" +
        (version ? "?version=" + encodeURIComponent(version) : ""),
    );
  }
  async pack(pack) {
    return exclusive(pack.sha256, async () => {
      const hit = await cached(pack.sha256);
      if (hit) {
        this.onEvent({ type: "cache-hit", pack: pack.id });
        return new Uint8Array(hit);
      }
      let proxy = this.transport === "proxy";
      let refreshes = 0;
      while (true) {
        const lease = await this.request(
          this.base + "/packs/" + pack.id + "/lease",
          { method: "POST", waitCooldown: refreshes > 0 },
        );
        const controller = new AbortController();
        let status = 0;
        const timer = setInterval(
          () =>
            this.request(this.base + "/leases/" + lease.id + "/renew", {
              method: "POST",
              retry: false,
            }).catch(() => controller.abort()),
          lease.renew_after_seconds * 1000,
        );
        let bytes;
        let fallback = false;
        try {
          const constrainedUA = typeof lease.download_user_agent === "string";
          const browser = typeof navigator !== "undefined" && !globalThis.process?.versions?.node;
          if (lease.url && constrainedUA && browser && !proxy) {
            if (this.transport === "direct")
              throw new Error("Browser cannot set the required download User-Agent; use auto or proxy transport");
            proxy = true;
            this.onEvent({ type: "header-fallback", pack: pack.id });
          }
          const direct = !!lease.url && !proxy;
          this.onEvent({
            type: "download",
            pack: pack.id,
            transport: direct ? "direct" : "proxy",
          });
          let response;
          try {
            response = await fetch(
              direct ? lease.url : this.url + lease.proxy_url,
              {
                headers: direct
                  ? { Range: `bytes=0-${pack.size - 1}`, ...(constrainedUA ? { "User-Agent": lease.download_user_agent } : {}) }
                  : { Authorization: "Bearer " + this.token },
                signal: controller.signal,
              },
            );
          } catch (error) {
            if (
              direct &&
              this.transport === "auto" &&
              error instanceof TypeError
            ) {
              fallback = true;
            } else throw error;
          }
          if (!fallback) {
            status = response.status;
            // The pack may have moved from local staging to the cloud after
            // this lease was issued. Release it and resolve the same pack again.
            if (!direct && status === 409) {
              await response.body?.cancel();
              continue;
            }
            // Expiry is discovered by using the URL, not by guessing a TTL.
            // Completing this session invalidates exactly its old URL. The
            // next acquisition waits for the shared API budget/cooldown.
            if ([401, 403, 404, 410].includes(status) && refreshes++ < 2) {
              await response.body?.cancel();
              this.onEvent({ type: "link-refresh", pack: pack.id });
              continue;
            }
            if (!response.ok)
              throw new VaultError(status, "pack download failed");
            if (
              direct &&
              (status !== 206 ||
                response.headers.get("Content-Range") !==
                  `bytes 0-${pack.size - 1}/${pack.size}`)
            )
              throw new Error("invalid cloud range response");
            bytes = new Uint8Array(await response.arrayBuffer());
            if (bytes.byteLength !== pack.size)
              throw new Error("pack length mismatch");
            const sha = new SHA256().update(bytes).hex();
            if (sha !== pack.sha256) throw new Error("pack checksum mismatch");
          }
        } finally {
          clearInterval(timer);
          await this.request(this.base + "/leases/" + lease.id + "/complete", {
            method: "POST",
            body: { status },
            retry: false,
          });
        }
        if (fallback) {
          proxy = true;
          this.onEvent({ type: "cors-fallback", pack: pack.id });
          continue;
        }
        await cache(pack.sha256, bytes.buffer);
        return bytes;
      }
    });
  }
  async *stream(id, { start = 0, end, version = "" } = {}) {
    let plan = await this.plan(id, version);
    const size = plan.object.size;
    end = end ?? size;
    if (start < 0 || end < start || end > size)
      throw new RangeError("invalid plaintext range");
    if (start === end) return;
    if (plan.pending_url) {
      try {
        const response = await this.request(plan.pending_url, {
          headers: { Range: `bytes=${start}-${end - 1}` },
          raw: true,
        });
        if (start === end) {
          await response.body?.cancel();
          return;
        }
        for await (const part of response.body) yield part;
        return;
      } catch (error) {
        if (error.status !== 409) throw error;
        plan = await this.plan(id, plan.object.version);
      }
    }
    const full = start === 0 && end === size,
      hash = full ? new SHA256() : null;
    for (const chunk of plan.chunks) {
      if (
        chunk.plain_offset + chunk.plain_size <= start ||
        chunk.plain_offset >= end
      )
        continue;
      const pack = await this.pack(plan.packs[chunk.pack]);
      const encrypted = pack.subarray(
        chunk.cipher_offset,
        chunk.cipher_offset + chunk.cipher_size,
      );
      const key = await crypto.subtle.importKey(
        "raw",
        from64(chunk.key),
        "AES-GCM",
        false,
        ["decrypt"],
      );
      const plain = new Uint8Array(
        await crypto.subtle.decrypt(
          {
            name: "AES-GCM",
            iv: encrypted.subarray(0, 12),
            additionalData: from64(chunk.aad),
            tagLength: 128,
          },
          key,
          encrypted.subarray(12),
        ),
      );
      if (plain.length !== chunk.plain_size)
        throw new Error("plaintext length mismatch");
      hash?.update(plain);
      yield plain.subarray(
        Math.max(0, start - chunk.plain_offset),
        Math.min(plain.length, end - chunk.plain_offset),
      );
    }
    if (hash && hash.hex() !== plan.object.sha256)
      throw new Error("object checksum mismatch");
  }
  async read(id, options = {}) {
    const parts = [];
    let size = 0;
    for await (const part of this.stream(id, options)) {
      parts.push(part);
      size += part.length;
    }
    const out = new Uint8Array(size);
    let offset = 0;
    for (const part of parts) {
      out.set(part, offset);
      offset += part.length;
    }
    return out;
  }
}
