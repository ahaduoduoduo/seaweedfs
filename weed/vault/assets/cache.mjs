const memory = new Map();
let memoryBytes = 0;
const maxBytes = 256 * 1024 * 1024;
let database;
function db() {
  if (!globalThis.indexedDB) return null;
  return (database ??= new Promise((resolve, reject) => {
    const q = indexedDB.open("seaweed-vault-ciphertext-v1", 1);
    q.onupgradeneeded = () =>
      q.result
        .createObjectStore("packs", { keyPath: "id" })
        .createIndex("time", "time");
    q.onsuccess = () => resolve(q.result);
    q.onerror = () => reject(q.error);
  }));
}
async function operation(mode, fn) {
  const database = await db();
  if (!database) return;
  return new Promise((resolve, reject) => {
    const tx = database.transaction("packs", mode);
    const req = fn(tx.objectStore("packs"));
    tx.oncomplete = () => resolve(req?.result);
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}
export async function cached(id) {
  const entry = memory.get(id);
  if (entry) {
    memory.delete(id);
    memory.set(id, entry);
    return entry;
  }
  try {
    const row = await operation("readonly", (s) => s.get(id));
    if (row) {
      remember(id, row.bytes);
      return row.bytes;
    }
  } catch {}
  return null;
}
function remember(id, bytes) {
  if (memory.has(id)) memoryBytes -= memory.get(id).byteLength;
  memory.delete(id);
  memory.set(id, bytes);
  memoryBytes += bytes.byteLength;
  while (memoryBytes > maxBytes) {
    const first = memory.keys().next().value;
    memoryBytes -= memory.get(first).byteLength;
    memory.delete(first);
  }
}
export async function cache(id, bytes) {
  remember(id, bytes);
  try {
    await operation("readwrite", (s) =>
      s.put({ id, bytes, time: Date.now(), size: bytes.byteLength }),
    );
    const database = await db();
    if (!database) return;
    await new Promise((resolve, reject) => {
      const tx = database.transaction("packs", "readwrite"),
        store = tx.objectStore("packs");
      let total = 0;
      const q = store.index("time").openCursor(null, "prev");
      q.onsuccess = () => {
        const cursor = q.result;
        if (!cursor) return;
        total += cursor.value.size;
        if (total > maxBytes) cursor.delete();
        cursor.continue();
      };
      tx.oncomplete = resolve;
      tx.onerror = () => reject(tx.error);
    });
  } catch {
    /* Private mode/quota: bounded memory cache remains available. */
  }
}
const pending = new Map();
export async function exclusive(id, fn) {
  if (globalThis.navigator?.locks)
    return navigator.locks.request("vault-pack-" + id, fn);
  if (pending.has(id)) return pending.get(id);
  const p = fn();
  pending.set(id, p);
  try {
    return await p;
  } finally {
    pending.delete(id);
  }
}
export async function clearCache() {
  memory.clear();
  memoryBytes = 0;
  await operation("readwrite", (s) => s.clear());
}
