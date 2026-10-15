#!/usr/bin/env node
// Node 22+ reference CLI; the HTTP protocol is language independent.
import { openAsBlob, createWriteStream } from "node:fs";
import { readFile, rename, rm } from "node:fs/promises";
import { once } from "node:events";
import { basename } from "node:path";
import { VaultClient } from "../../weed/vault/assets/client.mjs";
const [command, ...args] = process.argv.slice(2);
const token = process.env.VAULT_TOKEN_FILE
  ? (await readFile(process.env.VAULT_TOKEN_FILE, "utf8")).trim()
  : process.env.VAULT_TOKEN;
if (!token || !process.env.VAULT_URL || !process.env.VAULT_SPACE) {
  console.error(
    "Set VAULT_URL, VAULT_SPACE and VAULT_TOKEN_FILE (or VAULT_TOKEN).",
  );
  process.exit(2);
}
const client = new VaultClient({
  url: process.env.VAULT_URL,
  space: process.env.VAULT_SPACE,
  token,
  transport: process.env.VAULT_TRANSPORT || "auto",
});
try {
  if (command === "put") {
    const [id, path, group = "", revision = "0"] = args;
    if (!id || !path) throw new Error("put ID FILE [GROUP] [IF_VERSION]");
    const blob = await openAsBlob(path);
    const batch = await client.putBatch({
      group,
      objects: [
        {
          id,
          blob,
          ifVersion: Number(revision),
          metadata: { name: basename(path) },
        },
      ],
    });
    console.log(JSON.stringify(batch));
    await client.waitRemote(batch.id);
    console.log(JSON.stringify(await client.object(id)));
  } else if (command === "get") {
    const [id, path, start, end] = args;
    if (!id || !path) throw new Error("get ID FILE [START] [END_EXCLUSIVE]");
    const temp = path + ".vault-" + crypto.randomUUID();
    const out = createWriteStream(temp, { flags: "wx", mode: 0o600 });
    try {
      for await (const bytes of client.stream(id, {
        start: Number(start || 0),
        end: end === undefined ? undefined : Number(end),
      })) {
        if (!out.write(bytes)) await once(out, "drain");
      }
      out.end();
      await once(out, "finish");
      await rename(temp, path);
    } catch (error) {
      out.destroy();
      await rm(temp, { force: true });
      throw error;
    }
    console.log(path);
  } else if (command === "list") {
    let after = "";
    do {
      const result = await client.list(after);
      for (const object of result.objects) console.log(JSON.stringify(object));
      after = result.next;
    } while (after);
  } else if (command === "plan") {
    console.log(JSON.stringify(await client.plan(args[0]), null, 2));
  } else throw new Error("Commands: put, get, list, plan");
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
