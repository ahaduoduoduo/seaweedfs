import { VaultClient } from "./client.mjs";
const $ = (id) => document.getElementById(id);
let client,
  next = "",
  previewURL;
function status(text, error = false) {
  $("status").textContent = text;
  $("status").classList.toggle("error", error);
}
function event(e) {
  if (e.type === "upload") {
    $("progress").hidden = false;
    $("progress").value = e.offset / e.size;
    return;
  }
  if (["download", "cache-hit", "cors-fallback"].includes(e.type)) {
    $("events").textContent = [
      `${new Date().toLocaleTimeString()} ${e.type} ${e.transport || ""}`,
      $("events").textContent,
    ]
      .join("\n")
      .slice(0, 3000);
  }
}
async function run(fn) {
  try {
    await fn();
  } catch (error) {
    status(error.message, true);
  }
}
async function list(append = false) {
  const result = await client.list(append ? next : "");
  next = result.next;
  if (!append) $("files").replaceChildren();
  for (const o of result.objects) {
    const li = document.createElement("li"),
      left = document.createElement("div"),
      name = document.createElement("div"),
      meta = document.createElement("div"),
      button = document.createElement("button");
    name.className = "name";
    name.textContent = o.metadata?.name || o.id;
    meta.className = "meta";
    meta.textContent = `${(o.size / 1048576).toFixed(2)} MiB · 版本 ${o.revision} · ${{ accepted: "等待封存", packed: "正在上传", remote: "已保存至云端" }[o.state] || o.state}`;
    left.append(name, meta);
    button.textContent = "读取";
    button.onclick = () =>
      run(async () => {
        if (o.size > 32 * 1048576)
          throw new Error(
            "该文件超过页面预览上限，请使用 SDK stream() 或 CLI 下载。",
          );
        button.disabled = true;
        try {
          status("正在读取 " + name.textContent);
          const bytes = await client.read(o.id);
          $("previewTitle").textContent = name.textContent;
          $("preview").replaceChildren();
          if (previewURL) URL.revokeObjectURL(previewURL);
          if (
            o.content_type.startsWith("image/") ||
            o.content_type.startsWith("video/")
          ) {
            previewURL = URL.createObjectURL(
              new Blob([bytes], { type: o.content_type }),
            );
            const el = document.createElement(
              o.content_type.startsWith("image/") ? "img" : "video",
            );
            el.src = previewURL;
            if (el.tagName === "VIDEO") el.controls = true;
            else el.alt = name.textContent;
            $("preview").append(el);
          } else
            $("preview").textContent = new TextDecoder().decode(
              bytes.subarray(0, 1024 * 1024),
            );
          $("previewCard").hidden = false;
          status("读取完成，在客户端解密");
        } finally {
          button.disabled = false;
        }
      });
    li.append(left, button);
    $("files").append(li);
  }
  if (!$("files").children.length) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "这里还没有内容";
    $("files").append(li);
  }
  $("more").hidden = !next;
}
$("connect").onclick = () =>
  run(async () => {
    client = new VaultClient({
      url: location.origin,
      space: $("space").value,
      token: $("token").value,
      transport: $("transport").value,
      onEvent: event,
    });
    await list();
    $("send").disabled = false;
    $("refresh").disabled = false;
    status("已连接 " + client.space);
  });
$("refresh").onclick = () => run(() => list());
$("more").onclick = () => run(() => list(true));
$("send").onclick = () =>
  run(async () => {
    const files = [...$("upload").files];
    if (!files.length) throw new Error("请选择文件");
    $("send").disabled = true;
    try {
      status("正在校验并上传");
      const batch = await client.putBatch({
        group: $("group").value,
        objects: files.map((blob) => ({
          id: crypto.randomUUID(),
          blob,
          metadata: { name: blob.name },
        })),
      });
      status("已提交 " + files.length + " 个文件，后台自动封存");
      $("progress").hidden = true;
      $("upload").value = "";
      await list();
      await client.waitRemote(batch.id);
      await list();
      status("这组内容已保存至云端");
    } finally {
      $("send").disabled = false;
    }
  });
$("closePreview").onclick = () => {
  $("previewCard").hidden = true;
  if (previewURL) URL.revokeObjectURL(previewURL);
};
