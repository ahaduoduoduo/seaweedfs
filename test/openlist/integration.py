#!/usr/bin/env python3
"""Real SeaweedFS + real OpenList gateway + local, disk-backed 115 simulator.

No provider credentials, Docker, third-party Python packages or NAS required.
Processes always stop; --keep retains logs/data for inspection.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


HTTP = urllib.request.build_opener(NoRedirect)


def request(url, method="GET", data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        resp = HTTP.open(req, timeout=45)
    except urllib.error.HTTPError as exc:
        resp = exc
    with resp:
        return resp.status, dict(resp.headers), resp.read()


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class Lab:
    def __init__(self, args, root):
        self.args, self.root = args, root
        self.processes, self.logs = [], []
        self.g, self.m, self.mg, self.v, self.vg, self.f, self.fg = free_ports(7)
        self.gateway = f"http://127.0.0.1:{self.g}"
        self.filer = f"http://127.0.0.1:{self.f}"
        self.volumes = root / "volumes"
        self.volumes.mkdir()
        self.conf = root / "config"
        self.conf.mkdir()
        backend = []
        for name in ("notes", "photos", "files"):
            token = self.conf / f"{name}.token"
            token.write_text(f"local-lab-{name}-token")
            token.chmod(0o600)
            backend.append(f'''[storage.backend.openlist.{name}]
enabled = true
endpoint = "{self.gateway}/seaweedfs/v1"
space = "{name}"
token_file = "{token}"
read_window_mib = "64"
cache_mib = "64"
max_object_mib = "64"
timeout_seconds = "30"
''')
        (self.conf / "master.toml").write_text("\n".join(backend))
        (self.conf / "filer.toml").write_text(f'''[leveldb2]
enabled = true
dir = "{root / 'metadata'}"
[filer.options]
metaLog.collection = "filer-meta"
''')

    def start(self, name, args):
        log = open(self.root / f"{name}.log", "ab")
        self.logs.append(log)
        process = subprocess.Popen(args, cwd=self.conf, stdout=log, stderr=log)
        self.processes.append(process)
        return process

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def wait_http(self, process, url):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            assert process.poll() is None, f"process exited: {process.returncode}; logs: {self.root}"
            try:
                if request(url)[0] == 200:
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.2)
        raise AssertionError(f"not ready: {url}; logs: {self.root}")

    def start_gateway(self):
        self.gateway_process = self.start("openlist", [self.args.gateway, "-listen", f"127.0.0.1:{self.g}", "-dir", str(self.root / "openlist")])
        self.wait_http(self.gateway_process, self.gateway + "/lab/status")

    def start_weed(self):
        self.weed_process = self.start("weed", [self.args.weed, "server", "-ip=127.0.0.1", "-ip.bind=127.0.0.1", "-filer", "-filer.encryptVolumeData", "-filer.maxMB=4", "-filer.saveToFilerLimit=0", "-volume.fileSizeLimitMB=5", "-volume.max=30", "-volume.preStopSeconds=0", "-master.volumeSizeLimitMB=60", "-master.telemetry=false", f"-dir={self.volumes}", f"-master.port={self.m}", f"-master.port.grpc={self.mg}", f"-volume.port={self.v}", f"-volume.port.grpc={self.vg}", f"-filer.port={self.f}", f"-filer.port.grpc={self.fg}"])
        self.wait_http(self.weed_process, self.filer + "/?limit=1")

    def shell(self, commands, expect_failure=False):
        result = subprocess.run([self.args.weed, "shell", f"-master=127.0.0.1:{self.m}.{self.mg}", f"-filer=127.0.0.1:{self.f}.{self.fg}"], input="lock\n"+commands+"\nunlock\nexit\n", text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=self.conf, timeout=50)
        with open(self.root / "shell.log", "a") as log:
            log.write(result.stdout)
        assert (result.returncode != 0) if expect_failure else (result.returncode == 0), result.stdout
        return result.stdout

    def stats(self):
        return json.loads(request(self.gateway + "/lab/status")[2])

    def control(self, **values):
        assert request(self.gateway + "/lab/control", "POST", json.dumps(values).encode(), {"Content-Type": "application/json"})[0] == 204

    def object(self, name, key="", method="GET", data=None, extra=None):
        headers = {"Authorization": f"Bearer local-lab-{name}-token"}
        headers.update(extra or {})
        path = "/objects" + ("/" + key if key else "")
        return request(self.gateway + f"/seaweedfs/v1/{name}" + path, method, data, headers)

    def upload(self, path, data):
        status, _, body = request(self.filer + path, "PUT", data, {"Content-Type": "application/octet-stream"})
        assert status in (200, 201), (status, body)

    def tier(self, name, volume_id, expect_failure=False):
        return self.shell(f"volume.tier.upload -collection={name} -volumeId={volume_id} -dest=openlist.{name}", expect_failure=expect_failure)

    def run(self):
        self.start_gateway()
        self.start_weed()
        print("PASS: native SeaweedFS and real OpenList gateway started", flush=True)
        for name in ("notes", "photos", "files"):
            self.shell(f"fs.configure -locationPrefix=/{name}/ -collection={name} -volumeGrowthCount=1 -fsync -apply")
        marker = b"private-note-content-never-in-cloud-plaintext-"
        contents = {"/notes/item.json": marker + b'{"image":"image.bin","video":"video.bin"}', "/notes/image.bin": os.urandom(130000), "/notes/video.bin": os.urandom(2200000), "/photos/001.bin": os.urandom(45000), "/files/example.bin": os.urandom(8000)}
        for i in range(40):
            contents[f"/notes/attachment-{i:02d}.bin"] = os.urandom(4000)
        for path, data in contents.items():
            self.upload(path, data)
        targets = {}
        for name in ("notes", "photos", "files"):
            dats = list(self.volumes.glob(f"{name}_*.dat"))
            assert len(dats) == 1, dats
            targets[name] = dats[0]
        # Upload failure preserves the only local data copy; retry can succeed.
        self.control(fail_upload=True)
        notes = targets["notes"]
        vid = int(notes.stem.split("_")[-1])
        self.tier("notes", vid, expect_failure=True)
        assert notes.exists(), "failed tier removed local data"
        assert json.loads(notes.with_suffix(".vif").read_text()).get("files", []) == []
        assert json.loads(self.object("notes")[2]) == []
        self.control()
        # A failed local .vif rename after remote success remains retryable.
        vif = notes.with_suffix(".vif")
        backup = notes.with_suffix(".vif.saved")
        vif.rename(backup)
        vif.mkdir()
        self.tier("notes", vid, expect_failure=True)
        assert notes.exists(), "failed metadata commit removed local data"
        committed_uploads = self.stats()["uploads"]
        vif.rmdir()
        backup.rename(vif)
        for name, dat in targets.items():
            self.tier(name, int(dat.stem.split("_")[-1]))
            assert not dat.exists(), f"tier did not remove {dat}; see shell.log"
            info = json.loads(dat.with_suffix(".vif").read_text())
            assert info["files"][0]["backendType"] == "openlist"
            if name == "notes":
                assert self.stats()["uploads"] == committed_uploads, "metadata retry repeated cloud upload"
        packs = list((self.root / "openlist/cloud").glob("*/*.pack"))
        assert len(packs) == 3, packs
        assert all(marker not in p.read_bytes() for p in packs), "plaintext leaked into cloud pack"
        print("PASS: 45 files encrypted into 3 business packs; failed upload retained local data", flush=True)

        # Concurrent small-file reads share one remote range for the notes pack.
        self.control(download_delay_ms=1250)
        before = self.stats()
        def check(item):
            path, expected = item
            status, _, body = request(self.filer + path)
            assert status == 200 and body == expected, (path, status, len(body))
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(check, contents.items()))
        after = self.stats()
        assert after["downloads"] - before["downloads"] == 3, (before, after)
        assert after["lists"] == 0 and after["credential_leaks"] == 0, after
        assert after["max_active_downloads"] == 1, after
        self.control()
        starts = after["download_starts_ms"]
        assert all(b-a >= 850 for a, b in zip(starts, starts[1:])), starts
        status, _, body = request(self.filer + "/notes/video.bin", headers={"Range": "bytes=123-999"})
        assert status == 206 and body == contents["/notes/video.bin"][123:1000]
        assert self.stats()["downloads"] == after["downloads"]
        print("PASS: concurrent reads + plaintext Range matched; 3 CDN requests, zero lists, no token forwarding", flush=True)

        # Idempotent commit, local HEAD/list, auth and business isolation.
        inventory = json.loads(self.object("notes")[2])
        key = inventory[0]["key"]
        pack = next(p for p in packs if p.parent.name == "notes")
        count = self.stats()["uploads"]
        assert self.object("notes", key, "PUT", pack.read_bytes())[0] == 200
        assert self.stats()["uploads"] == count
        assert self.object("notes", key, "HEAD")[0] == 200
        assert self.object("photos", key, "HEAD")[0] == 404
        assert self.object("notes", key, extra={"Authorization": "Bearer wrong-token"})[0] == 401
        # An ambiguous response leaves an orphan but never exposes a pending key.
        payload = os.urandom(32000)
        orphan_key = hashlib.sha256(payload).hexdigest()
        self.control(lose_upload_receipt=True)
        assert self.object("files", orphan_key, "PUT", payload)[0] == 502
        assert self.object("files", orphan_key, "HEAD")[0] == 409
        self.control()
        assert self.object("files", orphan_key, "PUT", payload)[0] == 201
        print("PASS: idempotent receipts, isolation, ambiguous-upload recovery", flush=True)

        # Full process restart: volume mount must not scan/download remote packs.
        self.stop(self.weed_process)
        self.stop(self.gateway_process)
        self.start_gateway()
        self.start_weed()
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            status, _, body = request(f"http://127.0.0.1:{self.m}/dir/lookup?volumeId={vid}")
            if status == 200 and json.loads(body).get("locations"):
                break
            time.sleep(0.2)
        else:
            raise AssertionError("restarted volume did not register with master")
        assert self.stats()["downloads"] == 0 and self.stats()["links"] == 0, self.stats()
        check(("/notes/item.json", contents["/notes/item.json"]))
        assert self.stats()["downloads"] == 1
        print("PASS: persistent receipts and .idx/.vif recovery; restart made zero cloud requests", flush=True)

        # Cold pack 405 triggers a persistent circuit; subsequent failures stay local.
        self.control(cdn_status=405)
        status, _, _ = request(self.filer + "/photos/001.bin")
        assert status >= 400, status
        blocked_count = self.stats()["downloads"]
        assert blocked_count == 2, self.stats()
        status, headers, _ = self.object("files", orphan_key)
        assert status == 503 and int(headers["Retry-After"]) > 0
        assert self.stats()["downloads"] == blocked_count
        self.stop(self.gateway_process)
        self.start_gateway()
        assert self.object("files", orphan_key)[0] == 503
        another = os.urandom(100)
        assert self.object("files", hashlib.sha256(another).hexdigest(), "PUT", another)[0] == 503
        assert self.stats()["uploads"] == 0 and self.stats()["links"] == 0
        print("PASS: CDN 405 cooldown survives restart and blocks reads/uploads without provider traffic", flush=True)

    def close(self):
        for process in reversed(self.processes):
            self.stop(process)
        for log in self.logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weed", required=True)
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    args.weed, args.gateway = str(Path(args.weed).resolve()), str(Path(args.gateway).resolve())
    root = Path(tempfile.mkdtemp(prefix="seaweed-openlist-e2e-"))
    print(f"Lab directory: {root}", flush=True)
    lab = Lab(args, root)
    try:
        lab.run()
        print("ALL LOCAL INTEGRATION CHECKS PASSED", flush=True)
    except BaseException:
        args.keep = True
        raise
    finally:
        lab.close()
        if not args.keep:
            shutil.rmtree(root)
        else:
            print(f"Logs/data retained: {root}", flush=True)


if __name__ == "__main__":
    main()
