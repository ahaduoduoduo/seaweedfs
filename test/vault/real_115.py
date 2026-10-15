#!/usr/bin/env python3
"""Explicit real-provider acceptance run. Config supplies URL and token-file paths.

Writes uniquely prefixed objects to three already provisioned spaces; never lists
or removes provider directories, changes provider credentials, or injects faults.
"""
import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import subprocess
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('vault_client', ROOT / 'examples/vault/client.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def digest(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


class MeasuredVault(module.Vault):
    def __init__(self, *args):
        super().__init__(*args)
        self.transfers = []

    def download_pack(self, pack):
        start = time.perf_counter()
        data = super().download_pack(pack)
        self.transfers.append({'pack': pack['id'], 'bytes': len(data),
                               'seconds': time.perf_counter() - start})
        return data


class Run:
    def __init__(self, args):
        self.args = args
        self.config = json.loads(Path(args.config).read_text())
        self.url = self.config['url']
        self.tokens = {s: Path(p).read_text().strip() for s, p in self.config['token_files'].items()}
        self.out = Path(args.output).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.prefix = args.prefix
        self.report = {'prefix': self.prefix, 'url': self.url, 'producers': 6,
                       'pack_limit_mib': 64, 'uploads': [], 'reads': []}
        self.lock = threading.Lock()

    def client(self, space):
        return MeasuredVault(self.url, space, self.tokens[space])

    def save(self):
        (self.out / 'report.json').write_text(json.dumps(self.report, ensure_ascii=False, indent=2))

    def prepare(self):
        image, video, backup = [self.out / name for name in ['sample.jpg', 'sample.mp4', 'backup.bin']]
        if not image.exists():
            subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-f', 'lavfi',
                            '-i', 'testsrc2=size=2048x1536:rate=1', '-vf', 'noise=alls=35:allf=t',
                            '-frames:v', '1', '-q:v', '2', str(image)], check=True)
        if not video.exists():
            subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-f', 'lavfi',
                            '-i', 'testsrc2=size=1920x1080:rate=30', '-t', '12', '-c:v', 'libx264',
                            '-preset', 'ultrafast', '-crf', '12', '-movflags', '+faststart', str(video)], check=True)
        if not backup.exists():
            with backup.open('wb') as target:
                for _ in range(self.args.large_mib):
                    target.write(os.urandom(1 << 20))
        self.fixtures = {'image': image, 'video': video, 'backup': backup}
        self.manifests = {name: {'size': p.stat().st_size, 'sha256': digest(p)} for name, p in self.fixtures.items()}
        self.report['fixtures'] = self.manifests
        self.save()

    def put(self, client, name, objects, group):
        entries = []
        for oid, path, mime in objects:
            entries.append({'id': self.prefix + '-' + oid, 'size': path.stat().st_size,
                            'sha256': digest(path), 'content_type': mime, 'if_version': 0,
                            'metadata': {'name': path.name, 'acceptance_run': self.prefix}})
        start = time.perf_counter()
        batch = client.create(self.prefix + '-' + name, entries, group, name)
        state = client.request('GET', client.base + '/batches/' + batch)
        if state['state'] == 'uploading':
            for entry, (_, path, _) in zip(entries, objects):
                client.upload(batch, entry['id'], path)
            client.commit(batch)
        row = {'space': client.space, 'batch': batch, 'name': name,
               'bytes': sum(e['size'] for e in entries), 'seconds': time.perf_counter() - start,
               'objects': [e['id'] for e in entries]}
        with self.lock:
            self.report['uploads'].append(row)
            self.save()
        return row

    def produce(self, space, worker):
        c = self.client(space)
        if space == 'notes':
            for n in range(worker, 24, 2):
                article = self.out / f'note-{n:03d}.json'
                article.write_text(json.dumps({'title': '真实 115 收藏测试', 'body': '笔记正文与附件一起保存。' * 600,
                                               'image': self.prefix + f'-note-{n:03d}-image'}, ensure_ascii=False))
                self.put(c, f'note-{n:03d}', [(f'note-{n:03d}-json', article, 'application/json'),
                                            (f'note-{n:03d}-image', self.fixtures['image'], 'image/jpeg')], f'note-{n:03d}')
        elif space == 'photos':
            for n in range(worker, 64, 2):
                self.put(c, f'photo-{n:03d}', [(f'photo-{n:03d}', self.fixtures['image'], 'image/jpeg')], 'album-20261015')
        else:
            kind = 'backup' if worker else 'video'
            self.put(c, kind, [(kind, self.fixtures[kind], 'video/mp4' if kind == 'video' else 'application/octet-stream')], 'files')

    def write(self):
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda pair: self.produce(*pair), [(s, n) for s in self.tokens for n in range(2)]))
        self.report['nas_commit_seconds'] = time.perf_counter() - start
        self.report['logical_bytes'] = sum(x['bytes'] for x in self.report['uploads'])
        print('NAS commits:', round(self.report['nas_commit_seconds'], 3), 'seconds;', self.report['logical_bytes'], 'bytes', flush=True)
        for space in self.tokens:
            self.client(space).flush()
        deadline = time.monotonic() + 2400
        while time.monotonic() < deadline:
            states = {s: self.client(s).request('GET', f'/v1/spaces/{s}/status') for s in self.tokens}
            if all(v['accepted'] == 0 and v['packed'] == 0 for v in states.values()):
                break
            print('Cloud progress:', json.dumps(states), flush=True)
            time.sleep(15)
        else:
            raise TimeoutError('real-provider upload did not finish in 40 minutes')
        self.report['cloud_complete_seconds'] = time.perf_counter() - start
        assert all(self.client(x['space']).request('GET', f"/v1/spaces/{x['space']}/batches/{x['batch']}")['state'] == 'remote'
                   for x in self.report['uploads'])
        self.save()

    def latency(self):
        results = {}
        for name, path in [('list', '/objects'), ('metadata', '/objects/' + self.prefix + '-note-000-json'),
                           ('plan', '/objects/' + self.prefix + '-note-000-json/plan')]:
            c = self.client('notes')
            samples = []
            for _ in range(30):
                start = time.perf_counter()
                c.request('GET', c.base + path)
                samples.append((time.perf_counter() - start) * 1000)
            results[name] = {'p50_ms': statistics.median(samples), 'p95_ms': sorted(samples)[28]}
        self.report['local_metadata_latency'] = results
        self.save()

    def read(self, space, oid, path, *, client=None, start=0, end=None, label=None):
        c = client or self.client(space)
        begin = time.perf_counter()
        first = None
        hasher = hashlib.sha256()
        size = 0
        before = len(c.transfers)
        for part in c.read(self.prefix + '-' + oid, start, end):
            if first is None:
                first = time.perf_counter() - begin
            hasher.update(part)
            size += len(part)
        elapsed = time.perf_counter() - begin
        with path.open('rb') as source:
            source.seek(start)
            want = hashlib.sha256(source.read() if end is None else source.read(end - start)).hexdigest()
        assert hasher.hexdigest() == want, 'plaintext integrity mismatch'
        transfers = c.transfers[before:]
        row = {'label': label or oid, 'bytes': size, 'seconds': elapsed, 'first_plaintext_seconds': first,
               'mib_per_second': size / (1 << 20) / elapsed,
               'downloaded_pack_bytes': sum(x['bytes'] for x in transfers), 'pack_transfers': len(transfers)}
        with self.lock:
            self.report['reads'].append(row)
            self.save()
        print('Read:', json.dumps(row), flush=True)
        return row

    def reads(self):
        self.latency()
        photo = self.client('photos')
        self.read('photos', 'photo-000', self.fixtures['image'], client=photo, label='photo_cold')
        for n in range(1, 8):
            self.read('photos', f'photo-{n:03d}', self.fixtures['image'], client=photo, label='album_neighbor')
        self.read('photos', 'photo-000', self.fixtures['image'], label='new_client_cached_url')
        files = self.client('files')
        self.read('files', 'backup', self.fixtures['backup'], client=files, start=(60 << 20)-128,
                  end=(60 << 20)+128, label='cross_pack_range_256B')
        browser_cfg = {'url': self.url, 'token_files': self.config['token_files'], 'prefix': self.prefix,
                       'output': str(self.out), 'image': str(self.fixtures['image']),
                       'article': str(self.out / 'note-000.json')}
        browser_path = self.out / 'browser-config.json'
        browser_path.write_text(json.dumps(browser_cfg)); browser_path.chmod(0o600)
        env = {**os.environ, 'VAULT_URL': self.url, 'VAULT_SPACE': 'files',
               'VAULT_TOKEN_FILE': self.config['token_files']['files']}
        output = self.out / 'downloaded-backup.bin'
        start = time.perf_counter()
        def cli_download():
            began = time.perf_counter()
            result = subprocess.run([os.environ.get('NODE', 'node'), str(ROOT / 'examples/vault/cli.mjs'),
                                     'get', self.prefix + '-backup', str(output)], env=env,
                                    capture_output=True, check=True, timeout=900)
            seconds = time.perf_counter()-began
            self.report['cli_backup_download'] = {'seconds': seconds,
                'mib_per_second': self.manifests['backup']['size'] / (1 << 20) / seconds}
            return result
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            jobs = [pool.submit(self.read, 'notes', 'note-000-json', self.out / 'note-000.json', label='concurrent_note'),
                    pool.submit(cli_download),
                    pool.submit(subprocess.run, [os.environ.get('NODE', 'node'), str(ROOT / 'test/vault/real_browser.mjs'),
                                str(browser_path)], capture_output=True, check=True, timeout=900)]
            for job in jobs:
                result = job.result()
                if isinstance(result, subprocess.CompletedProcess):
                    print(result.stdout.decode(), flush=True)
        self.report['concurrent_python_cli_browser_seconds'] = time.perf_counter() - start
        assert digest(output) == self.manifests['backup']['sha256']
        output.unlink()
        self.report['browser'] = json.loads((self.out / 'browser-report.json').read_text())
        self.cdn_transfers()
        self.save()

    def cdn_transfers(self):
        c = self.client('files')
        plan = c.request('GET', c.base + '/objects/' + self.prefix + '-backup/plan')
        packs = list(plan['packs'].values())[:4]
        sessions = []
        for pack in packs:
            start = time.perf_counter()
            lease = c.request('POST', c.base + '/packs/' + pack['id'] + '/lease')
            sessions.append((pack, lease, time.perf_counter()-start))
        def transfer(item):
            pack, lease, resolve = item
            status = 0
            try:
                start = time.perf_counter()
                headers = {'Range': f"bytes=0-{pack['size']-1}"}
                if isinstance(lease.get('download_user_agent'), str):
                    headers['User-Agent'] = lease['download_user_agent']
                req = urllib.request.Request(lease['url'], headers=headers)
                with urllib.request.urlopen(req, timeout=120) as response:
                    status = response.status
                    ttfb = time.perf_counter()-start
                    hasher, count = hashlib.sha256(), 0
                    while part := response.read(1 << 20):
                        count += len(part); hasher.update(part)
                elapsed = time.perf_counter()-start
                assert status == 206 and count == pack['size'] and hasher.hexdigest() == pack['sha256']
                return {'bytes': count, 'seconds': elapsed, 'ttfb_seconds': ttfb,
                        'cached_url_acquisition_ms': resolve*1000, 'mib_per_second': count/(1 << 20)/elapsed}
            finally:
                c.request('POST', c.base + '/leases/' + lease['id'] + '/complete', {'status': status})
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(transfer, sessions))
        seconds = time.perf_counter()-start
        self.report['four_concurrent_cached_urls'] = {'seconds': seconds, 'transfers': results,
            'aggregate_mib_per_second': sum(r['bytes'] for r in results)/(1 << 20)/seconds}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--prefix', default='dsm-20261015')
    parser.add_argument('--large-mib', type=int, default=512)
    parser.add_argument('--phase', choices=['prepare', 'write', 'read', 'all'], default='all')
    args = parser.parse_args()
    run = Run(args)
    old = run.out / 'report.json'
    if old.exists():
        run.report = json.loads(old.read_text())
    run.prepare()
    if args.phase in ['all', 'write']:
        run.write()
    if args.phase in ['all', 'read']:
        run.reads()
    print('Report:', run.out / 'report.json', flush=True)


if __name__ == '__main__':
    main()
