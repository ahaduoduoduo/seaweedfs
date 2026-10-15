#!/usr/bin/env python3
"""Real Vault + real OpenList gateway, disk-backed 115 isolation only."""
import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('client', ROOT / 'examples/vault/client.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Vault = module.Vault


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def request(url, method='GET', body=None):
    data = None if body is None else json.dumps(body).encode()
    with urllib.request.urlopen(urllib.request.Request(url, data, {'Content-Type': 'application/json'}, method=method), timeout=30) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def wait(fn, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(.2)
    raise TimeoutError('condition did not become true')


def expect(code, fn):
    try:
        fn()
    except urllib.error.HTTPError as error:
        assert error.code == code, (error.code, error.read())
    else:
        raise AssertionError('expected HTTP ' + str(code))


class Lab:
    def __init__(self, args):
        self.args = args
        self.root = Path(tempfile.mkdtemp(prefix='vault-scenarios-'))
        self.logdir = ROOT / 'build/vault-test-results'
        self.logdir.mkdir(parents=True, exist_ok=True)
        self.gateway = 'http://127.0.0.1:' + str(port())
        self.url = 'http://127.0.0.1:' + str(port())
        self.children = []
        self.handles = []
        self.tokens = {s: 'local-client-' + s + '-token-32-characters' for s in ['notes', 'photos', 'files']}
        self.admin = 'local-administrator-token-32-characters'
        for name, value in {'admin': self.admin, 'recovery': 'local-lab-recovery-token-32-characters', 'key': 'pQcQyjw/bnQrfZbZiBQpMvHuR/BrRXYDkOw11nTXOG0=', **self.tokens, **{s+'-gateway': 'local-lab-'+s+'-token' for s in self.tokens}}.items():
            (self.root / name).write_text(value)
        self.config = dict(listen=self.url.removeprefix('http://'), public_url=self.url, dir=str(self.root/'vault'), gateway=self.gateway+'/seaweedfs/v1', admin_token_file=str(self.root/'admin'), recovery_key_file=str(self.root/'key'), gateway_recovery_token_file=str(self.root/'recovery'), pack_mib=64, staging_mib=4096, seal_seconds=3, checkpoint_seconds=3600, lease_seconds=30, spaces=[dict(name=s, token_file=str(self.root/s), gateway_token_file=str(self.root/(s+'-gateway'))) for s in self.tokens])
        self.config_path = self.root / 'config.json'
        self.config_path.write_text(json.dumps(self.config))
        self.clients = {s: Vault(self.url, s, t) for s,t in self.tokens.items()}
        self.start_gateway()
        self.start_vault()

    def launch(self, name, command):
        handle = open(self.logdir/(name+'.log'), 'ab')
        self.handles.append(handle)
        process = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT)
        self.children.append(process)
        return process

    def start_gateway(self):
        self.gw = self.launch('openlist', [self.args.openlist_lab, '-listen', self.gateway.removeprefix('http://'), '-dir', str(self.root/'provider'), '-read-interval-ms', '1000'])
        wait(lambda: request(self.gateway+'/lab/status'))

    def start_vault(self):
        self.vault = self.launch('vault', [self.args.weed, 'vault', '-config', str(self.config_path)])
        wait(lambda: request(self.url+'/health'))

    def stop(self, child, crash=False):
        if child.poll() is None:
            child.kill() if crash else child.terminate()
            child.wait(timeout=30)

    def close(self):
        for child in self.children:
            self.stop(child)
        for handle in self.handles:
            handle.close()
        if self.args.keep:
            print('Retained lab:', self.root, flush=True)
        else:
            shutil.rmtree(self.root)

    def fixture(self, name, size):
        path = self.root/name
        rng = random.Random(name)
        with path.open('wb') as output:
            left = size
            while left:
                data = rng.randbytes(min(left, 4<<20))
                output.write(data)
                left -= len(data)
        return path

    def manifest(self, id, path, version=0, mime='application/octet-stream'):
        return dict(id=id, size=path.stat().st_size, sha256=hashlib.file_digest(path.open('rb'),'sha256').hexdigest(), content_type=mime, if_version=version, metadata={'name':path.name})

    def put(self, space, key, entries, group='', sort_key=''):
        client = self.clients[space]
        batch = client.create(key,[manifest for manifest,path in entries],group,sort_key)
        for manifest,path in entries:
            client.upload(batch,manifest['id'],path)
        client.commit(batch)
        return batch

    def admin_request(self, method, path, body=None):
        return Vault(self.url, 'notes', self.admin).request(method,path,body)


def run(lab):
    notes,photos,files = [lab.clients[s] for s in ['notes','photos','files']]
    print('1. Resumable upload survives SIGKILL; no cloud listing on startup', flush=True)
    resume = lab.fixture('resume.bin', 9<<20)
    batch = files.create('resume', [lab.manifest('resume',resume)], 'resume')
    with resume.open('rb') as source:
        files.request('PUT',files.base+'/batches/'+batch+'/objects/resume',source.read(4<<20),{'Upload-Offset':'0'})
    before = request(lab.gateway+'/lab/status')
    lab.stop(lab.vault,crash=True)
    lab.start_vault()
    assert request(lab.gateway+'/lab/status') == before
    files.upload(batch,'resume',resume)
    files.commit(batch)
    files.wait_remote(batch)
    assert hashlib.sha256(b''.join(files.read('resume'))).hexdigest() == lab.manifest('resume',resume)['sha256']

    print('2. Six concurrent producers: notes, album photos, video and large backup', flush=True)
    image = lab.root/'sample.jpg'
    video = lab.root/'sample.mp4'
    subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=size=2048x1536:rate=1','-vf','noise=alls=35:allf=t','-frames:v','1','-q:v','2',str(image)],check=True)
    subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=size=1920x1080:rate=30','-t','12','-c:v','libx264','-preset','ultrafast','-crf','12','-movflags','+faststart',str(video)],check=True)
    backup = lab.fixture('backup.bin', lab.args.large_mib<<20)
    text = lab.root/'article.json'
    text.write_text(json.dumps({'title':'旅行笔记','body':'收藏正文与图片视频附件。'*1200,'photos':['image'],'video':'video'},ensure_ascii=False))
    manifests = {p.name:lab.manifest('unused',p) for p in [image,video,text,backup]}
    batches=[]
    def produce(kind,index):
        result=[]
        if kind=='notes':
            for n in range(index,60,2):
                entries=[]
                for label,path in [('json',text),('image',image)]:
                    m={**manifests[path.name],'id':f'n{n:03d}-{label}','content_type':'application/json' if label=='json' else 'image/jpeg'}
                    entries.append((m,path))
                result.append(lab.put(kind,f'note-{n}',entries,'notebook-travel',f'{n:04d}'))
        elif kind=='photos':
            for n in range(index,lab.args.photos,2):
                m={**manifests[image.name],'id':f'photo-{n:04d}','content_type':'image/jpeg'}
                result.append(lab.put(kind,f'photo-{n}',[(m,image)],'album-2026-10-15',f'{n:05d}'))
        else:
            path=backup if index else video
            m={**manifests[path.name],'id':'backup' if index else 'video','content_type':'application/octet-stream' if index else 'video/mp4'}
            result.append(lab.put(kind,m['id'],[(m,path)],'library'))
        return kind,result
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures=[pool.submit(produce,s,i) for s in ['notes','photos','files'] for i in range(2)]
        for future in concurrent.futures.as_completed(futures):
            space,ids=future.result();batches.extend((space,id) for id in ids)
    for space,id in batches:lab.clients[space].wait_remote(id)
    assert all(path.stat().st_size<=64<<20 for path in (lab.root/'provider/cloud').glob('*/*'))
    assert not list((lab.root/'vault/staging').iterdir()) and not list((lab.root/'vault/packs').iterdir())
    plan=notes.request('GET',notes.base+'/objects/n000-json/plan')
    image_plan=notes.request('GET',notes.base+'/objects/n000-image/plan')
    assert plan['chunks'][0]['pack']==image_plan['chunks'][0]['pack'],'related note and image split unnecessarily'
    print('2b. Cached URL reuse, parallel CDN reads, restart and expiration',flush=True)
    from link_reuse import run as link_reuse
    reuse_result=link_reuse(lab,request)
    print('3. Atomic version conflicts and business token isolation',flush=True)
    first=lab.put('notes','update-one',[(lab.manifest('n000-json',text,1,'application/json'),text)],'notebook-travel')
    conflict=notes.create('update-conflict',[lab.manifest('n000-json',text,1),lab.manifest('must-not-exist',image)])
    notes.upload(conflict,'n000-json',text);notes.upload(conflict,'must-not-exist',image)
    expect(409,lambda:notes.commit(conflict))
    expect(404,lambda:notes.request('GET',notes.base+'/objects/must-not-exist'))
    notes.request('DELETE',notes.base+'/batches/'+conflict)
    expect(403,lambda:Vault(lab.url,'photos',lab.tokens['notes']).request('GET','/v1/spaces/photos/objects'))
    delegated=notes.request('POST',notes.base+'/tokens',{'subject':'browser','scope':'read','ttl_seconds':600})['token']
    expect(403,lambda:Vault(lab.url,'notes',delegated).create('denied',[lab.manifest('denied',text)]))
    notes.wait_remote(first)
    print('4. Concurrent Python range reads plus JavaScript CLI full-file read',flush=True)
    start,end=(64<<20)-200,(64<<20)+333
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        jobs=[pool.submit(lambda:b''.join(files.read('backup',start,end))),pool.submit(lambda:b''.join(notes.read('n001-json'))),pool.submit(lambda:b''.join(photos.read('photo-0000')))]
        env={**os.environ,'VAULT_URL':lab.url,'VAULT_SPACE':'files','VAULT_TOKEN':lab.tokens['files']}
        cli=subprocess.Popen([os.environ.get('NODE','node'),str(ROOT/'examples/vault/cli.mjs'),'get','backup',str(lab.root/'download.bin')],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        lab.children.append(cli)
        results=[f.result() for f in jobs]
        stdout,stderr=cli.communicate(timeout=600)
        assert cli.returncode==0,(stdout,stderr)
    with backup.open('rb') as source:source.seek(start);assert results[0]==source.read(end-start)
    assert results[1]==text.read_bytes() and results[2]==image.read_bytes()
    assert hashlib.file_digest((lab.root/'download.bin').open('rb'),'sha256').hexdigest()==manifests[backup.name]['sha256']
    print('5. Browser UI, WebCrypto, multi-tab cache and CORS fallback',flush=True)
    browser_config={'url':lab.url,'gateway':lab.gateway,'tokens':lab.tokens,'image':str(image),'text':str(text),'output':str(lab.logdir)}
    browser_path=lab.root/'browser.json';browser_path.write_text(json.dumps(browser_config))
    subprocess.run([os.environ.get('NODE','node'),str(ROOT/'test/vault/browser.mjs'),str(browser_path)],check=True,timeout=300)
    stats=request(lab.gateway+'/lab/status')
    assert stats['lists']==0 and stats['credential_leaks']==0,stats
    starts=stats['link_request_starts_ms'];assert all(b-a>=900 for a,b in zip(starts,starts[1:])),starts
    print('6. Unified encrypted checkpoint; restore with both local catalogs removed',flush=True)
    cp=lab.admin_request('POST','/v1/checkpoints')['id']
    def finished():
        state=lab.admin_request('GET','/v1/checkpoints/'+cp)
        if state['state']=='failed':raise RuntimeError(state)
        return state['state']=='complete'
    wait(finished,180)
    ticket=lab.admin_request('GET','/v1/checkpoints/'+cp+'/ticket')
    ticket_path=lab.root/'ticket.json';ticket_path.write_text(json.dumps(ticket))
    metadata_size=sum(p.stat().st_size for p in (lab.root/'vault').glob('vault.db*'))
    stats_before_restore=request(lab.gateway+'/lab/status')
    lab.stop(lab.vault);lab.stop(lab.gw)
    shutil.rmtree(lab.root/'vault')
    for p in (lab.root/'provider').glob('receipts.db*'):p.unlink()
    lab.start_gateway()
    subprocess.run([lab.args.weed,'vault.restore','-config',str(lab.config_path),'-ticket',str(ticket_path)],check=True,timeout=180)
    lab.start_vault()
    recovered=Vault(lab.url,'files',lab.tokens['files'])
    assert b''.join(recovered.read('backup',start,end))==results[0]
    assert b''.join(Vault(lab.url,'notes',lab.tokens['notes']).read('n001-json'))==text.read_bytes()
    restore_stats=request(lab.gateway+'/lab/status');assert restore_stats['lists']==0 and restore_stats['credential_leaks']==0
    print('7. Automatic checkpoint scheduling and per-version recovery status',flush=True)
    assert recovered.request('GET',recovered.base+'/objects/backup')['checkpoint']==cp
    lab.stop(lab.vault)
    lab.config['checkpoint_seconds']=2
    lab.config_path.write_text(json.dumps(lab.config))
    lab.start_vault()
    def auto_complete():
        states=lab.admin_request('GET','/v1/checkpoints')['checkpoints']
        return next((item for item in states if item['state']=='complete' and item['id']!=cp),None)
    automatic=wait(auto_complete,120)
    assert recovered.request('GET',recovered.base+'/objects/backup')['checkpoint']==automatic['id']
    lab.stop(lab.vault)
    lab.config['checkpoint_seconds']=3600
    lab.config_path.write_text(json.dumps(lab.config))
    lab.start_vault()
    print('8. Upload failure, restart recovery, CDN cooldown and signed-ticket rejection',flush=True)
    from faults import run as faults
    faults(lab,text,request,wait,expect,Vault)
    report=dict(link_reuse=reuse_result,baseline_objects=60*2+lab.args.photos+3,objects=60*2+lab.args.photos+5,logical_bytes=(lab.args.photos+60)*image.stat().st_size+60*text.stat().st_size+backup.stat().st_size+video.stat().st_size+resume.stat().st_size,large_file_mib=lab.args.large_mib,video_bytes=video.stat().st_size,photo_bytes=image.stat().st_size,producers=6,metadata_bytes=metadata_size,before_restore=stats_before_restore,after_restore=restore_stats,scenarios=['resumable_crash','parallel_businesses','atomic_conflict','space_auth','range_decrypt','node_full_download','browser_ui','cross_tab_cache','cors_fallback','catalog_loss_restore','automatic_checkpoint','upload_failure_restart','cdn_cooldown_restart','tampered_ticket_rejected','cached_link_parallel_downloads','link_expiration_single_refresh','link_cache_restart','read_session_schema_upgrade','unknown_expiry_concurrent_refresh','bounded_link_refresh'])
    (lab.logdir/'report.json').write_text(json.dumps(report,indent=2))
    print(json.dumps({k:v for k,v in report.items() if k not in ['before_restore','after_restore']},indent=2),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--weed',required=True);parser.add_argument('--openlist-lab',required=True);parser.add_argument('--large-mib',type=int,default=512);parser.add_argument('--photos',type=int,default=120);parser.add_argument('--keep',action='store_true');args=parser.parse_args()
    lab=Lab(args)
    try:run(lab)
    finally:lab.close()
