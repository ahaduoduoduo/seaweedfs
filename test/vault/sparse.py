#!/usr/bin/env python3
"""Real-process sparse collection, full-pack uploads and cloud-only recovery."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time

from scenarios import Lab, Vault, request, wait, expect, port, ROOT


def run(lab):
    lab.stop(lab.vault)
    lab.config.pop('seal_seconds')  # Exercise the real default, not a test override.
    lab.config_path.write_text(json.dumps(lab.config))
    lab.start_vault()
    notes, photos, files = (lab.clients[s] for s in ('notes','photos','files'))
    assert notes.request('GET',notes.base+'/status')['seal_seconds']==0
    image=lab.fixture('sparse-photo.bin',2<<20)
    text=lab.root/'sparse-note.json'
    text.write_text(json.dumps({'title':'零散收藏','body':'正文与附件。'*500},ensure_ascii=False))
    original=text.read_bytes()
    lonely=lab.put('photos','lonely',[(lab.manifest('lonely',image),image)],'single-photo')
    def note(index):
        return lab.put('notes',f'note-{index:03d}',[(lab.manifest(f'n{index:03d}-json',text),text),(lab.manifest(f'n{index:03d}-image',image),image)],f'group-{index:03d}')
    batches=[note(0)]
    # Persistently old, tiny collections must survive restart without sealing.
    lab.stop(lab.vault,crash=True)
    with sqlite3.connect(lab.root/'vault/vault.db') as db:
        db.execute('UPDATE versions SET created=?',(int(time.time())-86400,))
        db.execute('ALTER TABLE batches DROP COLUMN flush_requested')
    lab.start_vault()
    time.sleep(1.3)
    assert request(lab.gateway+'/lab/status')['uploads']==0
    assert b''.join(notes.read('n000-json'))==original
    print('PASS: aged sparse writes stay readable on NAS after restart/schema upgrade',flush=True)

    for i in range(1,32):
        batches.append(note(i))
    wait(lambda:notes.request('GET',notes.base+'/batches/'+batches[0])['state']=='remote')
    assert notes.request('GET',notes.base+'/batches/'+batches[-1])['state']=='accepted'
    assert photos.request('GET',photos.base+'/batches/'+lonely)['state']=='accepted'
    note_packs=list((lab.root/'provider/cloud/notes').glob('*.pack'))
    assert len(note_packs)==1 and (60<<20)<=note_packs[0].stat().st_size<=(64<<20)
    p=notes.request('GET',notes.base+'/objects/n000-json/plan')
    q=notes.request('GET',notes.base+'/objects/n000-image/plan')
    assert p['chunks'][0]['pack']==q['chunks'][0]['pack']
    print('PASS: 32 independent note groups aggregate; older small business does not block a full business',flush=True)

    large=lab.fixture('sparse-large.bin',70<<20)
    large_batch=lab.put('files','large',[(lab.manifest('large',large),large)],'backup')
    wait(lambda:len(list((lab.root/'provider/cloud/files').glob('*.pack')))==1)
    assert files.request('GET',files.base+'/batches/'+large_batch)['state']=='accepted'
    prefix=list((lab.root/'provider/cloud/files').glob('*.pack'))[0]
    prefix_digest=hashlib.file_digest(prefix.open('rb'),'sha256').hexdigest()
    assert (60<<20)<=prefix.stat().st_size<=(64<<20)
    assert b''.join(files.read('large',65<<20,66<<20))==large.read_bytes()[65<<20:66<<20]
    # A local new revision must not replace its cloud-backed predecessor in backup.
    text.write_bytes(b'{"title":"new local revision"}')
    changed=lab.put('notes','local-edit',[(lab.manifest('n000-json',text,1),text)],'group-000')
    lab.stop(lab.vault)
    lab.config['checkpoint_seconds']=2
    lab.config_path.write_text(json.dumps(lab.config));lab.start_vault()
    def complete():
        return next((x for x in lab.admin_request('GET','/v1/checkpoints')['checkpoints'] if x['state']=='complete'),None)
    cp=wait(complete,90)['id']
    assert notes.request('GET',notes.base+'/batches/'+changed)['state']=='accepted'
    assert notes.request('GET',notes.base+'/objects/n000-json')['checkpoint']==''
    assert files.request('GET',files.base+'/batches/'+large_batch)['state']=='accepted'
    count=request(lab.gateway+'/lab/status')['uploads']
    time.sleep(2.5)
    assert request(lab.gateway+'/lab/status')['uploads']==count,'idle checkpoints must not keep uploading metadata'
    ticket=lab.admin_request('GET','/v1/checkpoints/'+cp+'/ticket')
    ticket_path=lab.root/'sparse-ticket.json';ticket_path.write_text(json.dumps(ticket))
    restore_url='http://127.0.0.1:'+str(port())
    cfg={**lab.config,'dir':str(lab.root/'restored'),'public_url':restore_url,'listen':restore_url.removeprefix('http://')}
    cfg_path=lab.root/'restore.json';cfg_path.write_text(json.dumps(cfg))
    subprocess.run([lab.args.weed,'vault.restore','-config',str(cfg_path),'-ticket',str(ticket_path)],check=True,timeout=120)
    restored=lab.launch('sparse-restored',[lab.args.weed,'vault','-config',str(cfg_path)])
    wait(lambda:request(restore_url+'/health'))
    recovered=Vault(restore_url,'notes',lab.tokens['notes'])
    assert recovered.request('GET',recovered.base+'/objects/n000-json')['revision']==1
    assert b''.join(recovered.read('n000-json'))==original
    expect(404,lambda:recovered.request('GET',recovered.base+'/objects/n031-json'))
    lab.stop(restored)
    print('PASS: automatic checkpoint leaves tails local; restore selects complete cloud revisions',flush=True)

    lab.stop(lab.vault);lab.config['checkpoint_seconds']=3600
    lab.config_path.write_text(json.dumps(lab.config));lab.start_vault()
    env={**os.environ,'VAULT_URL':lab.url,'VAULT_SPACE':'photos','VAULT_TOKEN':lab.tokens['photos']}
    subprocess.run([os.environ.get('NODE','node'),str(ROOT/'examples/vault/cli.mjs'),'sync'],env=env,check=True,timeout=60)
    assert photos.request('GET',photos.base+'/batches/'+lonely)['state']=='remote'
    assert notes.request('GET',notes.base+'/batches/'+changed)['state']=='accepted'
    assert files.request('GET',files.base+'/batches/'+large_batch)['state']=='accepted'
    read_token=notes.request('POST',notes.base+'/tokens',{'subject':'readonly','scope':'read','ttl_seconds':60})['token']
    expect(403,lambda:Vault(lab.url,'notes',read_token).flush())
    pending=notes.flush()['batches']
    pending_files=files.flush()['batches']
    lab.stop(lab.vault,crash=True);lab.start_vault()
    for batch in pending:notes.wait_remote(batch)
    for batch in pending_files:files.wait_remote(batch)
    assert b''.join(Vault(lab.url,'files',lab.tokens['files']).read('large'))==large.read_bytes()
    assert hashlib.file_digest(prefix.open('rb'),'sha256').hexdigest()==prefix_digest
    with sqlite3.connect(lab.root/'vault/vault.db') as db:
        assert db.execute("SELECT count(*) FROM packs WHERE space='files' AND state='remote'").fetchone()[0]==2
    # SIGKILL during an upload may leave an unreferenced tail attempt. The
    # already acknowledged full prefix must never be transmitted again.
    assert sum(hashlib.file_digest(p.open('rb'),'sha256').hexdigest()==prefix_digest for p in (lab.root/'provider/cloud/files').glob('*.pack'))==1
    assert not list((lab.root/'vault/staging').iterdir())
    assert not list((lab.root/'vault/packs').iterdir())
    stats=request(lab.gateway+'/lab/status')
    assert stats['lists']==0 and stats['credential_leaks']==0
    report={'note_groups':32,'capacity_mib':64,'large_file_mib':70,'time_sealing_disabled':True,'partial_pack_resume':True,'automatic_checkpoint_preserves_local_tails':True,'cloud_revision_restore':True,'manual_flush_restart':True,'space_isolation':True,'cloud_stats':stats}
    (lab.logdir/'sparse-report.json').write_text(json.dumps(report,indent=2))
    print('PASS: manual per-business sync, crash recovery and no duplicate full-pack upload',flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--weed',required=True);p.add_argument('--openlist-lab',required=True);p.add_argument('--keep',action='store_true')
    lab=Lab(p.parse_args())
    try:run(lab)
    finally:lab.close()
