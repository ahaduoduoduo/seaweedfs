"""Fault scenarios exercise durable states through the public API."""
import base64
import hashlib
import json
import time
import urllib.error


def run(lab, text, request, wait, expect, Vault):
    files = lab.clients['files']
    request(lab.gateway+'/lab/control', 'POST', {'fail_upload': True})
    batch = lab.put('files', 'failed-cloud-upload', [(lab.manifest('retry-object',text),text)], 'retry-object')
    wait(lambda: files.request('GET',files.base+'/batches/'+batch)['state']=='packed')
    wait(lambda: bool(files.request('GET',files.base+'/status').get('worker_error','')))
    # Acknowledged local content remains readable while the provider is down.
    assert b''.join(Vault(lab.url,'files',lab.tokens['files']).read('retry-object'))==text.read_bytes()
    lab.stop(lab.vault,crash=True)
    lab.start_vault()
    assert files.request('GET',files.base+'/batches/'+batch)['state']=='packed'
    request(lab.gateway+'/lab/control', 'POST', {})
    files.wait_remote(batch)
    # A provider rejection must be reported by the client and persist a cooldown.
    request(lab.gateway+'/lab/control', 'POST', {'cdn_status':405})
    expect(405,lambda: b''.join(Vault(lab.url,'files',lab.tokens['files']).read('retry-object')))
    plan=files.request('GET',files.base+'/objects/retry-object/plan')
    pack=plan['chunks'][0]['pack']
    expect(503,lambda:files.request('POST',files.base+'/packs/'+pack+'/lease'))
    lab.stop(lab.vault,crash=True);lab.start_vault()
    expect(503,lambda:files.request('POST',files.base+'/packs/'+pack+'/lease'))
    request(lab.gateway+'/lab/control','POST',{})
    # Metadata queries continue locally throughout account cooldown.
    assert files.request('GET',files.base+'/objects/retry-object')['state']=='remote'
    # Bad recovery tickets are rejected before receipts or output DB are changed.
    ticket=lab.root/'ticket.json'
    changed=json.loads(ticket.read_text());changed['signature']=base64.b64encode(b'x'*32).decode()
    bad=lab.root/'bad-ticket.json';bad.write_text(json.dumps(changed))
    config=json.loads(lab.config_path.read_text());config['dir']=str(lab.root/'bad-restore')
    bad_config=lab.root/'bad-config.json';bad_config.write_text(json.dumps(config))
    import subprocess
    result=subprocess.run([lab.args.weed,'vault.restore','-config',str(bad_config),'-ticket',str(bad)],capture_output=True,timeout=30)
    assert result.returncode!=0 and not (lab.root/'bad-restore/vault.db').exists()
