#!/usr/bin/env python3
"""Exercise the real 115 signature constraint through native and browser clients."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
from scenarios import Lab, request, wait

parser = argparse.ArgumentParser()
parser.add_argument('--weed', required=True)
parser.add_argument('--openlist-lab', required=True)
args = parser.parse_args()
args.keep = False
lab = Lab(args)
try:
    request(lab.gateway+'/lab/control', 'POST', {'require_empty_user_agent': True, 'busy_downloads': 1})
    path = lab.fixture('ua-contract.bin', 9 << 20)
    batch = lab.put('files', 'ua-contract', [(lab.manifest('ua-contract', path), path)])
    lab.clients['files'].flush()
    lab.clients['files'].wait_remote(batch)
    want = hashlib.sha256(path.read_bytes()).hexdigest()
    assert hashlib.sha256(b''.join(lab.clients['files'].read('ua-contract'))).hexdigest() == want
    initial = request(lab.gateway+'/lab/status')
    cfg = dict(url=lab.url, gateway=lab.gateway, token=lab.tokens['files'], want=want)
    config = lab.root/'ua-client.json'
    config.write_text(json.dumps(cfg))
    subprocess.run([os.environ.get('NODE', 'node'), str(Path(__file__).with_suffix('.mjs')), str(config)], check=True, timeout=120)
    after = request(lab.gateway+'/lab/status')
    assert after['links'] == initial['links'] == 1
    assert after['user_agent_mismatches'] == after['credential_leaks'] == 0
    lab.stop(lab.vault)
    lab.stop(lab.gw)
    # Also verify upgrading an existing catalog without the new nullable field.
    with sqlite3.connect(lab.root/'vault/vault.db') as db:
        db.execute('ALTER TABLE read_sessions DROP COLUMN download_ua')
    lab.start_gateway()
    lab.start_vault()
    lab.clients['files'].cache.clear()
    assert hashlib.sha256(b''.join(lab.clients['files'].read('ua-contract'))).hexdigest() == want
    assert request(lab.gateway+'/lab/status')['links'] == 0, 'restart lost URL constraints/cache'
    request(lab.gateway+'/lab/control', 'POST', {'require_empty_user_agent': True})
    cp = lab.admin_request('POST', '/v1/checkpoints')['id']
    def complete():
        state = lab.admin_request('GET', '/v1/checkpoints/'+cp)
        if state['state'] == 'failed': raise RuntimeError(state)
        return state['state'] == 'complete'
    wait(complete)
    ticket = lab.root/'ticket.json'
    ticket.write_text(json.dumps(lab.admin_request('GET', '/v1/checkpoints/'+cp+'/ticket')))
    request(lab.gateway+'/lab/control', 'POST', {'require_empty_user_agent': True, 'busy_downloads': 1})
    lab.stop(lab.vault)
    lab.config['dir'] = str(lab.root/'restored')
    lab.config_path.write_text(json.dumps(lab.config))
    subprocess.run([args.weed, 'vault.restore', '-config', str(lab.config_path), '-ticket', str(ticket)], check=True, timeout=120)
    lab.start_vault()
    lab.clients['files'].cache.clear()
    assert hashlib.sha256(b''.join(lab.clients['files'].read('ua-contract'))).hexdigest() == want
    assert request(lab.gateway+'/lab/status')['user_agent_mismatches'] == 0
    before = request(lab.gateway+'/lab/status')['links']
    lab.clients['files'].cache.clear()
    request(lab.gateway+'/lab/control', 'POST', {'require_empty_user_agent': True, 'busy_downloads': 9})
    try:
        b''.join(lab.clients['files'].read('ua-contract'))
        raise AssertionError('persistent CDN busy should fail after bounded retries')
    except RuntimeError as e:
        assert 'remains busy' in str(e)
    assert request(lab.gateway+'/lab/status')['links'] == before
    print('Download UA and CDN busy: Python, Node, browser proxy, cached restart, schema migration, Go restore and bounded same-URL retries passed', flush=True)
finally:
    lab.close()
