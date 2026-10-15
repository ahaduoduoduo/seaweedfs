#!/usr/bin/env python3
"""Launch a persistent, loopback-only Vault demo with the real OpenList lab."""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import time

parser=argparse.ArgumentParser()
parser.add_argument('--weed',required=True)
parser.add_argument('--openlist-lab',required=True)
parser.add_argument('--dir',required=True)
parser.add_argument('--vault-port',type=int,default=19400)
parser.add_argument('--gateway-port',type=int,default=19401)
args=parser.parse_args()
root=Path(args.dir).resolve();root.mkdir(mode=0o700,parents=True,exist_ok=True)
def secret(name,value):
    path=root/name
    if not path.exists():
        fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        with os.fdopen(fd,'w') as f:f.write(value)
    return str(path)
config=dict(listen=f'127.0.0.1:{args.vault_port}',public_url=f'http://127.0.0.1:{args.vault_port}',dir=str(root/'vault'),gateway=f'http://127.0.0.1:{args.gateway_port}/seaweedfs/v1',admin_token_file=secret('admin.token',secrets.token_urlsafe(32)),recovery_key_file=secret('recovery.key',base64.b64encode(secrets.token_bytes(32)).decode()),gateway_recovery_token_file=secret('openlist-recovery.token','local-lab-recovery-token-32-characters'),seal_seconds=5,checkpoint_seconds=300,spaces=[])
for space in ['notes','photos','files']:
    config['spaces'].append(dict(name=space,token_file=secret(space+'.token',secrets.token_urlsafe(32)),gateway_token_file=secret('openlist-'+space+'.token','local-lab-'+space+'-token')))
(root/'config.json').write_text(json.dumps(config,indent=2))
children=[]
def stop(*_):
    for child in children:
        if child.poll() is None:child.terminate()
    for child in children:
        try:child.wait(timeout=15)
        except subprocess.TimeoutExpired:child.kill();child.wait()
def interrupt(*_):
    stop()
    raise SystemExit()
signal.signal(signal.SIGTERM,interrupt);signal.signal(signal.SIGINT,interrupt)
try:
    children.append(subprocess.Popen([args.openlist_lab,'-listen',f'127.0.0.1:{args.gateway_port}','-dir',str(root/'provider')]))
    children.append(subprocess.Popen([args.weed,'vault','-config',str(root/'config.json')]))
    print(f"Web UI: {config['public_url']}\nNotes token file: {root/'notes.token'}\nConfiguration: {root/'config.json'}\nCtrl-C stops both processes; data persists.",flush=True)
    while all(child.poll() is None for child in children):time.sleep(.5)
    raise SystemExit(next(child.returncode for child in children if child.poll() is not None))
finally:stop()
