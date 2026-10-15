"""Small Python HTTP reference. Install cryptography for AES-GCM reads."""
import base64
import hashlib
import json
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class Vault:
    def __init__(self, url, space, token):
        self.url, self.space, self.token = url.rstrip('/'), space, token
        self.base = '/v1/spaces/' + space
        self.cache = OrderedDict()

    def request(self, method, path, body=None, headers=None, raw=False, wait_cooldown=False):
        head = {'Authorization': 'Bearer ' + self.token, **(headers or {})}
        if body is not None and not isinstance(body, bytes):
            body = json.dumps(body).encode()
            head['Content-Type'] = 'application/json'
        start = time.monotonic()
        while True:
            try:
                with urllib.request.urlopen(urllib.request.Request(self.url + path, body, head, method=method), timeout=600) as response:
                    result = response.read()
                    return result if raw else json.loads(result) if result else None
            except urllib.error.HTTPError as error:
                delay = max(1, int(error.headers.get('Retry-After', 1)))
                if (error.code != 429 and not (error.code == 503 and 'Retry-After' in error.headers and (wait_cooldown or delay <= 5))) or time.monotonic() - start + delay > 600:
                    raise
                error.close()
                time.sleep(delay)

    def create(self, key, objects, group='', sort_key=''):
        return self.request('POST', self.base + '/batches', dict(idempotency_key=key, group=group, sort_key=sort_key, objects=objects))['id']

    def upload(self, batch, object_id, path):
        state = self.request('GET', self.base + '/batches/' + batch)
        offset = next(o['uploaded'] for o in state['objects'] if o['id'] == object_id)
        with open(path, 'rb') as source:
            source.seek(offset)
            while data := source.read(4 << 20):
                self.request('PUT', self.base + '/batches/' + batch + '/objects/' + object_id, data, {'Upload-Offset': str(offset)})
                offset += len(data)

    def commit(self, batch):
        return self.request('POST', self.base + '/batches/' + batch + '/commit')

    def flush(self):
        return self.request('POST', self.base + '/flush')

    def wait_remote(self, batch):
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            state = self.request('GET', self.base + '/batches/' + batch)
            if state['state'] == 'remote':
                return state
            time.sleep(0.5)
        raise TimeoutError('cloud upload did not finish')

    def download_pack(self, pack):
        for attempt in range(3):
            lease = self.request('POST', self.base + '/packs/' + pack['id'] + '/lease', wait_cooldown=attempt > 0)
            status = 0
            try:
                if lease['url']:
                    # CDN requests never contain the Vault token.
                    req = urllib.request.Request(lease['url'], headers={'Range': f"bytes=0-{pack['size']-1}"})
                    with urllib.request.urlopen(req, timeout=60) as response:
                        status = response.status
                        assert status == 206 and response.headers['Content-Range'] == f"bytes 0-{pack['size']-1}/{pack['size']}"
                        data = response.read()
                else:
                    data = self.request('GET', lease['proxy_url'], raw=True)
                assert len(data) == pack['size'] and hashlib.sha256(data).hexdigest() == pack['sha256']
                return data
            except urllib.error.HTTPError as error:
                status = error.code
                if status not in (401, 403, 404, 410) or attempt == 2:
                    raise
                error.close()
                # Finally reports the exact old URL before a paced reacquisition.
            finally:
                self.request('POST', self.base + '/leases/' + lease['id'] + '/complete', {'status': status})

    def read(self, object_id, start=0, end=None):
        # Streaming plaintext chunks; ciphertext cache capped at 128 MiB.
        plan = self.request('GET', self.base + '/objects/' + object_id + '/plan')
        end = plan['object']['size'] if end is None else end
        if 'pending_url' in plan:
            yield self.request('GET', plan['pending_url'], headers={'Range': f'bytes={start}-{end-1}'}, raw=True)
            return
        for chunk in plan['chunks']:
            off, length = chunk['plain_offset'], chunk['plain_size']
            if off + length <= start or off >= end:
                continue
            pack = plan['packs'][chunk['pack']]
            digest = pack['sha256']
            if digest not in self.cache:
                data = self.download_pack(pack)
                self.cache[digest] = data
                while sum(map(len, self.cache.values())) > 128 << 20:
                    self.cache.popitem(last=False)
            self.cache.move_to_end(digest)
            data = self.cache[digest][chunk['cipher_offset']:chunk['cipher_offset'] + chunk['cipher_size']]
            plain = AESGCM(base64.b64decode(chunk['key'])).decrypt(data[:12], data[12:], base64.b64decode(chunk['aad']))
            yield plain[max(0, start-off):min(length, end-off)]
