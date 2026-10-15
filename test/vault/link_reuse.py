"""Distinguish provider URL resolution from arbitrary concurrent CDN reads."""
import concurrent.futures
import hashlib
import json
import sqlite3
import time
import urllib.error
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def call(url, method='GET', body=None, headers=None):
    opener=urllib.request.build_opener(NoRedirect)
    try:
        response=opener.open(urllib.request.Request(url,body,headers or {},method=method),timeout=60)
    except urllib.error.HTTPError as error:
        response=error
    with response:
        return response.status,dict(response.headers),response.read()


def run(lab, request):
    clients=lab.clients
    plans=[clients[s].request('GET',clients[s].base+'/objects/'+object_id+'/plan') for s,object_id in [('notes','n000-json'),('photos','photo-0000'),('files','backup')]]
    packs=[next(iter(p['packs'].values())) for p in plans]
    spaces=['notes','photos','files']
    before=request(lab.gateway+'/lab/status')
    first=clients['notes'].request('POST',clients['notes'].base+'/packs/'+packs[0]['id']+'/lease')
    assert request(lab.gateway+'/lab/status')['links']==before['links']+1
    # A different uncached pack consumes budget; the current pack does not.
    code,_,_=call(lab.url+clients['photos'].base+'/packs/'+packs[1]['id']+'/lease','POST',headers={'Authorization':'Bearer '+lab.tokens['photos']})
    assert code==429,code
    code,_,body=call(lab.url+clients['notes'].base+'/packs/'+packs[0]['id']+'/lease','POST',headers={'Authorization':'Bearer '+lab.tokens['notes']})
    assert code==201,code
    duplicate=json.loads(body)
    assert duplicate['url']==first['url'] and request(lab.gateway+'/lab/status')['links']==before['links']+1
    handles=[('notes',first),('notes',duplicate)]
    for space,pack in zip(spaces[1:],packs[1:]):
        handles.append((space,clients[space].request('POST',clients[space].base+'/packs/'+pack['id']+'/lease')))
    issued=request(lab.gateway+'/lab/status')['links']
    request(lab.gateway+'/lab/control','POST',{'download_delay_ms':1800})
    def download(handle):
        status,_,data=call(handle['url'],headers={'Range':'bytes=0-65535'})
        assert status==206 and len(data)==65536
        return data
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        chunks=list(pool.map(download,[handle for _,handle in handles]))
    assert chunks[0]==chunks[1]
    concurrent_stats=request(lab.gateway+'/lab/status')
    assert concurrent_stats['max_active_downloads']>=4,concurrent_stats
    # Reusing old URLs later does not call OpenList or the provider link API.
    assert download(first)==chunks[0]
    assert request(lab.gateway+'/lab/status')['links']==issued
    # The same authorized proxy handle also permits overlapping/repeated reads.
    def proxy(_):
        status,_,data=call(lab.url+first['proxy_url'],headers={'Authorization':'Bearer '+lab.tokens['notes']})
        assert status==200 and hashlib.sha256(data).hexdigest()==packs[0]['sha256']
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(proxy,range(2)))
    assert request(lab.gateway+'/lab/status')['links']==issued
    request(lab.gateway+'/lab/control','POST',{})
    for space,handle in handles:
        clients[space].request('POST',clients[space].base+'/leases/'+handle['id']+'/complete',{'status':206})
    # Simulate the released singleton schema to exercise a real upgrade.
    survivor=clients['notes'].request('POST',clients['notes'].base+'/packs/'+packs[0]['id']+'/lease')
    lab.stop(lab.vault)
    with sqlite3.connect(lab.root/'vault/vault.db') as db:
        db.execute("CREATE TABLE lease(id INTEGER PRIMARY KEY CHECK(id=1),lease_id TEXT NOT NULL,space TEXT NOT NULL,pack TEXT NOT NULL,owner TEXT NOT NULL,url TEXT NOT NULL,expires INTEGER NOT NULL,started INTEGER NOT NULL DEFAULT 0)")
        db.execute('INSERT INTO lease(id,lease_id,space,pack,owner,url,expires) SELECT 1,lease_id,space,pack,owner,url,expires FROM read_sessions WHERE lease_id=?',(survivor['id'],))
        db.execute('DROP TABLE read_sessions')
    lab.stop(lab.gw);lab.start_gateway();lab.start_vault()
    clients['notes'].request('POST',clients['notes'].base+'/leases/'+survivor['id']+'/renew')
    for space,pack,old in zip(spaces,packs,[first,handles[2][1],handles[3][1]]):
        handle=clients[space].request('POST',clients[space].base+'/packs/'+pack['id']+'/lease')
        assert handle['url']==old['url']
        clients[space].request('POST',clients[space].base+'/leases/'+handle['id']+'/complete',{'status':206})
    assert request(lab.gateway+'/lab/status')['links']==0,'restart regenerated cached URLs'
    clients['notes'].request('POST',clients['notes'].base+'/leases/'+survivor['id']+'/complete',{'status':206})
    # Expired URL: simultaneous callers cause exactly one fresh resolution.
    payload=b'expiration-probe'*65536
    key=hashlib.sha256(payload).hexdigest()
    url=lab.gateway+'/seaweedfs/v1/files/objects/'+key
    header={'Authorization':'Bearer local-lab-files-token'}
    assert call(url,'PUT',payload,header)[0]==201
    request(lab.gateway+'/lab/control','POST',{'link_ttl_ms':750})
    assert call(url,headers=header)[0]==302
    initial=request(lab.gateway+'/lab/status')['links']
    assert call(url,headers=header)[0]==302
    assert request(lab.gateway+'/lab/status')['links']==initial
    time.sleep(1.1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        responses=list(pool.map(lambda _:call(url,headers=header)[0],range(5)))
    assert responses==[302]*5,responses
    assert request(lab.gateway+'/lab/status')['links']==initial+1
    request(lab.gateway+'/lab/control','POST',{})
    # The real 115 behavior: no advertised expiry. Old links suddenly fail.
    # Separate clients report the same old URL, then share one replacement.
    def recovered_read(_):
        independent=type(clients['notes'])(lab.url,'notes',lab.tokens['notes'])
        return hashlib.sha256(b''.join(independent.read('n000-json'))).hexdigest()
    for expired_status in (410,403,403):
        request(lab.gateway+'/lab/control','POST',{'expire_links':True,'expired_status':expired_status})
        before_refresh=request(lab.gateway+'/lab/status')['links']
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            results=list(pool.map(recovered_read,range(5)))
        assert results==[plans[0]['object']['sha256']]*5,results
        assert request(lab.gateway+'/lab/status')['links']==before_refresh+1,'failed URLs caused duplicate provider resolutions'
    request(lab.gateway+'/lab/control','POST',{'cdn_status':410})
    before_failure=request(lab.gateway+'/lab/status')['links']
    try:
        recovered_read(0)
    except urllib.error.HTTPError as error:
        assert error.code==410,error.code
    else:
        raise AssertionError('permanent rejection must stop retrying')
    assert request(lab.gateway+'/lab/status')['links']==before_failure+2,'refresh retries must be bounded'
    request(lab.gateway+'/lab/control','POST',{})
    return {'parallel_downloads':concurrent_stats['max_active_downloads'],'fresh_links_for_three_packs':issued-before['links'],'links_after_restart':0,'known_expiry_refreshes':1,'unknown_expiry_refreshes_for_five_clients':1,'bounded_refresh_retries':True,'repeated_direct_and_proxy_downloads':True,'legacy_session_migration':True}
