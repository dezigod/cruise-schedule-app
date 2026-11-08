import base64,copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from calendar_archive import build_archives

class ArchiveTests(unittest.TestCase):
    def sample(self):
        return json.loads((Path(__file__).resolve().parents[1]/'official-cruise-calendar-v1.json').read_text())
    def test_year_limits_and_provenance_and_repeat_are_stable(self):
        value=self.sample(); archives=build_archives([value])
        again=build_archives(list(archives.values())+[value])
        self.assertEqual(archives,again)
        for name,a in archives.items():
            year=str(a['archiveYear']); self.assertEqual(a['delivery'],'hosted')
            self.assertTrue(all(v['date'].startswith(year) for v in a['visits']))
            docs={(d['id'],d['sha256']) for d in a['documents']}
            self.assertTrue(all((c['documentID'],c['documentHash']) in docs for v in a['visits'] for c in v['candidates']))
            self.assertFalse(json.loads(base64.b64decode(a['payload']))['coverage']['complete'])
    def test_date_rolloff_preserves_older_records(self):
        old=self.sample(); new=copy.deepcopy(old)
        payload=json.loads(base64.b64decode(new['payload'])); first=payload['days'][0]['date']
        payload['days']=payload['days'][1:];new['payload']=base64.b64encode(json.dumps(payload).encode()).decode()
        new['visits']=[v for v in new['visits'] if v['date']!=first]
        archives=build_archives([old,new]); result=archives['cruise-archive-'+first[:4]+'.json']
        self.assertTrue(any(v['date']==first for v in result['visits']))
    def test_explicit_cancellation_replaces_date_not_union(self):
        old=self.sample(); new=copy.deepcopy(old); day=new['visits'][0]['date']
        visit=new['visits'][0];visit['visitStatus']='cancelled';new['visits']=[visit]
        new['payload']=base64.b64encode(json.dumps({'days':[]}).encode()).decode()
        a=build_archives([old,new])['cruise-archive-'+day[:4]+'.json']
        rows=[v for v in a['visits'] if v['date']==day]
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['visitStatus'],'cancelled')
        self.assertEqual(next(d for d in json.loads(base64.b64decode(a['payload']))['days'] if d['date']==day)['ships'],[])
    def test_durable_stage_and_atomic_delivery_include_archives(self):
        from unittest.mock import patch
        from github_state import GitHubState,PATH,STAGED,FEED,BRANCH,blob_id,json_text
        remote=GitHubState('dezigod/cruise-schedule-app','synthetic');remote.sha='a'*40
        branches={BRANCH:{PATH:remote.sha},'main':{FEED:'b'*40}}
        for p in Path(__file__).resolve().parents[1].glob('cruise-archive-*.json'):
            branches['main'][p.name]=blob_id(p.read_bytes())
        def commit(branch,tip,tree,changes,message):
            for name,change in changes.items(): branches[branch][name]=change.get('sha') or blob_id(change['content'].encode())
        with patch.object(remote,'snapshot',side_effect=lambda branch:('tip','tree',branches[branch])),patch.object(remote,'commit_files',side_effect=commit),patch.object(remote,'save'):
            state=remote.stage({'providers':{}},self.sample(),'b'*40)
            self.assertTrue(state['pendingDelivery']['archives'])
            self.assertEqual(remote.deliver(copy.deepcopy(state)),'published')
            for name,sha in state['pendingDelivery']['archives'].items():self.assertEqual(branches['main'][name],sha)
            self.assertEqual(remote.deliver(copy.deepcopy(state)),'already published')
    def test_metadata_only_supplements_exact_source_row(self):
        old=self.sample(); payload=json.loads(base64.b64decode(old['payload'])); row=payload['days'][0]['ships'][0]
        row['listingDetails']['previousPort']='Supported port';old['payload']=base64.b64encode(json.dumps(payload).encode()).decode()
        newer=copy.deepcopy(old);np=json.loads(base64.b64decode(newer['payload']));np['days'][0]['ships'][0]['listingDetails'].pop('previousPort')
        newer['payload']=base64.b64encode(json.dumps(np).encode()).decode()
        year=payload['days'][0]['date'][:4]
        archive=build_archives([old,newer])['cruise-archive-'+year+'.json']
        got=json.loads(base64.b64decode(archive['payload']))['days'][0]['ships'][0]
        self.assertEqual(got['listingDetails']['previousPort'],'Supported port')
        np['days'][0]['ships'][0]['arrival']='23:59';newer['payload']=base64.b64encode(json.dumps(np).encode()).decode()
        archive=build_archives([old,newer])['cruise-archive-'+year+'.json']
        got=json.loads(base64.b64decode(archive['payload']))['days'][0]['ships'][0]
        self.assertNotIn('previousPort',got['listingDetails'])
    def test_same_hash_different_urls_and_nested_candidate_references(self):
        old=self.sample(); new=copy.deepcopy(old)
        original=new['documents'][0]; oldurl=original['url'];original['url']=oldurl+'?edition=alternate'
        np=json.loads(base64.b64decode(new['payload']))
        day=np['days'][0]['date'];np['days']=np['days'][:1];new['visits']=[v for v in new['visits'] if v['date']==day]
        for v in new['visits']:
            for c in v['candidates']:
                if c['sourceURL']==oldurl:c['sourceURL']=original['url']
        for ship in np['days'][0]['ships']:
            if ship['sourceURL']==oldurl:ship['sourceURL']=original['url']
        np['days'][0]['ships'][0]['listingDetails']['candidates']=copy.deepcopy(new['visits'][0]['candidates'])
        new['payload']=base64.b64encode(json.dumps(np).encode()).decode()
        a=build_archives([old,new])['cruise-archive-'+day[:4]+'.json']
        refs={(d['id'],d['url'],d['sha256']) for d in a['documents']}
        self.assertTrue(all((c['documentID'],c['sourceURL'],c['documentHash']) in refs for v in a['visits'] for c in v['candidates']))
        rows=json.loads(base64.b64decode(a['payload']))['days']
        self.assertTrue(all((c['documentID'],c['sourceURL'],c['documentHash']) in refs for d in rows for s in d['ships'] for c in s.get('listingDetails',{}).get('candidates',[])))
