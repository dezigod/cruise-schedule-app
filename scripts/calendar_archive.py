"""Keep last published scheduled listings by date; never infer actual visits/empty ports."""
import base64, copy, json, hashlib
from pathlib import Path

def build_archives(calendars):
    # Ordered oldest to newest. Whole-date replacement prevents resurrecting a
    # cancelled/revised row; absent dates are not evidence of a zero-call day.
    dates={}
    metadata={}
    for calendar in calendars:
        payload=json.loads(base64.b64decode(calendar['payload']))
        days={d['date']:d for d in payload['days']}
        for day,row in days.items():
            for ship in row['ships']:
                details=ship.setdefault('listingDetails',{})
                matches=[d for d in calendar['documents'] if d['url']==ship.get('sourceURL')]
                digest=details.get('documentHash') or (matches[0]['sha256'] if len(matches)==1 else None)
                if not digest: continue
                key=(day,ship['name'].strip().casefold(),digest,ship.get('arrival'),ship.get('departure'),ship.get('rawDock',ship.get('dock')))
                # Only non-status facts from the exact immutable source row may
                # supplement a newer transport payload using an older schema.
                allowed={'originalVesselName','sourceObtainedAt','sourceIssueDate','sourceRevision','sourcePage','sourceRow','previousPort','nextPort','cruiseLine','cruiseLineSourceURL'}
                saved=metadata.get(key,{})
                for field in allowed:
                    if details.get(field) is None and saved.get(field) is not None: details[field]=saved[field]
                metadata[key]={k:v for k,v in details.items() if k in allowed and v is not None}
        visits={}
        for visit in calendar.get('visits',[]): visits.setdefault(visit['date'],[]).append(visit)
        for day in set(days)|set(visits):
            dates[day]=(copy.deepcopy(days.get(day,{'date':day,'ships':[]})),copy.deepcopy(visits.get(day,[])),calendar)
    result={}
    for year in sorted({int(d[:4]) for d in dates}):
        selected={d:v for d,v in dates.items() if d.startswith(str(year)+'-')}
        documents={}; days=[]; visits=[]; held=set(); conflicts=set()
        for day,(row,items,calendar) in sorted(selected.items()):
            mapping={}
            for doc in calendar['documents']:
                identifier='document-'+doc['sha256']+'-'+hashlib.sha256(doc['url'].encode()).hexdigest()[:12]
                mapping[doc['id']]=identifier
                candidate=dict(doc,id=identifier)
                if identifier not in documents or candidate['checkedAt']<documents[identifier]['checkedAt']:
                    documents[identifier]=candidate
            for ship in row['ships']:
                if ship.get('sourceDocumentIDs'): ship['sourceDocumentIDs']=[mapping[i] for i in ship['sourceDocumentIDs']]
                details=ship.setdefault('listingDetails',{})
                for candidate in details.get('candidates',[]):
                    if candidate.get('documentID') in mapping: candidate['documentID']=mapping[candidate['documentID']]
                if details.get('documentID') in mapping:
                    details['documentID']=mapping[details['documentID']]
                else:
                    matches=[d for d in calendar['documents'] if d['url']==ship.get('sourceURL')]
                    if len(matches)==1:
                        doc=matches[0]
                        details.update(documentID=mapping[doc['id']],documentHash=doc['sha256'],sourceCheckedAt=doc['checkedAt'])
            for visit in items:
                for candidate in visit['candidates']: candidate['documentID']=mapping[candidate['documentID']]
            days.append(row); visits.extend(items)
            if day in calendar.get('heldDays',[]): held.add(day)
            if day in calendar.get('conflictDays',[]): conflicts.add(day)
        # Only documents actually referenced by retained records are needed.
        used={c['documentID'] for v in visits for c in v['candidates']}
        used|={s.get('listingDetails',{}).get('documentID') for d in days for s in d['ships']}
        used|={c['documentID'] for d in days for s in d['ships'] for c in s.get('listingDetails',{}).get('candidates',[])}
        docs=[v for k,v in documents.items() if k in used] or list(documents.values())
        payload={'days':days,'lastUpdated':min(d['checkedAt'] for d in docs),'sources':sorted({d['url'] for d in docs}),'coverage':{'island':'St. Thomas','complete':False}}
        archive={'schemaVersion':1,'delivery':'hosted','archiveYear':year,'windowStart':f'{year}-01-01T04:00:00Z','windowEnd':f'{year}-12-31T04:00:00Z','documents':docs,'payload':base64.b64encode(json.dumps(payload).encode()).decode(),'visits':visits,'heldDays':sorted(held),'conflictDays':sorted(conflicts),'notes':['Historical published schedules, not confirmed arrivals or attendance. Missing dates do not prove no calls. Last known dated records retained; later published records replace the same date.']}
        if len(docs)>24 or len(json.dumps(archive).encode())>3_000_000: raise ValueError('Archive exceeds supported client limits; retain prior archive and review partitioning')
        result[f'cruise-archive-{year}.json']=archive
    return result

def archives_for_checkout(root, calendar, supplements=None):
    root=Path(root)
    old=[json.loads(p.read_text()) for p in sorted(root.glob('cruise-archive-*.json'))]
    previous=root/'official-cruise-calendar-v1.json'
    if previous.exists(): old.append(json.loads(previous.read_text()))
    return build_archives((supplements or [])+old+[calendar])
