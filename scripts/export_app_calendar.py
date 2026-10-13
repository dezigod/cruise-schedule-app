"""Export private, unreviewed official listings for the development app.
Never creates reviewed zero-call coverage or modifies schedule.json.
"""
import argparse, base64, json, re
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from official_fetch import private_directory, save_private, validate_url
from official_parse import reconcile

def export_calendar(documents, catalog, start, end, now=None):
    now = now or datetime.now(timezone.utc)
    if end < start or (end-start).days > 730: raise ValueError('Invalid window')
    if catalog.get('schemaVersion') != 1: raise ValueError('Invalid capacity catalog')
    capacities = {}
    for record in catalog['records']:
        key = record['ship'].casefold().strip()
        if key in capacities: raise ValueError('Duplicate capacity identity')
        capacities[key] = record
    docs=[]; days={}; held=set(); notes=[]
    conflicts=sorted({c['localDate'] for c in reconcile(documents) if c['status']=='conflicting' and c['island']=='stThomas' and start.isoformat()<=c['localDate']<=end.isoformat()})
    for doc in documents:
        validate_url(doc['provider'], doc['url'])
        if not re.fullmatch('[a-f0-9]{64}',doc['sha256']): raise ValueError('Invalid document hash')
        if any(not c['localDate'] for c in doc['calls']): raise ValueError('Unresolved source date')
        stamp=doc.get('checkedAt') or doc.get('obtainedAt')
        if not stamp: raise ValueError('Missing source check time')
        checked_at=datetime.fromisoformat(stamp.replace('Z','+00:00'))
        if checked_at.tzinfo is None or checked_at > now: raise ValueError('Invalid source check time')
        stamp=checked_at.astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
        # Parser retains acquisition timestamps on the document.
        docs.append({'id':doc['id'],'url':doc['url'],'sha256':doc['sha256'],'checkedAt':stamp,'obtainedAt':doc.get('obtainedAt')})
        notes.append(doc['id']+': '+doc['url']+'; original obtained '+str(doc.get('obtainedAt'))+'; last HTTP validation '+stamp+'; SHA256 '+doc['sha256'])
        for issue in doc.get('issues',[]):
            if issue['kind']=='monthly_total_mismatch':
                month=issue['month']; cursor=start
                while cursor<=end:
                    if cursor.strftime('%Y-%m')==month: held.add(cursor.isoformat())
                    cursor+=timedelta(days=1)
                notes.append('Official monthly totals do not reconcile for '+month+'; all dates in that month are held for review.')
        for call in doc['calls']:
            local=call['localDate']
            if not local or not start.isoformat() <= local <= end.isoformat(): continue
            if call['island'] not in ('stThomas','unknown'): continue
            if call['flags'] or call['island']=='unknown': held.add(local)
            if call['island']!='stThomas': continue
            record=capacities.get(call['canonicalVessel'].casefold().strip())
            capacity=None
            if record and record['status']=='sourced' and record['basis']=='doubleOccupancy':
                value=record['value']; parsed=urlsplit(record['sourceURL'])
                checked=datetime.fromisoformat(record['checkedAt'].replace('Z','+00:00'))
                if type(value) is not int or not 0<value<20000 or parsed.scheme!='https' or parsed.hostname not in ['www.ncl.com','www.royalcaribbean.com','www.carnival.com'] or parsed.username or parsed.password: raise ValueError('Invalid capacity evidence')
                if checked > now: raise ValueError('Future capacity check')
                if (now-checked).days<=365: capacity=value
            display = record['ship'] if record else {
                'norwegian getaway':'Norwegian Getaway', 'queen mary 2':'Queen Mary 2',
                'seadream i':'SeaDream I', 'seadream ii':'SeaDream II'
            }.get(call['canonicalVessel'],call['originalVesselName'])
            row={'name':display,'passengers':capacity,'island':'St. Thomas',
                 'dock':call['dock'],'rawDock':call['rawDock'],'arrival':call['arrival'],'departure':call['departure'],
                 'sourceName':doc['provider'].upper(),'sourceURL':doc['url']}
            days.setdefault(local,[]).append(row)
    if not 1<=len(docs)<=6: raise ValueError('Invalid document count')
    for record in capacities.values():
        notes.append(record['ship']+': '+str(record['value'])+' ('+record['basis']+', '+record['status']+'). '+record['sourceURL'])
    # Duplicate/conflicting variants are retained for the app reconciler.
    checked=min(d['checkedAt'] for d in docs)
    payload={'days':[{'date':d,'ships':rows} for d,rows in sorted(days.items())],
             'lastUpdated':checked,'sources':[d['url'] for d in docs],
             'coverage':{'island':'St. Thomas','complete':False}}
    def midnight(d): return d.isoformat()+'T04:00:00Z'
    return {'schemaVersion':1,'windowStart':midnight(start),'windowEnd':midnight(end),'documents':docs,
            'payload':base64.b64encode(json.dumps(payload).encode()).decode(),'heldDays':sorted(held),'conflictDays':conflicts,'notes':notes}

def main():
    parser=argparse.ArgumentParser()
    for name in ['documents','catalog','output','start','end']: parser.add_argument('--'+name,required=True)
    args=parser.parse_args()
    documents=json.loads(Path(args.documents).read_text())
    value=export_calendar(documents,json.loads(Path(args.catalog).read_text()),date.fromisoformat(args.start),date.fromisoformat(args.end))
    output=private_directory(args.output)
    save_private(output/'official-cruise-calendar-v1.json',json.dumps(value,indent=2).encode())
    print(json.dumps({'documents':len(value['documents']),'heldDays':len(value['heldDays']),'output':str(output)}))
if __name__=='__main__': main()
