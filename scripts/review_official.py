"""Stage 1: private candidate review only. Default execution is strictly offline.

No feed writer, review approval, app activation, scheduler or VINow collector.
"""
from __future__ import annotations
import argparse
import csv
import io
import json
from collections import Counter
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path

from audit_schedule import location

from official_fetch import (CollectionBlocked, OfficialFetcher, PROVIDERS, discover_pdfs,
                            private_directory, save_json, save_private, validate_url)
from official_parse import AST, candidate_calendar, parse_pdf

PROFILES = {'wico_monthly':'wico', 'wico_fiscal':'wico', 'vipa_fiscal':'vipa'}

def normalized_record(record):
    headers = record.get('headers', {})
    return {**record, 'obtainedAt':record.get('obtainedAt',record['checkedAt']),
            'etag':record.get('etag',headers.get('ETag')),
            'lastModified':record.get('lastModified',headers.get('Last-Modified')),
            'nextCheckDueAt':record.get('nextCheckDueAt')}

def iso_seconds(value):
    return datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')

def local_stamp(day, time='00:00'):
    return datetime.fromisoformat(day+'T'+time).replace(tzinfo=AST).astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')

def candidate_import(documents):
    """Reuse the app's ReviewedSourceImport schema. Scope stays in a private sidecar.

    Suspicious rows are quarantined, not corrected. Every valid source row is kept,
    including contradictions; no preferred berth, capacity or human review stamp.
    """
    calls=[]; quarantined=[]
    for doc in documents:
        source={'authority':doc['provider'],'url':doc['url'],'obtainedAt':iso_seconds(doc['obtainedAt']),
                # The app's scalar timestamp cannot express day-only precision.
                # Preserve printed issueDate in the document sidecar; never invent midnight publication.
                'issuedAt':None,
                'reviewState':'candidate','reviewedAt':None,'reviewerAlias':None,'rightsScope':'localReviewOnly'}
        for call in doc['calls']:
            if call['flags'] or not call['localDate']:
                quarantined.append({'documentID':doc['id'],'page':call['provenance']['page'],
                                    'row':call['provenance']['row'],'flags':call['flags']}); continue
            calls.append({'shipIdentity':call['canonicalVessel'],'island':call['island'],
                          'localDay':local_stamp(call['localDate']), 'dock':call['dockCode'],
                          'arrival':local_stamp(call['localDate'],call['arrival']) if call['arrival'] else None,
                          'departure':local_stamp(call['localDate'],call['departure']) if call['departure'] else None,
                          'guestCapacity':None,'source':source})
    return {'schemaVersion':1,'cruiseCalls':calls,'waterRecords':[],'ferryRecords':[]}, quarantined

def source_diff(documents, previous):
    def signature(call):
        return (call['localDate'],call['island'],call['canonicalVessel'],call['rawDock'],call['arrival'],call['departure'])
    old={d['id']:d for d in previous}; results=[]
    for doc in documents:
        before=old.get(doc['id']); changed=bool(before and before['sha256']!=doc['sha256'])
        removed=sorted(set(map(signature,before['calls']))-set(map(signature,doc['calls'])),key=str) if before else []
        added=sorted(set(map(signature,doc['calls']))-set(map(signature,before['calls'])),key=str) if before else []
        results.append({'documentID':doc['id'],'sha256':doc['sha256'],'previousSHA256':before['sha256'] if before else None,
                        'revisionChanged':changed,'invalidatesPriorReview':changed,'removedOrChangedRows':removed,
                        'addedOrChangedRows':added,'removedRowsMeaning':'Possible cancellation or source change; human review required',
                        'previousRowCount':len(before['calls']) if before else None,'candidateRowCount':len(doc['calls']),
                        'approvalRequired':True})
    for identifier, doc in old.items():
        if identifier not in {d['id'] for d in documents}:
            results.append({'documentID':identifier,'sourceNowMissing':True,'invalidatesPriorReview':True,'approvalRequired':True})
    return results

def comparison_context(feed):
    """Read-only unofficial context, never port coverage or forecasting input."""
    by_date={}
    for day in feed.get('days',[]):
        counts=Counter(location(call)[0] for call in day.get('ships',[]))
        by_date[day['date']]={'listedRows':sum(counts.values()),'rowsByIsland':dict(counts),
                              'authority':'unofficial','provesStThomasCoverage':False}
    return by_date

def write_outputs(output, documents, start, end, previous=(), comparison_feed=None):
    output=private_directory(output)
    rows,resolutions=candidate_calendar(documents,start,end)
    context=comparison_context(comparison_feed or {})
    for row in rows:
        row['unofficialComparison']=context.get(row['localDate'])
        listed=row['unofficialComparison']
        if listed and not row['parsedUniqueStThomasCalls'] and not listed['rowsByIsland'].get('St. Thomas'):
            row['blockers'].append('unofficial_other_island_or_unknown_rows_do_not_prove_st_thomas_coverage')
            if row['candidateKind']=='empty_day_scope_review': row['candidateKind']='other_island_only_scope_review'
    imported,quarantined=candidate_import(documents)
    conflicts=[r for r in resolutions if r['conflicts']]
    duplicates=[{'island':r['island'],'localDate':r['localDate'],'canonicalVessel':r['canonicalVessel'],
                 'matchingDuplicateRows':r['matchingDuplicateRows'],'sourceRowCount':r['sourceRowCount']}
                for r in resolutions if r['matchingDuplicateRows']]
    report={'stage':1,'productionEnabled':False,'recurringCollectionEnabled':False,
            'humanApprovalRequired':True,'calendarDays':len(rows),'sourceRows':sum(len(d['calls']) for d in documents),
            'uniqueCandidateCalls':len(resolutions),'conflictingCalls':len(conflicts),
            'matchingDuplicateRows':sum(r['matchingDuplicateRows'] for r in resolutions),
            'quarantinedRows':len(quarantined),'importCandidateRows':len(imported['cruiseCalls']),
            'emptyDayCandidates':sum(r['candidateKind']=='empty_day_scope_review' for r in rows),
            'reviewedNoCallDays':0, 'scheduledDemandNotAttendance':True,
            'publicationPrecisionNote':'Printed issue dates remain in source sidecars; candidate import has no invented exact publication timestamp',
            'rightsStatus':'Owner attested collection/commercial/full-date use; derived forecasts, attribution, request cadence and redistribution still gated',
            'documents':[{'id':d['id'],'url':d['url'],'sha256':d['sha256'],'pages':d['pageCount'],
                          'period':d['declaredPeriod'],'revision':d['revisionIdentifier'],'issueDate':d['issueDate'],
                          'obtainedAt':d['obtainedAt'],'checkedAt':d['checkedAt'],'nextCheckDueAt':d.get('nextCheckDueAt'),
                          'calls':len(d['calls']),'callsByIsland':dict(Counter(c['island'] for c in d['calls'])),
                          'representedMonths':d['representedMonths'],'missingMonths':d['missingMonths'],
                          'portCodesSeen':d['portCodesSeen'],
                          'issuesByKind':dict(Counter(i['kind'] for i in d['issues']))} for d in documents]}
    for name,value in [('parsed-documents',documents),('candidate-calendar',rows),('conflicts',conflicts),
                       ('duplicate-report',duplicates),('candidate-reviewed-source-import',imported),
                       ('quarantined-import-rows',quarantined),('source-diff',source_diff(documents,previous)),('quality-summary',report)]:
        save_json(output/(name+'.json'),value)
    stream=io.StringIO(); writer=csv.writer(stream)
    writer.writerow(['AST date','state','candidate kind','parsed unique St Thomas rows','confirmed scheduled count',
                     'port','authority','source IDs','parsing status','scope verified','blockers'])
    for row in rows:
        for port,item in row['portCoverage'].items():
            writer.writerow([row['localDate'],row['state'],row['candidateKind'],row['parsedUniqueStThomasCalls'],'unknown',
                             port,item['authority'],';'.join(item['documents']),item['parsingStatus'],False,';'.join(row['blockers'])])
    save_private(output/'date-port-coverage.csv',stream.getvalue().encode())
    # Local, dependency-free disclosure report; nothing is sent to a website or app.
    body=[]
    for row in rows:
        ports=''.join(f"<li>{escape(p)}: {escape(v['parsingStatus'])}; scope unverified</li>" for p,v in row['portCoverage'].items())
        day_conflicts=''.join('<li>'+escape(r['canonicalVessel'])+': '+escape(', '.join(r['conflicts']))+'</li>' for r in conflicts if r['localDate']==row['localDate'])
        evidence=''.join('<tr>'+''.join('<td>'+escape(str(v))+'</td>' for v in (c['originalVesselName'],c['island'],c['rawDock'],c['arrival'],c['departure'],d['id'],c['provenance']['page'],','.join(c['flags'])))+'</tr>' for d in documents for c in d['calls'] if c['localDate']==row['localDate'])
        body.append(f"<details><summary>{escape(row['localDate'])} · {escape(row['candidateKind'].replace('_',' '))} · {row['parsedUniqueStThomasCalls']} parsed St. Thomas calls · {escape(row['state'])}</summary><ul>{ports}</ul><p>Confirmed schedule count: unknown. Human review pending.</p><ul>{day_conflicts}</ul><table><tr><th>Original vessel</th><th>Island</th><th>Original berth</th><th>Arrival</th><th>Departure</th><th>Source</th><th>Page</th><th>Flags</th></tr>{evidence}</table><p>{escape(', '.join(row['blockers']).replace('_',' '))}</p></details>")
    source_list=''.join('<li>'+escape(d['id'])+' · '+escape(d['revisionIdentifier'] or 'revision unknown')+' · issue '+escape(d['issueDate'] or 'unknown')+' · obtained '+escape(d['obtainedAt'])+' · checked '+escape(d['checkedAt'])+' · <a href="'+escape(Path(d['path']).as_uri(),quote=True)+'">private original</a> · SHA-256 '+escape(d['sha256'])+'</li>' for d in documents)
    html='<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Private official cruise candidates</title><style>body{background:#111827;color:#f3f4f6;font:16px system-ui;max-width:1000px;margin:40px auto;padding:16px}details{padding:12px;border-bottom:1px solid #374151}summary{cursor:pointer}p{color:#cbd5e1}a{color:#93c5fd}table{font-size:13px;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #374151;text-align:left}li{overflow-wrap:anywhere}</style><h1>Private official cruise candidates</h1><p>Stage 1. No human approvals. No production activation. No recurring collection. Counts below describe extracted schedule rows, never attendance or observed crowds. Empty-day candidates require all port scopes and revisions to be verified. Unknown stays unknown.</p><details><summary>Source versions, acquisition and review limits</summary><ul>'+source_list+'</ul><p>Publication dates remain unknown where not printed. Next review deadline, full port completeness, attribution and derived forecast permission require human confirmation. Source PDFs and this report remain private.</p></details>'+''.join(body)
    save_private(output/'candidate-calendar.html',html.encode())
    return report

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',required=True,help='Private acquisition records (offline default)')
    parser.add_argument('--selection',required=True,help='Private explicit id -> supported parser profile JSON')
    parser.add_argument('--output',required=True,help='Private directory outside all Git checkouts')
    parser.add_argument('--start',type=date.fromisoformat,required=True); parser.add_argument('--end',type=date.fromisoformat,required=True)
    parser.add_argument('--previous',help='Prior private parsed-documents.json for review diff')
    parser.add_argument('--comparison-feed',help='Optional saved unofficial feed, read-only context; never coverage')
    parser.add_argument('--fetch-once',action='store_true',help='Explicitly request the approved one-shot acquisition; never recurring')
    parser.add_argument('--authorization',help='Private owner attestation needed only for explicit fetching')
    args=parser.parse_args(argv); output=private_directory(args.output)
    if args.end < args.start or (args.end-args.start).days>730: raise CollectionBlocked('Calendar range exceeds the bounded local review horizon')
    records=[normalized_record(r) for r in json.loads(Path(args.manifest).read_text())]
    selected=json.loads(Path(args.selection).read_text()); by_id={r['id']:r for r in records}
    if not isinstance(selected,dict) or len(selected)>6: raise CollectionBlocked('Select at most six source documents explicitly')
    for identifier,profile in selected.items():
        if profile not in PROFILES or identifier not in by_id or by_id[identifier]['provider']!=PROFILES[profile]:
            raise CollectionBlocked('Unsupported source/profile selection')
    if args.fetch_once:
        if not args.authorization: raise CollectionBlocked('Explicit private owner attestation required')
        fetcher=OfficialFetcher(output/'acquisition',json.loads(Path(args.authorization).read_text()))
        fetcher.seed(records)
        for provider in sorted({PROFILES[p] for p in selected.values()}):
            listing=fetcher.fetch(provider,PROVIDERS[provider]['listing'],provider+'-listing',pdf=False,one_shot=True)
            by_id[provider+'-listing']=listing
            links=discover_pdfs(provider,Path(listing['path']).read_text()); save_json(output/(provider+'-discovery.json'),links)
            published={link['url'] for link in links}
            for identifier,profile in selected.items():
                if PROFILES[profile]!=provider: continue
                record=by_id[identifier]
                if record['url'] not in published: raise CollectionBlocked('Selected PDF is not linked by the current official page')
                by_id[identifier]=fetcher.fetch(provider,record['url'],identifier,one_shot=True)
        save_json(output/'acquisition-manifest.json',list(by_id.values()))
    documents=[]
    for identifier,profile in selected.items():
        record=by_id[identifier]; validate_url(record['provider'],record['url'])
        path=Path(record['path']).resolve()
        if any((p/'.git').exists() for p in [path.parent,*path.parents]): raise CollectionBlocked('Original must be private and outside Git')
        documents.append(parse_pdf(path,record,profile))
    previous=json.loads(Path(args.previous).read_text()) if args.previous else []
    comparison=json.loads(Path(args.comparison_feed).read_text()) if args.comparison_feed else None
    report=write_outputs(output,documents,args.start,args.end,previous,comparison)
    print(json.dumps({'stage':1,'productionEnabled':False,'calendarDays':report['calendarDays'],
                      'sourceRows':report['sourceRows'],'conflictingCalls':report['conflictingCalls'],
                      'reviewedNoCallDays':0,'output':str(output)}))
    return 0

if __name__=='__main__':
    try: raise SystemExit(main())
    except (CollectionBlocked, ValueError, KeyError, OSError) as error:
        # Source document text is private; emit only an error class, never parser input.
        print(json.dumps({'stage':1,'status':'blocked','errorType':type(error).__name__,'productionEnabled':False}))
        raise SystemExit(1)
