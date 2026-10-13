"""Deterministic, private PDF candidates. No human review/permission is manufactured."""
from __future__ import annotations
import calendar
import hashlib
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

AST = ZoneInfo("America/St_Thomas")
MONTHS = {name.lower(): index for index, name in enumerate(calendar.month_name) if name}
MONTHS.update({name.lower(): index for index, name in enumerate(calendar.month_abbr) if name})
DOCKS = {"WICO": ("stThomas", "WICO", "havensight"),
         "AMSS": ("stThomas", "CB", "crownBay"), "AMNS": ("stThomas", "CB", "crownBay"), "AMHP": ("stThomas", "CB", "crownBay"),
         "IANCH": ("stThomas", "IANCH", "stThomasAnchorages"), "OANCH": ("stThomas", "OANCH", "stThomasAnchorages"),
         "STJ": ("stJohn", "STJ", "stJohn")}
DATE_PATTERN = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{2}|\d{4})$")
PORT_SCOPES = ["havensight", "crownBay", "stThomasAnchorages"]

def parse_day(value):
    match = DATE_PATTERN.fullmatch(value.strip())
    if not match: return None
    d, month, year = match.groups(); year = int(year) + (2000 if len(year) == 2 else 0)
    return date(year, MONTHS[month.lower()], int(d))

def parse_time(value):
    text = value.strip().upper().replace(".", "")
    match = re.fullmatch(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", text)
    if not match: return None
    hour, minute, suffix = match.groups(); hour, minute = int(hour), int(minute)
    if suffix:
        if not 1 <= hour <= 12: return None
        hour = hour % 12 + (12 if suffix == "PM" else 0)
    if not 0 <= hour <= 23 or not 0 <= minute <= 59: return None
    return f"{hour:02}:{minute:02}"

def parse_wico_times(value):
    match = re.fullmatch(r"\s*(\d{1,2})\s*[: ]\s*(\d{2})\s*[-–—]\s*(\d{1,2})\s*[: ]\s*(\d{2})\s*", value)
    if not match: return None, None
    a, b, c, d = match.groups()
    return parse_time(f"{a}:{b}"), parse_time(f"{c}:{d}")

def canonical_vessel(value, provider):
    # A documented VIPA column suffix is retained separately, never an AI/fuzzy alias.
    code = None; text = " ".join(value.split())
    if provider == "vipa":
        suffix = re.search(r"\s*\(([A-Z0-9]{1,3})\)$", text)
        if suffix: code = suffix.group(1); text = text[:suffix.start()].strip()
    key = " ".join(text.casefold().split())
    # Exact spelling/number variants only. Raw PDF names remain in provenance.
    # Operator names: ncl.com/cruise-ships/norwegian-getaway,
    # cunard.com/en-us/cruise-ships/queen-mary-2, seadream.com/yachts.
    aliases = {"ncl getaway": "norwegian getaway", "queen mary ii": "queen mary 2",
               "sea dream i": "seadream i", "sea dream 1": "seadream i",
               "sea dream ii": "seadream ii", "sea dream 2": "seadream ii", "seadream 2": "seadream ii"}
    return aliases.get(key, key), code

def grouped_rows(words):
    rows = []
    for word in sorted(words, key=lambda item: (item["top"], item["x0"])):
        target = next((row for row in reversed(rows[-3:]) if abs(row["top"] - word["top"]) <= 2.4), None)
        if target is None:
            target = {"top": word["top"], "words": []}; rows.append(target)
        target["words"].append(word)
    return sorted(rows, key=lambda row: row["top"])

def cells(row, boundaries):
    return [" ".join(word["text"] for word in sorted(row["words"], key=lambda w:w["x0"])
                     if left <= word["x0"] < right).strip() for left, right in zip(boundaries, boundaries[1:])]

def metadata_from_text(text, provider, profile):
    periods = []
    revision = None; issue_date = None
    if provider == "wico":
        issued = re.search(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(20\d{2})\s+Rev(?:ision)?\s*(\d+)", text, re.I)
        if issued and issued.group(1).lower() in MONTHS:
            issue_date = date(int(issued.group(3)), MONTHS[issued.group(1).lower()], int(issued.group(2))).isoformat()
            revision = "Revision " + issued.group(4)
        interval = re.search(r"(\d{1,2}/\d{1,2}/20\d{2})\s*[-–]\s*(\d{1,2}/\d{1,2}/20\d{2})", text)
        if interval:
            periods = [datetime.strptime(value, "%m/%d/%Y").date().isoformat() for value in interval.groups()]
    if profile == "wico_monthly":
        heading = re.search(r"\b(" + "|".join(calendar.month_name[1:]) + r")\s+(20\d{2})\b", text, re.I)
        if heading:
            month, year = MONTHS[heading.group(1).lower()], int(heading.group(2))
            periods = [date(year, month, 1).isoformat(), date(year, month, calendar.monthrange(year, month)[1]).isoformat()]
        if revision is None and re.search(r'\bOriginal\b',text,re.I): revision = 'Original (issue date unknown)'
    if provider == "vipa":
        period = re.search(r"OCTOBER\s+1,?\s+(20\d{2})\s+THROUGH\s+SEPTEMBER\s+30,?\s+(20\d{2})", text, re.I)
        if period: periods = [f"{period.group(1)}-10-01", f"{period.group(2)}-09-30"]
        # A fiscal-year heading is a scope/version label, not an issue date.
        fiscal = re.search(r"FISCAL\s+YEAR\s+(20\d{2})", text, re.I)
        if fiscal: revision = "Fiscal year " + fiscal.group(1) + " (issue/revision date unknown)"
    definitions = [code for code in ["IANCH", "OANCH", "WICO", "AMSS", "AMNS", "AMHP", "STJ"]
                   if re.search(r"\b" + code + r"\b", text)]
    return {"declaredPeriod": periods or None, "issueDate": issue_date, "revisionIdentifier": revision,
            "portCodesSeen": definitions, "publicationTimePrecision": "day" if issue_date else "unknown"}

def parse_pdf(path, acquisition, profile):
    import pdfplumber # Offline extraction. Installed separately; no generative fallback.
    path = Path(path); provider = acquisition["provider"]
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    if sha != acquisition["sha256"]: raise ValueError("Original PDF checksum does not match acquisition record")
    with pdfplumber.open(path) as pdf:
        page_text = [page.extract_text() or "" for page in pdf.pages]
        metadata = metadata_from_text("\n".join(page_text), provider, profile)
        calls = []; issues = []; pages = []; footers = []; inherited_day = None; inherited_origin = None
        monthly_start = date.fromisoformat(metadata["declaredPeriod"][0]) if profile == "wico_monthly" and metadata["declaredPeriod"] else None
        for number, page in enumerate(pdf.pages, 1):
            rows = grouped_rows(page.extract_words()); parsed = 0; date_markers = 0; potential = 0
            expected_width = 1224 if profile == "vipa_fiscal" else 612
            if abs(page.width - expected_width) > 2 or abs(page.height - 792) > 2:
                issues.append({"kind":"unsupported_layout", "page":number}); pages.append({"page":number,"status":"unknown_layout"}); continue
            if not rows: issues.append({"kind":"image_or_empty_page", "page":number})
            for row_number, row in enumerate(rows, 1):
                raw = " ".join(word["text"] for word in sorted(row["words"], key=lambda w:w["x0"]))
                if provider == 'vipa' and 'TOTAL PORT CALLS' in raw.upper():
                    footer = re.search(r'FOR\s+([A-Za-z]+)\s*\((\d+)\s*/\s*(\d+)\)', raw, re.I)
                    if footer and footer.group(1).lower() in MONTHS and metadata['declaredPeriod']:
                        month = MONTHS[footer.group(1).lower()]
                        start_year = date.fromisoformat(metadata['declaredPeriod'][0]).year
                        year = start_year if month >= 10 else start_year + 1
                        footers.append({'month':f'{year}-{month:02}', 'declaredCalls':int(footer.group(2)),
                                        'declaredShipDays':int(footer.group(3)), 'page':number, 'row':row_number})
                    else:
                        issues.append({'kind':'unreadable_monthly_total', 'page':number, 'row':row_number})
                if profile == "wico_monthly":
                    cols = cells(row, [0,97,140,260,371,485,612]); day_cell, berth, vessel, origin, dest, times = cols
                    arrival, departure = parse_wico_times(times)
                    supplied_day = None
                    if monthly_start and re.fullmatch(r"(?:[A-Za-z]{3}\s+)?\d{1,2}", day_cell):
                        try: supplied_day = date(monthly_start.year, monthly_start.month, int(day_cell.split()[-1]))
                        except ValueError: issues.append({"kind":"invalid_date", "page":number,"row":row_number})
                elif profile == "wico_fiscal":
                    cols = cells(row, [0,76,123,169,306,408,520,612]); day_cell, weekday, berth, vessel, origin, dest, times = cols
                    arrival, departure = parse_wico_times(times)
                    try: supplied_day = parse_day(day_cell)
                    except (ValueError, KeyError): supplied_day = None; issues.append({"kind":"invalid_date","page":number,"row":row_number})
                elif profile == "vipa_fiscal":
                    cols = cells(row, [0,139,230,312,402,493,661,726,974,1224]); day_cell, weekday, berth, raw_arrival, raw_departure, vessel, brand, origin, dest = cols
                    arrival, departure = parse_time(raw_arrival), parse_time(raw_departure)
                    try: supplied_day = parse_day(day_cell)
                    except (ValueError, KeyError): supplied_day = None; issues.append({"kind":"invalid_date","page":number,"row":row_number})
                else: raise ValueError("Unsupported deterministic parser profile")
                has_date_marker = bool(DATE_PATTERN.fullmatch(day_cell) or (profile == "wico_monthly" and re.fullmatch(r"(?:[A-Za-z]{3}\s+)?\d{1,2}",day_cell)))
                if has_date_marker: date_markers += 1
                # Spaced ST J is a printed formatting variant, not a guessed port.
                dock_code = 'STJ' if berth == 'ST J' else berth
                known_berth = dock_code in DOCKS
                # Legends, titles and footer notes cannot become calls by carrying the prior date.
                is_header = not has_date_marker and not known_berth and bool(re.search(r"\b(Date|Vessel|Berth|Arrival|DEFINITIONS|TOTAL PORT|SUBJECT TO CHANGE)\b",raw,re.I))
                if supplied_day:
                    inherited_day = supplied_day; inherited_origin = {"page":number,"row":row_number}
                recognizable_time = bool(re.search(r'\d{1,2}\s*[: ]\s*\d{2}',times)) if profile.startswith('wico') else bool(re.search(r'\d{1,2}:\d{2}',raw_arrival+' '+raw_departure))
                probable_call = not is_header and (has_date_marker or (bool(vessel) and (known_berth or (bool(berth) and recognizable_time))))
                if not probable_call:
                    if has_date_marker: issues.append({"kind":"unmatched_date_row","page":number,"row":row_number,"rawText":raw})
                    elif not is_header and vessel and re.search(r"\d{1,2}\s*[: ]\s*\d{2}.*[-–].*\d",raw) and berth:
                        issues.append({"kind":"unmatched_possible_call","page":number,"row":row_number,"rawText":raw})
                    continue
                potential += 1
                local_day = supplied_day if has_date_marker else inherited_day
                flags = []
                if local_day is None: flags.append("date_unresolved")
                if profile == "vipa_fiscal" and supplied_day is None: flags.append("unexpected_blank_date")
                if not known_berth: flags.append("dock_island_unresolved")
                if not vessel: flags.append("vessel_missing")
                weekday_text = day_cell.split()[0] if profile == 'wico_monthly' and len(day_cell.split()) > 1 else weekday if profile != 'wico_monthly' else ''
                if local_day and weekday_text and weekday_text[:3].lower() != local_day.strftime('%a').lower(): flags.append('weekday_date_mismatch')
                if arrival is None or departure is None or (arrival and departure and arrival >= departure): flags.append("time_window_unresolved")
                if re.search(r"\b(CANCELLED|CANCELED|CANCELLATION|TENTATIVE|TBD|TBA|CHARTER|BOAT|YACHT|RESCHEDULED)\b",raw,re.I): flags.append("explicit_flag_requires_review")
                vessel_words = [word for word in row["words"] if vessel and word["text"] in vessel.split()]
                if vessel_words:
                    vx0, vx1 = min(w['x0'] for w in vessel_words), max(w['x1'] for w in vessel_words)
                    middle = sum((w['top']+w['bottom'])/2 for w in vessel_words)/len(vessel_words)
                    if any(abs(line['top']-middle)<2 and line['x1']-line['x0']>25 and line['x0']<vx1 and line['x1']>vx0 for line in page.lines): flags.append("possible_strikethrough_requires_review")
                identity, vessel_code = canonical_vessel(vessel, provider)
                island, dock, scope = DOCKS.get(dock_code, ("unknown","Unknown","unknown"))
                reference = {"documentID":acquisition["id"],"sha256":sha,"page":number,"row":row_number,
                             "bbox":[min(w['x0'] for w in row['words']),min(w['top'] for w in row['words']),max(w['x1'] for w in row['words']),max(w['bottom'] for w in row['words'])],
                             "rawText":raw,"rawColumns":cols,"dateInheritedFrom":None if supplied_day else inherited_origin}
                call = {"localDate":local_day.isoformat() if local_day else None,"island":island,"dock":dock,"rawDock":berth,"dockCode":dock_code,"portScope":scope,
                        "originalVesselName":vessel,"canonicalVessel":identity,"sourceVesselCode":vessel_code,"arrival":arrival,"departure":departure,
                        "guestCapacity":None,"capacityMeaning":"not supplied by these schedule columns; not attendance","flags":flags,
                        "trafficKind":"boat_or_charter_requires_review" if re.search(r'\b(BOAT|CHARTER|YACHT)\b',raw,re.I) else "cruise_schedule_row_not_independently_classified",
                        "sourceAuthority":provider,"sourceURL":acquisition["url"],"provenance":reference,"reviewState":"candidate"}
                calls.append(call); parsed += 1
                for flag in flags: issues.append({"kind":flag,"page":number,"row":row_number,"localDate":call['localDate']})
            pages.append({"page":number,"wordRows":len(rows),"explicitDateRows":date_markers,"possibleCallRows":potential,"parsedCallRows":parsed})
        # Missing issue time remains unknown, regardless of PDF metadata and Last-Modified.
        document = {**acquisition, **metadata, "parserProfile":profile,"parserVersion":"official-candidates-2","pageCount":len(pdf.pages),
                    "pdfFileMetadata":{k:str(v) for k,v in (pdf.metadata or {}).items() if k in ("CreationDate","ModDate")},
                    "reviewState":"candidate","reviewedAt":None,"reviewerAlias":None,"rightsScope":"localReviewOnly",
                    "pages":pages,"issues":issues,"calls":calls,"monthlyTotals":footers,"productionEnabled":False}
    represented = sorted({call['localDate'][:7] for call in calls if call['localDate']})
    if profile == "wico_monthly" and monthly_start: represented = sorted(set(represented+[monthly_start.strftime('%Y-%m')]))
    document['representedMonths'] = represented
    if metadata['declaredPeriod']:
        start, end = map(date.fromisoformat, metadata['declaredPeriod']); months=set(); cursor=start.replace(day=1)
        while cursor <= end:
            months.add(cursor.strftime('%Y-%m')); cursor=(cursor.replace(day=28)+timedelta(days=4)).replace(day=1)
        document['missingMonths'] = sorted(months-set(represented))
        for call in calls:
            if call['localDate'] and not start <= date.fromisoformat(call['localDate']) <= end:
                document['issues'].append({'kind':'date_outside_declared_period','localDate':call['localDate'],'page':call['provenance']['page']})
    else: document['missingMonths'] = ['declared period unknown']
    if provider == 'vipa':
        if not footers: document['issues'].append({'kind':'monthly_totals_missing'})
        for month in represented:
            totals = [f for f in footers if f['month'] == month]
            rows = [c for c in calls if c['localDate'] and c['localDate'].startswith(month)]
            actual = (len(rows), len({c['localDate'] for c in rows}))
            declared = {(f['declaredCalls'], f['declaredShipDays']) for f in totals}
            if declared != {actual}:
                document['issues'].append({'kind':'monthly_total_mismatch', 'month':month,
                                          'parsedCalls':actual[0], 'parsedShipDays':actual[1],
                                          'declaredTotals':sorted(declared)})
    document['structuralParsingComplete'] = bool(calls) and not document['issues']
    return document

def reconcile(documents):
    groups=defaultdict(list)
    for document in documents:
        for call in document['calls']:
            if call['localDate'] and call['canonicalVessel']:
                groups[(call['island'],call['localDate'],call['canonicalVessel'])].append(call)
    results=[]
    for (island, day, vessel), candidates in sorted(groups.items()):
        berth_variants={call.get('dockCode',call['rawDock']) for call in candidates}
        time_variants={(call['arrival'],call['departure']) for call in candidates if call['arrival'] and call['departure']}
        flags=sorted({flag for call in candidates for flag in call['flags']})
        conflicts=[]
        if len(berth_variants)>1: conflicts.append('berth_conflict')
        if len(time_variants)>1: conflicts.append('time_conflict')
        results.append({'island':island,'localDate':day,'canonicalVessel':vessel,'status':'conflicting' if conflicts else 'unknown' if flags else 'candidate',
                        'conflicts':conflicts,'flags':flags,'sourceRowCount':len(candidates),'matchingDuplicateRows':len(candidates)-1 if not conflicts and not flags else 0,
                        'candidates':candidates,'reviewState':'candidate','reviewedAt':None})
    return results

def candidate_calendar(documents, start, end):
    if end<start or (end-start).days>730: raise ValueError('Bounded candidate calendar requires at most 731 days')
    resolutions=reconcile(documents); rows=[]; day=start
    while day<=end:
        local=day.isoformat(); month=day.strftime('%Y-%m')
        calls=[r for r in resolutions if r['localDate']==local and r['island']=='stThomas']
        other=[r for r in resolutions if r['localDate']==local and r['island']!='stThomas']
        unresolved=[r for r in other if r['island']=='unknown']
        matrix={}
        for scope in PORT_SCOPES:
            authority='vipa' if scope=='crownBay' else 'wico'
            possible=[d for d in documents if d['provider']==authority and d['declaredPeriod'] and d['declaredPeriod'][0]<=local<=d['declaredPeriod'][1]
                      and month in d['representedMonths']]
            if scope=='stThomasAnchorages': possible=[d for d in possible if {'IANCH','OANCH'}<=set(d['portCodesSeen'])]
            matrix[scope]={'authority':authority,'documents':[d['id'] for d in possible],
                          'parsingStatus':'parsed_candidate' if possible and all(d['structuralParsingComplete'] for d in possible) else 'needs_review' if possible else 'missing',
                          'completenessVerified':False,'reviewState':'candidate','scheduledCallCount':None,
                          'nextReviewDueAt':None,'supersededByRevision':None,
                          'parsedRowsOnDate':sum(c['localDate']==local and c['island']=='stThomas' and c['portScope']==scope for d in possible for c in d['calls'])}
        all_present=all(item['documents'] for item in matrix.values())
        clean=all(item['parsingStatus']=='parsed_candidate' for item in matrix.values())
        conflicts=[r for r in calls if r['status']=='conflicting']
        blockers=['human_scope_and_revision_signoff_required','attribution_and_derived_forecast_scope_not_attested']
        if not all_present: blockers.append('missing_port_or_month_source')
        if not clean: blockers.append('parsing_or_scope_review_required')
        if other and not calls: blockers.append('other_island_rows_do_not_establish_st_thomas_coverage')
        if unresolved: blockers.append('unresolved_island_or_dock_rows')
        if conflicts: blockers.append('unresolved_source_conflicts')
        if any(d['issueDate'] is None for d in documents if d['id'] in {identifier for m in matrix.values() for identifier in m['documents']}): blockers.append('original_issue_date_unknown')
        rows.append({'localDate':local,'timezone':'America/St_Thomas','state':'conflicting' if conflicts else 'unknown',
                     'candidateKind':'unresolved_rows_review' if unresolved else 'empty_day_scope_review' if not calls and all_present else 'listed_calls_review' if calls else 'missing_coverage',
                     'parsedUniqueStThomasCalls':len(calls),'scheduledCallCount':None,'otherIslandCalls':len(other),
                     'portCoverage':matrix,'conflictingCalls':[r['canonicalVessel'] for r in conflicts],
                     'reviewState':'candidate','reviewerAlias':None,'reviewedAt':None,'blockers':blockers,'productionEnabled':False})
        day+=timedelta(days=1)
    return rows, resolutions
