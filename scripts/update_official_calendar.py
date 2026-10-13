"""Collect public official listings and export a conservative derived app feed.
PDFs and parsing details stay in the ephemeral working directory, never in Git.
"""
import argparse, calendar, json, re, time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit
import pdfplumber
from official_fetch import OfficialFetcher, CollectionBlocked, PROVIDERS, discover_pdfs, private_directory, save_json
from official_parse import parse_pdf
from export_app_calendar import export_calendar

INTERVAL = 6 * 3600
AUTHORIZATION = {"status":"owner_attested_not_provider_inspected",
    "automaticCollection":True, "collectionEnabled":True,
    "cadenceAuthorized":True, "checkIntervalSeconds":INTERVAL}

def select_links(provider, links, today):
    fiscal = today.year + (today.month >= 10)
    if provider == "wico":
        annual=[]; monthly=[]
        for link in links:
            name=unquote(urlsplit(link["url"]).path).split("/")[-1]
            if re.search(r"FY[-_ ]*"+str(fiscal)+r"\b", name, re.I):
                revision=re.search(r"REV[-_ ]*(\d+)",name,re.I)
                annual.append((int(revision.group(1)) if revision else 0,link))
            if re.search(calendar.month_name[today.month]+r"[-_ ]*"+str(today.year),name,re.I):
                monthly.append(link)
        if not annual: raise CollectionBlocked("Current WICO fiscal schedule missing")
        highest=max(x[0] for x in annual); annual=[x[1] for x in annual if x[0]==highest]
        if len(annual)!=1 or len(monthly)>1: raise CollectionBlocked("Ambiguous WICO revision")
        return [(annual[0],"wico_fiscal")]+[(x,"wico_monthly") for x in monthly]
    selected=[x for x in links if re.search(r"Fiscal Year\s+"+str(fiscal)+r"\b",x["label"],re.I)]
    if not 1<=len(selected)<=4: raise CollectionBlocked("Missing or ambiguous VIPA fiscal listings")
    return [(x,"vipa_fiscal") for x in selected]

def scoped_vipa_links(html):
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin
    from official_fetch import validate_url
    soup=BeautifulSoup(html,"html.parser"); found={}
    for anchor in soup.find_all("a",href=True):
        url=urljoin(PROVIDERS["vipa"]["listing"],anchor["href"])
        if not urlsplit(url).path.lower().endswith(".pdf"): continue
        try: validate_url("vipa",url)
        except (CollectionBlocked,ValueError): continue
        for parent in anchor.parents:
            text=parent.get_text(" ",strip=True)
            if re.search(r"St\.?\s*Croix",text,re.I): break
            if re.search(r"Crown\s+Bay",text,re.I) and re.search(r"St\.?\s*Thomas",text,re.I):
                found[url]={"url":url,"label":anchor.get_text(" ",strip=True)};break
    return list(found.values())

def validate_document(document):
    tolerated={"weekday_date_mismatch","time_window_unresolved","dock_island_unresolved",
               "vessel_missing","explicit_flag_requires_review","possible_strikethrough_requires_review",
               "unexpected_blank_date","monthly_total_mismatch"}
    if not document["calls"] or any(x["kind"] not in tolerated for x in document["issues"]):
        raise CollectionBlocked("Structural parsing discrepancy; previous feed retained")
    if any(not c["localDate"] for c in document["calls"]):
        raise CollectionBlocked("Unresolved source date")
    if not any(c["island"]=="stThomas" for c in document["calls"]):
        raise CollectionBlocked("No St. Thomas calls")

def collect(work, state_path, today=None):
    today=today or datetime.now(timezone.utc).astimezone(__import__("zoneinfo").ZoneInfo("America/St_Thomas")).date()
    work=private_directory(work)
    fetcher=OfficialFetcher(work/"acquisition",AUTHORIZATION)
    if state_path.exists():
        state=json.loads(state_path.read_text())
        fetcher.cache["providers"]=state.get("providers",{})
        print(json.dumps({"providerStatus":fetcher.cache["providers"]}),flush=True)
        for status in fetcher.cache["providers"].values():
            if status.get("accessRefused"):
                raise CollectionBlocked("Provider access refused; no requests made")
            if status.get("nextAllowedAt") and datetime.now(timezone.utc)<datetime.fromisoformat(status["nextAllowedAt"]):
                raise CollectionBlocked("Provider backoff is active; no requests made")
        last=state.get("lastSuccess")
        if last and (datetime.now(timezone.utc)-datetime.fromisoformat(last)).total_seconds()<INTERVAL-600:
            raise CollectionBlocked("Successful collection is already recent")
    last_request={}
    def fetch(provider,url,identifier,pdf=True):
        cached=fetcher.cache['documents'].get(url)
        if cached and (datetime.now(timezone.utc)-datetime.fromisoformat(cached['checkedAt'])).total_seconds()<INTERVAL:
            import hashlib
            original=Path(cached['path'])
            if original.is_file() and hashlib.sha256(original.read_bytes()).hexdigest()==cached['sha256']:
                return cached
        delay=5-(time.monotonic()-last_request.get(provider,0))
        if delay>0: time.sleep(delay)
        last_request[provider]=time.monotonic()
        return fetcher.fetch(provider,url,identifier,pdf=pdf)
    documents=[]
    try:
        for provider in ("wico","vipa"):
            listing=fetch(provider,PROVIDERS[provider]["listing"],provider+"-listing",pdf=False)
            html=Path(listing["path"]).read_text()
            links=scoped_vipa_links(html) if provider=="vipa" else discover_pdfs(provider,html)
            for i,(link,profile) in enumerate(select_links(provider,links,today)):
                record=fetch(provider,link["url"],provider+"-schedule-"+str(i))
                if provider=="vipa":
                    with pdfplumber.open(record["path"]) as pdf:
                        heading=(pdf.pages[0].extract_text() or "").split("Arrival Date")[0]
                    # VIPA also links St. Croix PDFs under identical fiscal labels.
                    if not re.search(r"Crown Bay[-\s–]*St\.?\s*Thomas",heading,re.I):
                        continue
                doc=parse_pdf(Path(record["path"]),record,profile)
                validate_document(doc); documents.append(doc)
        if sum(d["provider"]=="vipa" for d in documents)!=1:
            raise CollectionBlocked("Ambiguous St. Thomas VIPA revision")
        fiscal=today.year+(today.month>=10)
        end=date(fiscal,9,30)
        catalog=json.loads((Path(__file__).resolve().parents[1]/"data/ship-capacities.json").read_text())
        value=export_calendar(documents,catalog,today,end)
        value["delivery"]="hosted"
        if not json.loads(__import__("base64").b64decode(value["payload"]))["days"]:
            raise CollectionBlocked("Empty output")
        save_json(work/"official-cruise-calendar-v1.json",value)
        # Detailed parsing records remain local/ephemeral.
        save_json(work/"parsed-documents.json",documents)
        state={"providers":fetcher.cache["providers"],"lastSuccess":datetime.now(timezone.utc).isoformat()}
        save_json(state_path,state)
        return value
    except Exception:
        old=json.loads(state_path.read_text()) if state_path.exists() else {}
        save_json(state_path,{"providers":fetcher.cache["providers"],"lastSuccess":old.get("lastSuccess")})
        print(json.dumps({"providerStatus":fetcher.cache["providers"]}),flush=True)
        raise

def main():
    p=argparse.ArgumentParser(); p.add_argument("--work",required=True);p.add_argument("--state",required=True)
    args=p.parse_args(); state=Path(args.state);state.parent.mkdir(parents=True,exist_ok=True)
    result=collect(args.work,state)
    print(json.dumps({"documents":len(result["documents"]),"heldDates":len(result["heldDays"]),
                      "conflictDates":len(result["conflictDays"]),"status":"validated"}))
if __name__=="__main__": main()
