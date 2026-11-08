"""Review-only source change detector. Never writes app/Firebase facts."""
import argparse, hashlib, json, re, time, urllib.request, urllib.error, urllib.robotparser
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse
UA = "CrowdCastSourceReview/1.0"
LIMIT = 2_000_000

class Text(HTMLParser):
    def __init__(self):
        super().__init__(); self.skip = 0; self.parts = []
    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"): self.skip += 1
        # Image schedules can change without text changes.
        if tag == "img" and not self.skip:
            a = dict(attrs)
            self.parts.append(" ".join(a.get(k, "") for k in ("alt", "src")))
    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript") and self.skip: self.skip -= 1
    def handle_data(self, data):
        if not self.skip: self.parts.append(data)
def fingerprint(html):
    p = Text(); p.feed(html)
    text = re.sub(r"\s+", " ", " ".join(p.parts)).strip()
    if len(text) < 80: raise ValueError("insufficient source content")
    if any(x in text.lower() for x in ("verify you are human", "just a moment...", "access denied", "enable javascript and cookies to continue")):
        raise ValueError("source challenge; manual review required")
    return hashlib.sha256(text.encode()).hexdigest()

class SameHost(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        old, new = urlparse(req.full_url), urlparse(newurl)
        if new.scheme != "https" or old.hostname.removeprefix("www.") != new.hostname.removeprefix("www."):
            raise ValueError("redirect outside approved host")
        return super().redirect_request(req, fp, code, msg, headers, newurl)
def fetch(url):
    if urlparse(url).scheme != "https": raise ValueError("HTTPS required")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html,text/plain"})
    with urllib.request.build_opener(SameHost).open(req, timeout=20) as r:
        if r.status != 200: raise ValueError("unexpected source status")
        data = r.read(LIMIT + 1)
        if len(data) > LIMIT: raise ValueError("source exceeds size limit")
        return data.decode("utf-8", errors="replace")
def allowed(url, cache):
    p = urlparse(url); origin = p.scheme + "://" + p.netloc
    if origin not in cache:
        try:
            body = fetch(origin + "/robots.txt")
            parser = urllib.robotparser.RobotFileParser(); parser.parse(body.splitlines())
            cache[origin] = parser
        except urllib.error.HTTPError as e:
            if e.code == 404:
                parser = urllib.robotparser.RobotFileParser(); parser.parse([])
                cache[origin] = parser
            else: raise
    return cache[origin].can_fetch(UA, url)

def update(old, digest, now):
    row = dict(old)
    row["lastAttemptAt"] = now; row["lastSuccessfulCheckAt"] = now
    row.pop("error", None)
    previous = row.get("observedHash")
    if previous != digest:
        row["lastChangeDetectedAt"] = now
    row["observedHash"] = digest
    # Only a separately reviewed hash can clear this queue.
    try:
        reviewed = datetime.fromisoformat(row.get("reviewedAt", "").replace("Z", "+00:00"))
        age_due = (datetime.fromisoformat(now.replace("Z", "+00:00")) - reviewed).days >= 30
    except (ValueError, TypeError):
        age_due = True
    row["reviewRequired"] = row.get("reviewedHash") != digest or age_due
    row["status"] = "review_required" if row["reviewRequired"] else "unchanged_since_review"
    return row

def run(config, state):
    now = datetime.now(timezone.utc).isoformat(); cache = {}; rows = {}
    for source in config["sources"]:
        old = state.get("sources", {}).get(source["id"], {})
        try:
            if not allowed(source["url"], cache): raise ValueError("robots disallows collection")
            rows[source["id"]] = update(old, fingerprint(fetch(source["url"])), now)
        except Exception as e:
            row = dict(old); row.update(lastAttemptAt=now, status="check_failed", error=str(e)[:180], reviewRequired=True)
            rows[source["id"]] = row
        time.sleep(2)
    return {"schemaVersion": 1, "checkedAt": now, "sources": rows}
def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--config", default="places/sources.json"); ap.add_argument("--state", default="places/check-state.json"); args = ap.parse_args()
    config = json.loads(Path(args.config).read_text()); path = Path(args.state)
    state = json.loads(path.read_text()) if path.exists() else {}
    result = run(config, state); path.write_text(json.dumps(result, indent=2) + "\n")
    pending = sum(x["reviewRequired"] for x in result["sources"].values())
    failed = sum(x["status"] == "check_failed" for x in result["sources"].values())
    report = ["# Place source review", "", f"Checked: {result['checkedAt']}", f"{pending} pages need review; {failed} source checks failed.", "", "Source check time is NOT fact verification. Reviewed app facts remain unchanged.", "", "| Place / source | Result | Last successful check |", "|---|---|---|"]
    for source in config["sources"]:
        row = result["sources"][source["id"]]
        report.append(f"| [{source['label']}]({source['url']}) | {row['status']} | {row.get('lastSuccessfulCheckAt', 'Never')} |")
    Path("places/REVIEW_QUEUE.md").write_text("\n".join(report) + "\n")
    print(f"{len(result['sources'])} sources checked; {pending} review-required; {failed} failed")
if __name__ == "__main__": main()
