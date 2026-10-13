"""Private, opt-in official source acquisition. Never writes schedule.json or runs jobs."""
from __future__ import annotations
import hashlib
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit

PROVIDERS = {
    "wico": {"listing": "https://wico-ltd.com/", "hosts": {"wico-ltd.com", "www.wico-ltd.com"}},
    "vipa": {"listing": "https://www.viport.com/schedule-cruise-ports", "hosts": {"viport.com", "www.viport.com", "static.wixstatic.com"}},
}
MAX_BYTES = 15 * 1024 * 1024

class CollectionBlocked(RuntimeError): pass

def utcnow(): return datetime.now(timezone.utc)
def stamp(value): return value.astimezone(timezone.utc).isoformat()

def private_directory(path):
    path = Path(path).expanduser().resolve()
    # No raw documents/candidate data inside any Git checkout, even if .gitignore changes.
    if any((parent / ".git").exists() for parent in [path, *path.parents]):
        raise CollectionBlocked("Use a private output directory outside every Git checkout")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path

def save_private(path, data):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o600)
    temporary.replace(path)

def save_json(path, value): save_private(path, (json.dumps(value, indent=2) + "\n").encode())

def validate_url(provider, url):
    parsed = urlsplit(url)
    if provider not in PROVIDERS or parsed.scheme != "https" or parsed.hostname not in PROVIDERS[provider]["hosts"] \
       or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise CollectionBlocked("Only the approved official HTTPS source route is allowed")
    return url

class OfficialRedirects(urllib.request.HTTPRedirectHandler):
    def __init__(self, provider): self.provider = provider
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_url(self.provider, newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)

class PDFLinks(HTMLParser):
    def __init__(self, base):
        super().__init__(); self.base = base; self.links = []; self.current = None
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.current = {"url": urljoin(self.base, dict(attrs).get("href", "")), "label": ""}
    def handle_data(self, data):
        if self.current: self.current["label"] += data
    def handle_endtag(self, tag):
        if tag == "a" and self.current:
            if urlsplit(self.current["url"]).path.lower().endswith(".pdf"): self.links.append(self.current)
            self.current = None

def discover_pdfs(provider, html):
    parser = PDFLinks(PROVIDERS[provider]["listing"]); parser.feed(html)
    # Wix also serializes its official document URLs in JSON. No CMS/API endpoints are queried.
    normalized = unescape(html).replace("\\/", "/")
    for url in re.findall(r'https://[^\s"<>\\]+\.pdf', normalized):
        parser.links.append({"url": url, "label": "Official page document link; label needs review"})
    found = {}
    for link in parser.links:
        try: validate_url(provider, link["url"])
        except (CollectionBlocked, ValueError): continue
        link["label"] = " ".join(link["label"].split())
        if link["url"] not in found or "needs review" in found[link["url"]]["label"]: found[link["url"]] = link
    return list(found.values())

def retry_deadline(header, now, failures):
    fallback = now + timedelta(seconds=min(86400, 900 * 2 ** min(failures - 1, 8)))
    if not header: return fallback
    try: requested = now + timedelta(seconds=max(0, int(header)))
    except ValueError:
        try: requested = parsedate_to_datetime(header).astimezone(timezone.utc)
        except (ValueError, TypeError): return fallback
    return max(fallback, requested) # Never shorten the server's requested backoff.

class OfficialFetcher:
    def __init__(self, output, authorization, *, opener_factory=None, clock=utcnow):
        self.output = private_directory(output); self.authorization = authorization; self.clock = clock
        self.opener_factory = opener_factory or (lambda provider: urllib.request.build_opener(OfficialRedirects(provider)))
        self.cache_path = self.output / "http-cache.json"
        self.cache = json.loads(self.cache_path.read_text()) if self.cache_path.exists() else {"documents": {}, "providers": {}}

    def seed(self, records):
        """Reuse intact private acquisitions for conditional GET, without new requests."""
        for record in records:
            validate_url(record['provider'],record['url'])
            path = Path(record['path']).resolve()
            if any((p/'.git').exists() for p in [path.parent,*path.parents]) or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=record['sha256']:
                raise CollectionBlocked('Cache seed must reference an intact private original outside Git')
            self.cache['documents'].setdefault(record['url'],record)
        save_json(self.cache_path,self.cache)

    def authorize(self, *, one_shot=False):
        if self.authorization.get("status") != "owner_attested_not_provider_inspected" or self.authorization.get("automaticCollection") is not True:
            raise CollectionBlocked("Owner authorization for automatic collection is required; no network request made")
        if not one_shot:
            if self.authorization.get("collectionEnabled") is not True or self.authorization.get("cadenceAuthorized") is not True:
                raise CollectionBlocked("Recurring collection is disabled until exact cadence is authorized")
            seconds = self.authorization.get("checkIntervalSeconds")
            if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds < 60:
                raise CollectionBlocked("A specific authorized check interval is required")

    def fetch(self, provider, url, identifier, *, pdf=True, one_shot=False):
        self.authorize(one_shot=one_shot); validate_url(provider, url)
        if not re.fullmatch(r"[a-z0-9-]+", identifier): raise CollectionBlocked("Invalid local document identifier")
        now = self.clock(); state = self.cache["providers"].get(provider, {})
        if state.get("accessRefused"): raise CollectionBlocked("Provider access was refused; a human must resolve access first")
        if state.get("nextAllowedAt") and now < datetime.fromisoformat(state["nextAllowedAt"]): raise CollectionBlocked("Provider backoff is active")
        previous = self.cache["documents"].get(url, {})
        if not one_shot and previous.get("checkedAt") and now < datetime.fromisoformat(previous["checkedAt"]) + timedelta(seconds=self.authorization["checkIntervalSeconds"]):
            raise CollectionBlocked("Authorized check interval has not elapsed")
        headers = {"User-Agent": "CrowdCast-LocalSourceReview/1.0", "Accept": "application/pdf" if pdf else "text/html"}
        for field, header in [("etag", "If-None-Match"), ("lastModified", "If-Modified-Since")]:
            if previous.get(field): headers[header] = previous[field]
        try:
            with self.opener_factory(provider).open(urllib.request.Request(url, headers=headers), timeout=30) as response:
                validate_url(provider, response.url)
                if response.status != 200: raise CollectionBlocked("Unexpected HTTP response; no candidate replaced")
                body = response.read(MAX_BYTES + 1)
                # Ordinary published sites may load captcha libraries for contact forms.
                # Only explicit access/interstitial markers stop collection.
                if not pdf and re.search(rb'(cf[-_]chl|/cdn-cgi/challenge-platform|id=[\"\x27]challenge-form|<title[^>]*>[^<]*(?:access denied|just a moment|captcha|verify.*human))', body, re.I):
                    self.cache['providers'][provider] = {'accessRefused':True, 'lastHTTPStatus':200}
                    save_json(self.cache_path, self.cache)
                    raise CollectionBlocked('Access challenge detected; human resolution required')
                if len(body) > MAX_BYTES or (pdf and not body.startswith(b"%PDF-")):
                    raise CollectionBlocked("Size/format/access check failed; no document replaced")
                if pdf and "pdf" not in (response.headers.get("Content-Type") or "").lower():
                    raise CollectionBlocked("Unexpected document Content-Type")
                record = {"id": identifier, "provider": provider, "url": url, "finalURL": response.url,
                    "obtainedAt": stamp(now), "checkedAt": stamp(now), "sha256": hashlib.sha256(body).hexdigest(),
                    "etag": response.headers.get("ETag"), "lastModified": response.headers.get("Last-Modified"),
                    "cacheControl": response.headers.get("Cache-Control"), "contentType": response.headers.get("Content-Type"),
                    "sourceIssuedAt": None, "sourceIssueMeaning": "HTTP modification/obtainment is not original publication", "status": 200,
                    "nextCheckDueAt": stamp(now + timedelta(seconds=self.authorization["checkIntervalSeconds"])) if self.authorization.get("cadenceAuthorized") and self.authorization.get("checkIntervalSeconds") else None}
                # Content-addressed originals keep older source versions available for local diff/review.
                suffix = ".pdf" if pdf else ".html"
                path = self.output / (identifier + "-" + record["sha256"][:16] + suffix)
                if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() != record['sha256']:
                    raise CollectionBlocked('Content-addressed original is damaged; human review required')
                if not path.exists(): save_private(path, body)
                record["path"] = str(path)
                record["supersedesSHA256"] = previous.get("sha256") if previous.get("sha256") != record["sha256"] else None
                self.cache["documents"][url] = record; self.cache["providers"][provider] = {"failures": 0}
                save_json(self.cache_path, self.cache)
                return record
        except urllib.error.HTTPError as error:
            if error.code == 304:
                path = Path(previous.get("path", "/missing-cached-document"))
                if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != previous.get("sha256"):
                    raise CollectionBlocked("304 without an intact local original; no source data accepted") from error
                record = {**previous, "checkedAt": stamp(now), "status": 304}
                record["nextCheckDueAt"] = stamp(now + timedelta(seconds=self.authorization["checkIntervalSeconds"])) if self.authorization.get("cadenceAuthorized") and self.authorization.get("checkIntervalSeconds") else None
                self.cache["documents"][url] = record; save_json(self.cache_path, self.cache); return record
            failures = state.get("failures", 0) + 1
            self.cache["providers"][provider] = {"failures": failures, "accessRefused": error.code in (401, 403),
                "lastHTTPStatus": error.code, "nextAllowedAt": stamp(retry_deadline(error.headers.get("Retry-After"), now, failures))}
            save_json(self.cache_path, self.cache)
            raise CollectionBlocked(f"Official provider returned HTTP {error.code}; stopped without bypass or retry") from error
        except (urllib.error.URLError, TimeoutError, CollectionBlocked) as error:
            failures = state.get("failures", 0) + 1
            self.cache["providers"][provider] = {**self.cache['providers'].get(provider, {}), "failures": failures, "nextAllowedAt": stamp(retry_deadline(None, now, failures))}
            save_json(self.cache_path, self.cache)
            raise CollectionBlocked("Acquisition failed; prior original retained; provider backoff active") from error
