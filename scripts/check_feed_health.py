"""Check delivered data, not workflow badges. No direct provider requests."""
import argparse
import base64
import calendar
from datetime import datetime, timezone
import json
import re
import subprocess
import urllib.request
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo

TITLE = "Feed freshness alert"
CRUISE_STATE = "https://raw.githubusercontent.com/dezigod/cruise-schedule-app/collection-state/state.json"
CONFIG = {
    "cruise": ("https://raw.githubusercontent.com/dezigod/cruise-schedule-app/main/official-cruise-calendar-v1.json", 12 * 3600),
    "outage": ("https://raw.githubusercontent.com/dezigod/crowdcast-outage-feed/data/outages.json", 30 * 60),
}


def timestamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Missing timezone")
    return result


def document_roles(documents, local_date):
    fiscal_year = local_date.year + (local_date.month >= 10)
    month = calendar.month_name[local_date.month]
    roles = set()
    for doc in documents:
        provider = doc.get("id", "").split("-")[0]
        name = unquote(urlsplit(doc.get("url", "")).path).split("/")[-1]
        if provider == "wico":
            if re.search(rf"FY[-_ ]*{fiscal_year}\b", name, re.I):
                roles.add("WICO fiscal schedule")
            elif re.search(rf"{month}[-_ ]*{local_date.year}\b", name, re.I):
                roles.add("WICO current monthly schedule")
        elif provider == "vipa":
            revision = doc.get("sourceRevision") or ""
            # The collector already scopes this source to St. Thomas. Some
            # official PDFs omit a fiscal-year heading from their revision.
            year = re.search(r"fiscal year\s+(\d{4})", revision, re.I)
            if year is None or int(year.group(1)) == fiscal_year:
                roles.add("VIPA fiscal schedule")
    return roles


def source_times(kind, feed):
    if kind == "cruise":
        return {d["id"]: d.get("checkedAt") for d in feed.get("documents", [])}
    source = feed.get("source", {})
    return {key: source.get(key) for key in ("checkedAt", "updatedAt")}


def problems(kind, feed, now):
    limit = CONFIG[kind][1]
    failures = []
    if feed.get("schemaVersion") != 1:
        failures.append("Published feed has an unsupported format.")
    if kind == "cruise":
        try:
            days = json.loads(base64.b64decode(feed["payload"], validate=True))["days"]
            if (not isinstance(days, list) or not days
                    or not all(isinstance(d, dict) and isinstance(d.get("date"), str)
                               and isinstance(d.get("ships"), list) for d in days)):
                raise ValueError("Invalid calendar days")
        except (ValueError, KeyError, TypeError):
            failures.append("The published cruise calendar cannot be read by the app.")
        documents = feed.get("documents")
        if not isinstance(documents, list) or not documents:
            return ["Cruise feed has no source documents."]
        required = {"WICO fiscal schedule", "WICO current monthly schedule", "VIPA fiscal schedule"}
        local_date = now.astimezone(ZoneInfo("America/St_Thomas")).date()
        for role in sorted(required - document_roles(documents, local_date)):
            failures.append("Cruise feed is missing the " + role + ".")
        ids = [d.get("id") for d in documents]
        if len(set(ids)) != len(ids) or any(not isinstance(i, str) or not i for i in ids):
            failures.append("Cruise source document identifiers are missing or duplicated.")
        if any(isinstance(i, str) and i.split("-")[0] not in {"wico", "vipa"} for i in ids):
            failures.append("Cruise feed contains an unexpected provider.")
        if feed.get("collectionStatus") in {"failed", "blocked"}:
            failures.append("Cruise collector has not verified its latest attempt.")
        checks = [(str(d.get("id") or "Cruise document") + " source check", d.get("checkedAt")) for d in documents]
    else:
        islands = feed.get("islands")
        if (not isinstance(islands, list) or len(islands) != 4
                or {i.get("id") for i in islands if isinstance(i, dict)} != {"stThomas", "stCroix", "stJohn", "waterIsland"}):
            failures.append("The outage feed is missing expected island coverage.")
        checks = [("Outage collection", feed.get("source", {}).get("checkedAt")),
                  ("WAPA source publication", feed.get("source", {}).get("updatedAt"))]
        if feed.get("collectionStatus") != "ok":
            failures.append("Outage collector has not verified its latest attempt.")
    for label, value in checks:
        try:
            age = (now - timestamp(value)).total_seconds()
            if age < 0:
                failures.append(label + " timestamp is in the future.")
            elif age > limit:
                failures.append(label + " is older than the allowed freshness window.")
        except (TypeError, ValueError, AttributeError):
            failures.append(label + " timestamp is missing or invalid.")
    return sorted(set(failures))


def gh(*args):
    # GitHub CLI keeps its token in the environment, never command arguments.
    result = subprocess.run(["gh", *args], check=True, capture_output=True, text=True)
    return result.stdout


def cruise_state_problems(state, now):
    providers = state["providers"]
    if not isinstance(providers, dict) or set(providers) != {"wico", "vipa"}:
        raise ValueError("Missing cruise collector state")
    failures = []
    for provider, status in providers.items():
        if status.get("accessRefused"):
            failures.append(provider.upper() + " refused access. Review access with the provider before resuming.")
        elif status.get("failures", 0) > 0:
            failures.append(provider.upper() + " download failed. Waiting for the next permitted successful check.")
    if state.get("lastFailure") == "monthly_missing":
        failures.append("WICO's known monthly listing disappeared. The previous calendar was retained.")
    elif state.get("lastFailure") and not failures:
        failures.append("The last cruise collection failed. Inspect the collector run before changing the feed.")
    if state.get("pendingDelivery"):
        age = (now - timestamp(state["lastSuccess"])).total_seconds()
        if age < 0 or age > 1800:
            failures.append("A validated cruise calendar is waiting for delivery. Check the publication job.")
    return failures


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise OSError("Unexpected redirect while checking GitHub delivery")


def published_json(url, limit):
    bucket = int(datetime.now(timezone.utc).timestamp()) // 300
    request = urllib.request.Request(url + "?health_check=" + str(bucket),
        headers={"User-Agent": "CrowdCast-feed-health", "Cache-Control": "no-cache"})
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
        body = response.read(limit + 1)
    if len(body) > limit:
        raise ValueError("Feed size exceeded")
    return json.loads(body)


def alert(repository, failures):
    # GitHub's search index can lag after issue creation. Read the live
    # issue list so adjacent checks recognise the same incident.
    pages = json.loads(gh("api", "--paginate", "--slurp", "repos/" + repository + "/issues?state=open&per_page=100"))
    rows = (row for page in pages for row in page if "pull_request" not in row)
    incident = next((row for row in rows if row["title"] == TITLE), None)
    if failures:
        body = ("@dezigod Feed delivery needs attention.\n\n" + "\n".join("- " + f for f in failures) +
                "\n\nCheck the collector's Actions logs and the published source timestamps. "
                "The previous valid feed must be retained. Never bypass provider refusals or reset source timestamps. "
                "This issue closes automatically after the delivered feed is healthy again.")
        if incident is None:
            gh("issue", "create", "--repo", repository, "--title", TITLE, "--body", body)
        elif incident.get("body", "") != body:
            earlier = set(incident.get("body", "").splitlines())
            added = [f for f in failures if "- " + f not in earlier]
            number = str(incident["number"])
            gh("issue", "edit", number, "--repo", repository, "--body", body)
            if added:
                gh("issue", "comment", number, "--repo", repository, "--body",
                   "@dezigod The delivery incident changed.\n\n" + "\n".join("- " + f for f in added))
        return
    if incident:
        gh("issue", "close", str(incident["number"]), "--repo", repository,
           "--comment", "@dezigod Recovery verified: the delivered feed now passes source freshness and collection-status checks.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=CONFIG)
    parser.add_argument("--repository", required=True)
    args = parser.parse_args()
    expected = "dezigod/" + ("cruise-schedule-app" if args.kind == "cruise" else "crowdcast-outage-feed")
    if args.repository != expected:
        parser.error("Unexpected repository")
    try:
        feed = published_json(CONFIG[args.kind][0], 3_000_000)
        failures = problems(args.kind, feed, datetime.now(timezone.utc))
        print(json.dumps({"feed": args.kind, "observedAt": datetime.now(timezone.utc).isoformat(),
                          "sourceTimes": source_times(args.kind, feed)}))
    except (OSError, ValueError, TypeError, AttributeError, KeyError):
        failures = ["Published feed could not be downloaded or validated."]
    if args.kind == "cruise":
        try:
            state = published_json(CRUISE_STATE, 65_536)
            failures.extend(cruise_state_problems(state, datetime.now(timezone.utc)))
        except (OSError, ValueError, TypeError, AttributeError, KeyError):
            failures.append("Cruise collection state could not be downloaded or validated.")
    alert(args.repository, failures)
    print(json.dumps({"healthy": not failures, "problems": failures}))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
