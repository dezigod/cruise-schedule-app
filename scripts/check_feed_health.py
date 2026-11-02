"""Check delivered data, not workflow badges. No direct provider requests."""
import argparse
from datetime import datetime, timezone
import json
import subprocess
import urllib.request

TITLE = "Feed freshness alert"
CONFIG = {
    "cruise": ("https://raw.githubusercontent.com/dezigod/cruise-schedule-app/main/official-cruise-calendar-v1.json", 12 * 3600),
    "outage": ("https://raw.githubusercontent.com/dezigod/crowdcast-outage-feed/data/outages.json", 30 * 60),
}


def timestamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Missing timezone")
    return result


def problems(kind, feed, now):
    limit = CONFIG[kind][1]
    failures = []
    if kind == "cruise":
        documents = feed.get("documents")
        if not isinstance(documents, list) or not documents:
            return ["Cruise feed has no source documents."]
        if {d.get("id", "").split("-")[0] for d in documents} != {"wico", "vipa"}:
            failures.append("Cruise feed is missing an expected provider.")
        checks = [("Cruise source check", d.get("checkedAt")) for d in documents]
    else:
        checks = [("Outage collection", feed.get("source", {}).get("checkedAt")),
                  ("WAPA source publication", feed.get("source", {}).get("updatedAt"))]
        if feed.get("collectionStatus") != "ok":
            failures.append("Outage collector has not verified its latest attempt.")
    for label, value in checks:
        try:
            age = (now - timestamp(value)).total_seconds()
            if age < -300:
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


def alert(repository, failures):
    rows = json.loads(gh("issue", "list", "--repo", repository, "--state", "open",
                         "--search", '"' + TITLE + '" in:title', "--json", "number,title", "--limit", "100"))
    incident = next((row for row in rows if row["title"] == TITLE), None)
    if failures:
        body = ("@dezigod Feed delivery needs attention.\n\n" + "\n".join("- " + f for f in failures) +
                "\n\nCheck the collector's Actions logs and the published source timestamps. "
                "The previous valid feed must be retained. Never bypass provider refusals or reset source timestamps. "
                "This issue closes automatically after the delivered feed is healthy again.")
        if incident is None:
            gh("issue", "create", "--repo", repository, "--title", TITLE, "--body", body)
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
        request = urllib.request.Request(CONFIG[args.kind][0], headers={"User-Agent": "CrowdCast-feed-health", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read(3_000_001)
        if len(body) > 3_000_000:
            raise ValueError("Feed size exceeded")
        failures = problems(args.kind, json.loads(body), datetime.now(timezone.utc))
    except (OSError, ValueError, TypeError, AttributeError):
        failures = ["Published feed could not be downloaded or validated."]
    alert(args.repository, failures)
    print(json.dumps({"healthy": not failures, "problems": failures}))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
