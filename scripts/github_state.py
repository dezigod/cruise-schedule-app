"""Durable collection state and recoverable delivery of the validated public calendar."""
import base64
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime

BRANCH = "collection-state"
PATH = "state.json"
FEED = "official-cruise-calendar-v1.json"
STAGED = "validated-calendar.json"


def json_text(value):
    return json.dumps(value, indent=2) + "\n"


def blob_id(data):
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def validate(value):
    if not isinstance(value, dict) or set(value) - {"providers", "lastSuccess", "lastAttempt", "pendingDelivery", "lastFailure"}:
        raise ValueError("Unexpected collector state")
    if "lastFailure" in value and value["lastFailure"] not in {"monthly_missing", "collection_failed"}:
        raise ValueError("Unexpected collection failure")
    if "pendingDelivery" in value:
        pending = value["pendingDelivery"]
        if (not isinstance(pending, dict) or set(pending) != {"baseBlob", "feedBlob"}
                or not all(isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{40}", sha) for sha in pending.values())):
            raise ValueError("Invalid pending delivery")
    for key in ("lastSuccess", "lastAttempt"):
        if value.get(key) is not None:
            if datetime.fromisoformat(value[key]).tzinfo is None:
                raise ValueError("State timestamp must have timezone")
    providers = value.get("providers", {})
    if not isinstance(providers, dict) or set(providers) - {"wico", "vipa"}:
        raise ValueError("Unexpected provider")
    for status in providers.values():
        if not isinstance(status, dict) or set(status) - {"failures", "accessRefused", "lastHTTPStatus", "nextAllowedAt"}:
            raise ValueError("Unexpected provider fields")
        if "failures" in status and (type(status["failures"]) is not int or status["failures"] < 0):
            raise ValueError("Invalid failure count")
        if "accessRefused" in status and type(status["accessRefused"]) is not bool:
            raise ValueError("Invalid refusal flag")
        if "lastHTTPStatus" in status and type(status["lastHTTPStatus"]) is not int:
            raise ValueError("Invalid HTTP status")
        if status.get("nextAllowedAt") and datetime.fromisoformat(status["nextAllowedAt"]).tzinfo is None:
            raise ValueError("Backoff timestamp must have timezone")
    return value


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise OSError("State API redirect refused")


class GitHubState:
    def __init__(self, repository=None, token=None):
        self.repository = repository or os.environ.get("GITHUB_REPOSITORY", "")
        self.token = token or os.environ.get("GH_TOKEN", "")
        if self.repository != "dezigod/cruise-schedule-app" or not self.token:
            raise ValueError("Expected cruise repository and credential")
        self.sha = None

    def request(self, method, path, payload=None):
        request = urllib.request.Request("https://api.github.com/repos/" + self.repository + path,
            method=method, data=json.dumps(payload).encode() if payload is not None else None,
            headers={"Authorization": "Bearer " + self.token, "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "CrowdCast-collector-state"})
        for attempt in range(2):
            try:
                with urllib.request.build_opener(NoRedirect()).open(request, timeout=20) as response:
                    body = response.read(65537)
                    if len(body) > 65536:
                        raise ValueError("State response too large")
                    return json.loads(body)
            except urllib.error.HTTPError as error:
                if attempt or error.code not in {500, 502, 503, 504} or error.headers.get("Retry-After"):
                    raise
                error.close()
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt:
                    raise
            time.sleep(2)

    def load(self):
        # Missing state fails closed. It must never silently reset an access refusal.
        result = self.request("GET", "/contents/" + PATH + "?ref=" + BRANCH)
        value = validate(json.loads(base64.b64decode(result["content"])))
        self.sha = result["sha"]
        return value

    def save(self, value):
        validate(value)
        if self.sha is None:
            raise ValueError("Load durable state before saving")
        try:
            result = self.request("PUT", "/contents/" + PATH, {
                "branch": BRANCH, "sha": self.sha, "message": "Persist cruise collection throttle and provider state",
                "content": base64.b64encode(json_text(value).encode()).decode()})
        except urllib.error.HTTPError as error:
            # A retried conditional write can receive a conflict after GitHub
            # accepted the original write but its response was lost.
            if error.code != 409 or self.load() != value:
                raise
            error.close()
            return
        # Expected blob SHA prevents overwriting concurrent or manually reviewed state.
        self.sha = result["content"]["sha"]

    def snapshot(self, branch):
        tip = self.request("GET", "/git/ref/heads/" + branch)["object"]["sha"]
        tree = self.request("GET", "/git/commits/" + tip)["tree"]["sha"]
        result = self.request("GET", "/git/trees/" + tree)
        if result.get("truncated"):
            raise ValueError("Incomplete branch snapshot")
        blobs = {e["path"]: e["sha"] for e in result["tree"] if e["mode"] == "100644" and e["type"] == "blob"}
        return tip, tree, blobs

    def commit_files(self, branch, tip, tree, changes, message):
        entries = [dict(path=path, mode="100644", type="blob", **change) for path, change in changes.items()]
        updated = self.request("POST", "/git/trees", {"base_tree": tree, "tree": entries})
        commit = self.request("POST", "/git/commits", {"message": message, "tree": updated["sha"], "parents": [tip]})
        # A concurrent branch change rejects this update. A lost reply can be
        # reconciled from the stored blob IDs on the next run.
        self.request("PATCH", "/git/refs/heads/" + branch, {"sha": commit["sha"], "force": False})

    def stage(self, state, calendar, base_blob):
        content = json_text(calendar)
        state = validate(dict(state, pendingDelivery={"baseBlob": base_blob, "feedBlob": blob_id(content.encode())}))
        tip, tree, files = self.snapshot(BRANCH)
        if not self.sha or files.get(PATH) != self.sha:
            raise ValueError("Collection state changed while validating; previous state retained")
        saved = json_text(state)
        # Only the derived feed goes into Git. Its bytes and the collection
        # success become durable in the same commit.
        self.commit_files(BRANCH, tip, tree, {STAGED: {"content": content}, PATH: {"content": saved}},
                          "Stage validated public cruise calendar and collection result")
        self.sha = blob_id(saved.encode())
        return state

    def deliver(self, state=None):
        state = self.load() if state is None else state
        pending = state.get("pendingDelivery")
        if not pending:
            return "nothing pending"
        if any(s.get("accessRefused") for s in state.get("providers", {}).values()):
            raise ValueError("Access refusal requires review before releasing the staged calendar")
        _, _, staged = self.snapshot(BRANCH)
        if staged.get(PATH) != self.sha or staged.get(STAGED) != pending["feedBlob"]:
            raise ValueError("Staged calendar or collection state changed; inspect before publishing")
        tip, tree, files = self.snapshot("main")
        current = files.get(FEED)
        if current is None:
            raise ValueError("Published calendar missing; inspect main before restoring")
        if current == pending["feedBlob"]:
            outcome = "already published"
        elif current == pending["baseBlob"]:
            self.commit_files("main", tip, tree, {FEED: {"sha": pending["feedBlob"]}},
                              "chore: refresh official cruise calendar")
            outcome = "published"
        else:
            # An independent replacement takes precedence. Preserve both in
            # Git history and allow the next due collection to use the new base.
            outcome = "superseded"
        del state["pendingDelivery"]
        self.save(state)
        if outcome == "superseded":
            raise ValueError("Main calendar was independently replaced; staged copy retained for review")
        return outcome


if __name__ == "__main__":
    print(json.dumps({"delivery": GitHubState().deliver()}))
