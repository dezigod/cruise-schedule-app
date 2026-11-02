"""Durable, allowlisted collector state. No source documents or credentials are stored."""
import base64
import json
import os
import re
import urllib.request
from datetime import datetime

BRANCH = "collection-state"
PATH = "state.json"


def validate(value):
    if not isinstance(value, dict) or set(value) - {"providers", "lastSuccess", "lastAttempt"}:
        raise ValueError("Unexpected collector state")
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
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            body = response.read(65537)
            if len(body) > 65536:
                raise ValueError("State response too large")
            return json.loads(body)

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
        result = self.request("PUT", "/contents/" + PATH, {
            "branch": BRANCH, "sha": self.sha, "message": "Persist cruise collection throttle and provider state",
            "content": base64.b64encode((json.dumps(value, indent=2) + "\n").encode()).decode()})
        # Expected blob SHA prevents overwriting concurrent or manually reviewed state.
        self.sha = result["content"]["sha"]
