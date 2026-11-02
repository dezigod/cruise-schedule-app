import base64
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"scripts"))
from github_state import GitHubState, validate
from update_official_calendar import collect

class DurableStateTests(unittest.TestCase):
    def test_missing_remote_state_fails_closed(self):
        remote = GitHubState(repository="dezigod/cruise-schedule-app", token="synthetic")
        with patch.object(remote, "request", side_effect=OSError("missing")):
            with self.assertRaises(OSError): remote.load()
        with self.assertRaises(ValueError): remote.save({"providers": {}})

    def test_state_is_allowlisted_and_refusal_survives_roundtrip(self):
        for bad in [{"secret": "private"}, {"providers": {"other": {}}}, {"providers": {"wico": {"raw": "private"}}}]:
            with self.assertRaises(ValueError): validate(bad)
        state = {"providers": {"wico": {"accessRefused": True, "lastHTTPStatus": 403}}}
        remote = GitHubState(repository="dezigod/cruise-schedule-app", token="synthetic")
        with patch.object(remote, "request", return_value={"sha": "version1", "content": base64.b64encode(json.dumps(state).encode()).decode()}):
            self.assertEqual(remote.load(), state)
        with patch.object(remote, "request", return_value={"content": {"sha": "version2"}}) as request:
            remote.save(state)
            self.assertEqual(request.call_args.args[2]["sha"], "version1")
            self.assertEqual(remote.sha, "version2")

    def test_failed_remote_reservation_prevents_provider_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)
            with patch("update_official_calendar.OfficialFetcher.fetch") as fetch:
                with self.assertRaises(OSError):
                    collect(p/"work", p/"state.json", persist=lambda value: (_ for _ in ()).throw(OSError("remote unavailable")))
                fetch.assert_not_called()

    def test_provider_failure_persists_backoff_for_next_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory); saved=[]
            def broken(fetcher, provider, *args, **kwargs):
                fetcher.cache["providers"][provider] = {"accessRefused": True, "lastHTTPStatus": 403}
                raise OSError("source failed")
            with patch("update_official_calendar.OfficialFetcher.fetch", new=broken):
                with self.assertRaises(OSError): collect(p/"work", p/"state.json", persist=lambda value: saved.append(json.loads(json.dumps(value))))
            self.assertIsNotNone(saved[0]["lastAttempt"])
            self.assertTrue(saved[-1]["providers"]["wico"]["accessRefused"])
            with patch("update_official_calendar.OfficialFetcher.fetch") as fetch:
                with self.assertRaises(Exception): collect(p/"new-runner", p/"state.json")
                fetch.assert_not_called()
