import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("feed_health", ROOT / "scripts/check_feed_health.py")
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
NOW = datetime(2026, 11, 2, 18, tzinfo=timezone.utc)

class FeedHealthTests(unittest.TestCase):
    def cruise(self, at=NOW):
        return {"documents": [{"id": p + "-schedule-0", "checkedAt": at.isoformat()} for p in ["wico", "vipa"]]}

    def test_fresh_feed_and_unchanged_source_check_are_healthy(self):
        self.assertEqual(monitor.problems("cruise", self.cruise(), NOW), [])

    def test_green_but_deferred_runs_cannot_hide_stale_feed(self):
        self.assertTrue(monitor.problems("cruise", self.cruise(NOW-timedelta(hours=13)), NOW))

    def test_missing_provider_or_timestamp_and_future_timestamp_fail(self):
        for value in [{}, {"documents": [{"id": "wico-0"}]}, self.cruise(NOW+timedelta(hours=1))]:
            self.assertTrue(monitor.problems("cruise", value, NOW))

    def test_recent_download_cannot_hide_old_source_publication(self):
        value = {"collectionStatus": "ok", "source": {"checkedAt": NOW.isoformat(), "updatedAt": (NOW-timedelta(hours=1)).isoformat()}}
        self.assertTrue(monitor.problems("outage", value, NOW))
        value["source"]["updatedAt"] = NOW.isoformat()
        self.assertEqual(monitor.problems("outage", value, NOW), [])
        value["collectionStatus"] = "failed"
        self.assertTrue(monitor.problems("outage", value, NOW))

    def test_incident_is_deduplicated_and_recovery_closes_it(self):
        import json
        with patch.object(monitor, "gh", return_value=json.dumps([{"title": monitor.TITLE, "number": 1}])) as gh:
            monitor.alert("example/test", ["stale"])
            self.assertEqual(gh.call_count, 1)
            monitor.alert("example/test", [])
            self.assertEqual(gh.call_args.args[:2], ("issue", "close"))

    def test_new_incident_mentions_owner(self):
        with patch.object(monitor, "gh", return_value="[]") as gh:
            monitor.alert("example/test", ["stale"])
            self.assertEqual(gh.call_args.args[:2], ("issue", "create"))
            self.assertIn("@dezigod", gh.call_args.args[-1])
