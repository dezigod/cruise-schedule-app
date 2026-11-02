import importlib.util
import base64
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
        payload = base64.b64encode(b'{"days":[{"date":"2026-11-02","ships":[]}]}').decode()
        return {"schemaVersion": 1, "payload": payload, "documents": [
            {"id": "wico-schedule-0", "url": "https://wico-ltd.com/FY-2027-REV-4.pdf", "checkedAt": at.isoformat()},
            {"id": "wico-schedule-1", "url": "https://wico-ltd.com/November-2026-Arrivals.pdf", "checkedAt": at.isoformat()},
            {"id": "vipa-schedule-0", "sourceRevision": "Fiscal year 2027", "checkedAt": at.isoformat()}]}

    def test_each_expected_document_is_required_even_when_both_providers_are_present(self):
        for index in range(3):
            feed = self.cruise()
            del feed["documents"][index]
            self.assertEqual(len(monitor.problems("cruise", feed, NOW)), 1)

    def test_previous_month_and_previous_fiscal_year_do_not_count(self):
        feed = self.cruise()
        feed["documents"][1]["url"] = "https://wico-ltd.com/October-2026-Arrivals.pdf"
        feed["documents"][2]["sourceRevision"] = "Fiscal year 2026"
        self.assertEqual(len(monitor.problems("cruise", feed, NOW)), 2)

    def test_future_by_one_second_is_reported(self):
        self.assertTrue(monitor.problems("cruise", self.cruise(NOW + timedelta(seconds=1)), NOW))

    def test_fresh_metadata_cannot_hide_unreadable_calendar(self):
        feed = self.cruise()
        for invalid in ("bad%", base64.b64encode(b'{"days":[]}').decode()):
            feed["payload"] = invalid
            self.assertTrue(monitor.problems("cruise", feed, NOW))

    def test_fresh_feed_and_unchanged_source_check_are_healthy(self):
        self.assertEqual(monitor.problems("cruise", self.cruise(), NOW), [])

    def test_green_but_deferred_runs_cannot_hide_stale_feed(self):
        self.assertTrue(monitor.problems("cruise", self.cruise(NOW-timedelta(hours=13)), NOW))

    def test_missing_provider_or_timestamp_and_future_timestamp_fail(self):
        for value in [{}, {"documents": [{"id": "wico-0"}]}, self.cruise(NOW+timedelta(hours=1))]:
            self.assertTrue(monitor.problems("cruise", value, NOW))

    def test_recent_download_cannot_hide_old_source_publication(self):
        value = {"collectionStatus": "ok", "source": {"checkedAt": NOW.isoformat(), "updatedAt": (NOW-timedelta(hours=1)).isoformat()}}
        value.update(schemaVersion=1, islands=[{"id": i} for i in ("stThomas", "stCroix", "stJohn", "waterIsland")])
        self.assertTrue(monitor.problems("outage", value, NOW))
        value["source"]["updatedAt"] = NOW.isoformat()
        self.assertEqual(monitor.problems("outage", value, NOW), [])
        value["collectionStatus"] = "failed"
        self.assertTrue(monitor.problems("outage", value, NOW))

    def test_provider_refusal_and_overdue_publication_are_visible_before_feed_goes_stale(self):
        state = {"providers": {"vipa": {"accessRefused": True}, "wico": {"failures": 1}},
                 "pendingDelivery": {"baseBlob": "a" * 40, "feedBlob": "b" * 40},
                 "lastSuccess": (NOW - timedelta(minutes=31)).isoformat()}
        issues = monitor.cruise_state_problems(state, NOW)
        self.assertEqual(len(issues), 3)
        self.assertTrue(any("VIPA refused" in msg for msg in issues))
        state["providers"] = {"wico": {"failures": 0}, "vipa": {"failures": 0}}
        state["lastSuccess"] = NOW.isoformat()
        self.assertEqual(monitor.cruise_state_problems(state, NOW), [])

    def test_incident_is_deduplicated_and_recovery_closes_it(self):
        import json
        with patch.object(monitor, "gh", return_value="[]") as created:
            monitor.alert("example/test", ["stale"])
            body = created.call_args.args[-1]
        row = {"title": monitor.TITLE, "number": 1, "body": body}
        with patch.object(monitor, "gh", return_value=json.dumps([[row]])) as gh:
            monitor.alert("example/test", ["stale"])
            self.assertEqual(gh.call_count, 1)
            monitor.alert("example/test", ["stale", "provider refused"])
            self.assertEqual(gh.call_args.args[:2], ("issue", "comment"))
            self.assertIn("provider refused", gh.call_args.args[-1])
            self.assertNotIn("- stale", gh.call_args.args[-1])
            monitor.alert("example/test", [])
            self.assertEqual(gh.call_args.args[:2], ("issue", "close"))

    def test_new_incident_mentions_owner(self):
        with patch.object(monitor, "gh", return_value="[]") as gh:
            monitor.alert("example/test", ["stale"])
            self.assertEqual(gh.call_args.args[:2], ("issue", "create"))
            self.assertIn("@dezigod", gh.call_args.args[-1])
