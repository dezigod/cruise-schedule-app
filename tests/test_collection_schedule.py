"""Offline regressions: deferral must not masquerade as a fresh collection."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from official_fetch import CollectionBlocked
from update_official_calendar import CollectionDeferred, INTERVAL, collect, main


class CollectionScheduleTests(unittest.TestCase):
    now = datetime(2026, 10, 15, 19, 0, tzinfo=timezone.utc)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state.json"
        self.work = self.root / "work"
        self.work.mkdir()
        self.previous = self.work / "official-cruise-calendar-v1.json"
        self.previous.write_text("previous validated feed")
        self.output = self.root / "github-output"

    def saved_state(self, age, providers=None):
        self.state.write_text(json.dumps({
            "lastSuccess": (self.now - timedelta(seconds=age)).isoformat(),
            "providers": providers or {},
        }))

    def test_recent_collection_defers_without_network_or_changing_saved_data(self):
        for age in (0, INTERVAL - 600, INTERVAL - 1, -300):
            with self.subTest(age=age):
                self.saved_state(age)
                before = self.state.read_bytes()
                with patch("update_official_calendar.datetime", wraps=datetime) as clock, \
                     patch("update_official_calendar.OfficialFetcher.fetch") as fetch:
                    clock.now.return_value = self.now
                    with self.assertRaises(CollectionDeferred):
                        collect(self.work, self.state, date(2026, 10, 15))
                    fetch.assert_not_called()
                self.assertEqual(self.state.read_bytes(), before)
                self.assertEqual(self.previous.read_text(), "previous validated feed")

    def test_exact_six_hour_boundary_and_older_state_allow_a_request(self):
        for age in (INTERVAL, INTERVAL + 3600):
            with self.subTest(age=age):
                self.saved_state(age)
                with patch("update_official_calendar.datetime", wraps=datetime) as clock, \
                     patch("update_official_calendar.OfficialFetcher.fetch",
                           side_effect=CollectionBlocked("Offline network boundary")) as fetch:
                    clock.now.return_value = self.now
                    with self.assertRaisesRegex(CollectionBlocked, "Offline network boundary"):
                        collect(self.work, self.state, date(2026, 10, 15))
                    fetch.assert_called_once()
                self.assertEqual(self.previous.read_text(), "previous validated feed")

    def test_access_refusal_still_fails_even_with_recent_success(self):
        self.saved_state(1, {"vipa": {"accessRefused": True}})
        with patch("update_official_calendar.OfficialFetcher.fetch") as fetch:
            with self.assertRaisesRegex(CollectionBlocked, "access refused") as caught:
                collect(self.work, self.state, date(2026, 10, 15))
            self.assertNotIsInstance(caught.exception, CollectionDeferred)
            fetch.assert_not_called()

    def test_recent_failed_attempt_defers_without_claiming_a_new_success(self):
        self.saved_state(INTERVAL * 2)
        state=json.loads(self.state.read_text())
        state["lastAttempt"]=(self.now-timedelta(hours=1)).isoformat()
        self.state.write_text(json.dumps(state))
        before=self.state.read_bytes()
        with patch("update_official_calendar.datetime", wraps=datetime) as clock, \
             patch("update_official_calendar.OfficialFetcher.fetch") as fetch:
            clock.now.return_value=self.now
            with self.assertRaisesRegex(CollectionDeferred,"Previous collection attempt"):
                collect(self.work,self.state,date(2026,10,15))
            fetch.assert_not_called()
        self.assertEqual(self.state.read_bytes(),before)

    def test_real_failure_records_attempt_without_advancing_success(self):
        self.saved_state(INTERVAL * 2)
        last_success=json.loads(self.state.read_text())["lastSuccess"]
        with patch("update_official_calendar.datetime", wraps=datetime) as clock, \
             patch("update_official_calendar.OfficialFetcher.fetch",
                   side_effect=CollectionBlocked("Offline network failure")):
            clock.now.return_value=self.now
            with self.assertRaisesRegex(CollectionBlocked,"Offline network failure"):
                collect(self.work,self.state,date(2026,10,15))
        state=json.loads(self.state.read_text())
        self.assertEqual(state["lastSuccess"],last_success)
        self.assertEqual(state["lastAttempt"],self.now.isoformat())
        self.assertEqual(self.previous.read_text(),"previous validated feed")

    def test_provider_backoff_still_fails_without_network(self):
        self.saved_state(INTERVAL + 1, {"vipa": {
            "nextAllowedAt": (self.now + timedelta(hours=1)).isoformat(),
        }})
        with patch("update_official_calendar.datetime", wraps=datetime) as clock, \
             patch("update_official_calendar.OfficialFetcher.fetch") as fetch:
            clock.now.return_value = self.now
            with self.assertRaisesRegex(CollectionBlocked, "backoff") as caught:
                collect(self.work, self.state, date(2026, 10, 15))
            self.assertNotIsInstance(caught.exception, CollectionDeferred)
            fetch.assert_not_called()

    def run_cli(self):
        main(["--work", str(self.work), "--state", str(self.state)])

    def test_deferred_cli_succeeds_but_does_not_enable_publication(self):
        log = io.StringIO()
        self.output.write_text("unrelated=preserved\n")
        with patch.dict(os.environ, {"GITHUB_OUTPUT": str(self.output)}), \
             patch("update_official_calendar.collect", side_effect=CollectionDeferred("recent")), \
             contextlib.redirect_stdout(log):
            self.run_cli()
        self.assertEqual(self.output.read_text(), "unrelated=preserved\ncollected=false\n")
        self.assertEqual(json.loads(log.getvalue()), {
            "status": "deferred", "reason": "recent", "published": False,
        })
        self.assertEqual(self.previous.read_text(), "previous validated feed")

    def test_successful_cli_enables_publication_only_after_validation(self):
        with patch.dict(os.environ, {"GITHUB_OUTPUT": str(self.output)}), \
             patch("update_official_calendar.collect", return_value={
                 "documents": [1, 2], "heldDays": [], "conflictDays": [],
             }), contextlib.redirect_stdout(io.StringIO()):
            self.run_cli()
        self.assertEqual(self.output.read_text(), "collected=true\n")

    def test_real_collection_errors_remain_failures_and_keep_previous_feed(self):
        for error in (CollectionBlocked("Provider denied"), ValueError("Invalid state")):
            with self.subTest(error=type(error).__name__):
                with patch.dict(os.environ, {"GITHUB_OUTPUT": str(self.output)}), \
                     patch("update_official_calendar.collect", side_effect=error):
                    with self.assertRaises(type(error)):
                        self.run_cli()
                self.assertFalse(self.output.exists())
                self.assertEqual(self.previous.read_text(), "previous validated feed")


if __name__ == "__main__":
    unittest.main()
