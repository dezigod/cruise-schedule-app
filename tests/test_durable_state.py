import base64
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from io import BytesIO
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"scripts"))
from github_state import BRANCH, FEED, PATH, STAGED, GitHubState, blob_id, json_text, validate
from update_official_calendar import collect, guard_monthly_document
from official_fetch import CollectionBlocked

class DurableStateTests(unittest.TestCase):
    def test_stage_and_resume_after_failed_delivery_without_recollecting(self):
        from datetime import date
        remote = GitHubState("dezigod/cruise-schedule-app", "synthetic")
        old = blob_id(b"previous")
        remote.sha = old
        calendar = {"documents": [{"checkedAt": "2026-11-02T12:00:00Z"}], "delivery": "hosted"}
        branches = {BRANCH: {PATH: old}, "main": {FEED: old}}
        def snapshot(branch):
            return "tip", "tree", branches[branch].copy()
        def commit(branch, tip, tree, changes, message):
            for path, entry in changes.items():
                branches[branch][path] = entry["sha"] if "sha" in entry else blob_id(entry["content"].encode())
        with patch.object(remote, "snapshot", side_effect=snapshot), patch.object(remote, "commit_files", side_effect=commit):
            state = remote.stage({"providers": {}, "lastSuccess": "2026-11-02T12:01:00Z"}, calendar, old)
            self.assertEqual(branches[BRANCH][STAGED], blob_id(json_text(calendar).encode()))
            self.assertEqual(branches[BRANCH][PATH], blob_id(json_text(state).encode()))
            with patch.object(remote, "commit_files", side_effect=OSError("GitHub unavailable")), patch.object(remote, "save") as ack:
                with self.assertRaises(OSError):
                    remote.deliver(state.copy())
                ack.assert_not_called()
            self.assertEqual(branches["main"][FEED], old)
            with tempfile.TemporaryDirectory() as directory, patch("update_official_calendar.OfficialFetcher.fetch") as fetch:
                p = Path(directory)
                (p / "state").write_text(json.dumps(state))
                with self.assertRaisesRegex(CollectionBlocked, "before collecting"):
                    collect(p / "work", p / "state", date(2026, 11, 2))
                fetch.assert_not_called()
            # If main accepted the commit but the acknowledgement was lost,
            # the next run only needs to clear the original pending marker.
            with patch.object(remote, "save", side_effect=OSError("Lost acknowledgement")):
                with self.assertRaises(OSError):
                    remote.deliver(state.copy())
            self.assertEqual(branches["main"][FEED], branches[BRANCH][STAGED])
            with patch.object(remote, "save") as ack, patch.object(remote, "commit_files") as extra_commit:
                self.assertEqual(remote.deliver(state.copy()), "already published")
                extra_commit.assert_not_called()
                self.assertNotIn("pendingDelivery", ack.call_args.args[0])

    def test_conflicting_edit_wins_and_changed_state_fails_closed(self):
        remote = GitHubState("dezigod/cruise-schedule-app", "synthetic")
        remote.sha = "a" * 40
        state = {"providers": {}, "pendingDelivery": {"baseBlob": "b" * 40, "feedBlob": "c" * 40}}
        snapshots = [("t", "r", {PATH: remote.sha, STAGED: "c" * 40}), ("t", "r", {FEED: "d" * 40})]
        with (patch.object(remote, "snapshot", side_effect=snapshots),
              patch.object(remote, "save") as ack, patch.object(remote, "commit_files") as write):
            with self.assertRaisesRegex(ValueError, "independently replaced"):
                remote.deliver(state.copy())
            write.assert_not_called()
            self.assertNotIn("pendingDelivery", ack.call_args.args[0])
        with (patch.object(remote, "snapshot", return_value=("t", "r", {PATH: "changed"})),
              patch.object(remote, "commit_files") as write):
            with self.assertRaisesRegex(ValueError, "state changed"):
                remote.stage({"providers": {}}, {}, "b" * 40)
            with self.assertRaisesRegex(ValueError, "changed"):
                remote.deliver(state.copy())
            write.assert_not_called()

    def test_ref_update_never_forces_a_branch_or_replaces_unrelated_files(self):
        remote = GitHubState("dezigod/cruise-schedule-app", "synthetic")
        with patch.object(remote, "request", side_effect=[{"sha": "new-tree"}, {"sha": "new-commit"}, {}]) as request:
            remote.commit_files("main", "tip", "base", {FEED: {"sha": "blob"}}, "validated")
        tree, commit, update = [c.args for c in request.call_args_list]
        self.assertEqual(tree[2], {"base_tree": "base", "tree": [{"path": FEED, "type": "blob", "mode": "100644", "sha": "blob"}]})
        self.assertEqual(commit[2]["parents"], ["tip"])
        self.assertEqual(update, ("PATCH", "/git/refs/heads/main", {"sha": "new-commit", "force": False}))

    def test_known_month_cannot_vanish_but_rollover_can_use_annuals(self):
        from datetime import date
        last = {"documents": [{"id": "wico-schedule-1", "url": "https://wico-ltd.com/November-2026-Arrivals.pdf"}]}
        with self.assertRaisesRegex(CollectionBlocked, "monthly schedule disappeared"):
            guard_monthly_document(last, {"documents": []}, date(2026, 11, 28))
        guard_monthly_document(last, {"documents": []}, date(2026, 12, 1))

    def test_missing_remote_state_fails_closed(self):
        remote = GitHubState(repository="dezigod/cruise-schedule-app", token="synthetic")
        with patch.object(remote, "request", side_effect=OSError("missing")):
            with self.assertRaises(OSError): remote.load()
        with self.assertRaises(ValueError): remote.save({"providers": {}})

    def test_conditional_write_recovers_only_an_exact_already_saved_value(self):
        value = {"providers": {}, "lastAttempt": "2026-11-02T18:00:00Z"}
        for stored in (value, {"providers": {"vipa": {"accessRefused": True}}}):
            remote = GitHubState("dezigod/cruise-schedule-app", "synthetic")
            remote.sha = "prior"
            error = HTTPError("https://api.github.com/", 409, "conflict", {}, BytesIO())
            response = {"sha": "latest", "content": base64.b64encode(json.dumps(stored).encode()).decode()}
            with patch.object(remote, "request", side_effect=[error, response]) as request:
                if stored == value:
                    remote.save(value)
                    self.assertEqual(remote.sha, "latest")
                else:
                    with self.assertRaises(HTTPError):
                        remote.save(value)
                self.assertEqual([call.args[0] for call in request.call_args_list], ["PUT", "GET"])
            error.close()

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
