import unittest
from unittest.mock import patch
from check_sources import run
from check_sources import fingerprint, update, SameHost
import urllib.request
class Tests(unittest.TestCase):
    def test_initial_fetch_requires_review(self):
        row = update({}, "a", "now")
        self.assertTrue(row["reviewRequired"])
        self.assertNotIn("reviewedHash", row)
    def test_unchanged_does_not_clear_pending(self):
        row = update(update({}, "a", "before"), "a", "now")
        self.assertTrue(row["reviewRequired"])
        self.assertEqual(row["lastChangeDetectedAt"], "before")
    def test_reviewed_hash_and_changed_hash(self):
        old = {"reviewedHash": "a", "reviewedAt": "2026-11-07T00:00:00+00:00"}
        self.assertFalse(update(old, "a", "2026-11-08T00:00:00+00:00")["reviewRequired"])
        self.assertTrue(update(old, "b", "now")["reviewRequired"])
        self.assertEqual(update(old, "b", "now")["reviewedAt"], "2026-11-07T00:00:00+00:00")
    def test_review_age(self):
        old = {"reviewedHash": "a", "reviewedAt": "2026-10-01T00:00:00+00:00"}
        self.assertTrue(update(old, "a", "2026-11-08T00:00:00+00:00")["reviewRequired"])
    def test_failure_preserves_freshness(self):
        with patch("check_sources.allowed", side_effect=ValueError("blocked")), patch("check_sources.time.sleep"):
            state = run({"sources": [{"id": "a", "url": "https://example.com"}]}, {"sources": {"a": {"lastSuccessfulCheckAt": "before", "reviewedHash": "old"}}})
        self.assertEqual(state["sources"]["a"]["lastSuccessfulCheckAt"], "before")
        self.assertEqual(state["sources"]["a"]["status"], "check_failed")
    def test_normalization(self):
        text = "Hours and facilities " * 10
        self.assertEqual(fingerprint("<p>"+text+"</p>"), fingerprint("<p>"+text+"</p><script>random</script>"))
    def test_image_schedule_change(self):
        text = "Operating schedule " * 10
        self.assertNotEqual(fingerprint(text+'<img src="a.png">'), fingerprint(text+'<img src="b.png">'))
    def test_block_page(self):
        with self.assertRaises(ValueError): fingerprint("Access denied " * 20)
    def test_redirect_guard(self):
        with self.assertRaises(ValueError):
            SameHost().redirect_request(urllib.request.Request("https://example.com"), None, 302, "", {}, "https://elsewhere.com")
if __name__ == "__main__": unittest.main()
