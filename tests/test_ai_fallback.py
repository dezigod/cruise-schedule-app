"""Offline tests for the legacy scraper's OpenAI fallback. No network calls."""
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]

# CI installs only the official-parser requirements, so stand in for the
# scraper-only packages when they are absent. The tests never use them.
for name in ("openai", "requests"):
    if importlib.util.find_spec(name) is None:
        stub = types.ModuleType(name)
        stub.OpenAI = object
        stub.Session = object
        sys.modules[name] = stub

spec = importlib.util.spec_from_file_location("update_schedule", ROOT / "scripts/update_schedule.py")
scraper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scraper)

SHIP = {
    "date": "2026-12-03", "name": "Example Ship", "line": "Example Line", "passengers": 3000,
    "rawDock": "Havensight", "arrival": "08:00", "departure": "17:00",
}
PAGE = "<html><head><title>x</title></head><body><script>var secret = 1;</script><h3>December</h3><p>Example Ship</p></body></html>"


class FakeResponses:
    def __init__(self, output_text):
        self.output_text = output_text
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(output_text=self.output_text)


class FakeClient:
    def __init__(self, output_text):
        self.responses = FakeResponses(output_text)


class AIFallbackTests(unittest.TestCase):
    def test_extracts_ships_with_strict_schema_and_page_text_only(self):
        client = FakeClient(json.dumps({"ships": [SHIP]}))
        ships = scraper.extract_with_ai_fallback(client, "2026-12", PAGE)
        self.assertEqual(ships, [SHIP])
        call = client.responses.calls[0]
        self.assertEqual(call["model"], scraper.OPENAI_MODEL)
        self.assertTrue(call["text"]["format"]["strict"])
        self.assertEqual(call["text"]["format"]["type"], "json_schema")
        self.assertIn("2026-12", call["instructions"])
        self.assertIn("Example Ship", call["input"])
        self.assertNotIn("secret", call["input"])

    def test_skips_without_client_empty_page_or_cloudflare_challenge(self):
        self.assertEqual(scraper.extract_with_ai_fallback(None, "2026-12", PAGE), [])
        client = FakeClient("[]")
        self.assertEqual(scraper.extract_with_ai_fallback(client, "2026-12", "   "), [])
        challenge = "<html><title>Just a moment...</title><body>Enable JavaScript and cookies to continue</body></html>"
        self.assertEqual(scraper.extract_with_ai_fallback(client, "2026-12", challenge), [])
        self.assertEqual(client.responses.calls, [])

    def test_failed_calls_return_empty_list(self):
        client = FakeClient("not json")
        with patch.object(scraper.time, "sleep"):
            self.assertEqual(scraper.extract_with_ai_fallback(client, "2026-12", PAGE), [])
        self.assertEqual(len(client.responses.calls), 3)

    def test_parser_accepts_wrapper_bare_array_and_fenced_json(self):
        self.assertEqual(scraper.parse_ai_json_array(json.dumps({"ships": [SHIP]})), [SHIP])
        self.assertEqual(scraper.parse_ai_json_array(json.dumps([SHIP])), [SHIP])
        self.assertEqual(scraper.parse_ai_json_array("```json\n" + json.dumps([SHIP]) + "\n```"), [SHIP])
        with self.assertRaises(ValueError):
            scraper.parse_ai_json_array('{"other": 1}')

    def test_page_text_is_capped(self):
        with patch.object(scraper, "AI_FALLBACK_MAX_CHARS", 10):
            self.assertLessEqual(len(scraper.sanitize_html_for_prompt("<p>" + "a" * 100 + "</p>")), 10)


if __name__ == "__main__":
    unittest.main()
