import os
import json
import requests

import google.generativeai as genai

SOURCE_URL = os.environ.get("SOURCE_URL")
OUTPUT_PATH = "schedule.json"

SYSTEM_INSTRUCTIONS = (
    "You extract cruise schedule data and output ONLY valid JSON.\n"
    "Return a JSON array of objects with EXACT keys:\n"
    "date (YYYY-MM-DD), island, ship, dock, arrival (HH:MM), departure (HH:MM).\n"
    "No extra keys. No markdown. No commentary. Only JSON."
)

def fetch_source_text(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def json_is_valid_schedule(data) -> bool:
    if not isinstance(data, list):
        return False
    required = {"date", "island", "ship", "dock", "arrival", "departure"}
    for row in data:
        if not isinstance(row, dict):
            return False
        if set(row.keys()) != required:
            return False
    return True

def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY secret")

    if not SOURCE_URL:
        raise RuntimeError("Missing SOURCE_URL env var")

    source_text = fetch_source_text(SOURCE_URL)

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    prompt = SYSTEM_INSTRUCTIONS + "\n\nSOURCE (raw HTML/text):\n" + source_text[:200000]

    resp = model.generate_content(prompt)
    raw = resp.text.strip()

    try:
        data = json.loads(raw)
    except Exception as e:
        raise RuntimeError(
            "Gemini did not return valid JSON. First 500 chars:\n" + raw[:500]
        ) from e

    if not json_is_valid_schedule(data):
        raise RuntimeError("JSON shape invalid")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Wrote", OUTPUT_PATH, "with", len(data), "rows.")


if __name__ == "__main__":
    main()
