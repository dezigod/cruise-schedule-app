import os
import json
import requests

import google.generativeai as genai

DEFAULT_SOURCE_URL = "https://www.vinow.com/cruise/ship-schedule/"
SOURCE_URL = os.environ.get("SOURCE_URL", DEFAULT_SOURCE_URL)

OUTPUT_PATH = "schedule.json"

SYSTEM_INSTRUCTIONS = (
    "You extract cruise schedule data and output ONLY valid JSON.\n"
    "Return a JSON array of objects with EXACT keys:\n"
    "date (YYYY-MM-DD), island, ship, dock, arrival (HH:MM), departure (HH:MM).\n"
    "No extra keys. No markdown. No commentary. Only JSON."
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

session = requests.Session()
session.headers.update(BROWSER_HEADERS)

def proxy_url(url: str) -> str:
    if url.startswith("https://"):
        return "https://r.jina.ai/http/" + url[len("https://") :]
    if url.startswith("http://"):
        return "https://r.jina.ai/http/" + url[len("http://") :]
    return "https://r.jina.ai/http/" + url

def fetch_source_text(url: str) -> str:
    if not url:
        raise RuntimeError("Missing SOURCE_URL env var")
    resp = session.get(url, timeout=45)
    if resp.status_code == 403:
        resp = session.get(proxy_url(url), timeout=45)
    resp.raise_for_status()
    return resp.text

def json_is_valid_schedule(data) -> bool:
    required = {"date", "island", "ship", "dock", "arrival", "departure"}
    if not isinstance(data, list):
        return False
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

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    source_text = fetch_source_text(SOURCE_URL)
    prompt = f"""{SYSTEM_INSTRUCTIONS}

SOURCE:
{source_text[:200000]}
"""
    resp = model.generate_content(prompt)
    raw = (resp.text or "").strip()

    data = json.loads(raw)
    if not json_is_valid_schedule(data):
        keys = (
            list(data[0].keys())
            if data and isinstance(data, list) and isinstance(data[0], dict)
            else type(data)
        )
        raise RuntimeError(f"JSON shape invalid. Keys: {keys}")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(data)} rows.")

if __name__ == "__main__":
    main()
