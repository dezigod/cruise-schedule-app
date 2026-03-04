import os
import json
import requests
from google import genai

DEFAULT_SOURCE_URL = "https://www.vinow.com/cruise/ship-schedule/"
SOURCE_URL = os.environ.get("SOURCE_URL", DEFAULT_SOURCE_URL)
OUTPUT_PATH = "schedule.json"

MODEL_NAME = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")

SYSTEM_INSTRUCTIONS = """You extract cruise schedule data and output ONLY valid JSON.
Return a JSON array of objects with EXACT keys:
  - date (YYYY-MM-DD)
  - island
  - ship
  - dock
  - arrival (HH:MM)
  - departure (HH:MM)
No extra keys. No markdown. No commentary. Only JSON.
"""

R_JINA_PREFIX = "https://r.jina.ai/"
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


def fetch_source_text(url: str) -> str:
    sess = requests.Session()
    sess.headers.update(BROWSER_HEADERS)

    resp = sess.get(url, timeout=45)
    if resp.status_code == 403:
        proxy_url = f"{R_JINA_PREFIX}{url}"
        resp = sess.get(proxy_url, timeout=45)

    resp.raise_for_status()
    return resp.text


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


def main() -> None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY secret")

    source_text = fetch_source_text(SOURCE_URL)

    client = genai.Client(api_key=api_key)
    prompt = f"{SYSTEM_INSTRUCTIONS}\n\nSOURCE:\n{source_text[:180000]}"
    response = client.models.generate_content(model=MODEL_NAME, contents=prompt)

    raw = (getattr(response, "text", None) or "").strip()

    try:
        data = json.loads(raw)
    except Exception as e:
        raise RuntimeError(
            f"Gemini did not return valid JSON. First 400 chars:\n{raw[:400]}"
        ) from e

    if not json_is_valid_schedule(data):
        raise RuntimeError(
            "JSON shape invalid. Ensure only date/island/ship/dock/arrival/departure."
        )

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(data)} rows.")


if __name__ == "__main__":
    main()
