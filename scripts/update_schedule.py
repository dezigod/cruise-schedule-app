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
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    resp = session.get(url, timeout=45)

    # If VINow blocks GitHub Actions with a 403, fall back to Jina reader proxy.
    if resp.status_code == 403:
        proxy_url = f"{R_JINA_PREFIX}{url}"
        resp = session.get(proxy_url, timeout=45)

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
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY secret")

    source_text = fetch_source_text(SOURCE_URL)

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    resp = model.generate_content(
        SYSTEM_INSTRUCTIONS + "\n\nSOURCE:\n" + source_text[:200000]
    )
    raw = resp.text.strip()

    try:
        data = json.loads(raw)
    except Exception as e:
        raise RuntimeError(
            "Gemini did not return valid JSON. First 400 chars: " + raw[:400]
        ) from e

    if not json_is_valid_schedule(data):
        raise RuntimeError("JSON shape invalid. Got: " + str(data[:1]))

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(data)} rows.")


if __name__ == "__main__":
    main()
