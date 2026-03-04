import os
import sys
import json
import re
import requests
from google import genai

DEFAULT_SOURCE_URL = "https://www.vinow.com/cruise/ship-schedule/"
SOURCE_URL = os.environ.get("SOURCE_URL", DEFAULT_SOURCE_URL)
OUTPUT_PATH = "schedule.json"
ENV_MODEL_NAME = (os.environ.get("GEMINI_MODEL") or "").strip() or None

SYSTEM_INSTRUCTIONS = """You extract cruise schedule data and output ONLY valid JSON.
Return a JSON array of objects with EXACT keys:
date (YYYY-MM-DD), island, ship, dock, arrival (HH:MM), departure (HH:MM).
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
        "text/html,application/xhtml+xml,application/xml;q=0.9," +
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
        resp = sess.get(f"{R_JINA_PREFIX}{url}", timeout=45)

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


def extract_json_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return t

    # remove markdown code fences
    t = re.sub(r"```json\s*([\s\S]*?)```", r"\1", t, flags=re.IGNORECASE)
    t = re.sub(r"```([\s\S]*?)```", r"\1", t)
    t = t.strip()

    # strip json(...) wrapper
    if t.lower().startswith("json(") and t.endswith(")"):
        t = t[5:-1].strip()
    if t.lower().startswith("json="):
        t = t[5:].strip()

    # strip leading junk before first JSON bracket
    first = None
    for ch in ["[", "{"]:
        idx = t.find(ch)
        if idx != -1:
            if first is None or idx < first:
                first = idx
    if first and first > 0:
        t = t[first:]

    return t


def short_model_name(name: str) -> str:
    return name.split("/")[-1] if name else name


def build_model_candidates(client) -> list[str]:
    candidates = []
    tried: set[str] = set()

    if ENV_MODEL_NAME:
        candidates.append(ENV_MODEL_NAME)
        tried.add(ENV_MODEL_NAME)

    try:
        for model in client.models.list():
            name = getattr(model, "name", None)
            if not name:
                continue

            short = short_model_name(name)

            for nm in [name, short]:
                nm = nm.strip()
                if not nm or nm in tried:
                    continue
                # skip embeddings
                if "embed" in nm.lower():
                    continue
                candidates.append(nm)
                tried.add(nm)
    except Exception:
        pass

    common = [
        "gemini-1.5-pro-002",
        "gemini-1.5-pro",
        "gemini-1.5-flash-002",
        "gemini-1.5-flash",
        "gemini-1.0-pro-001",
        "gemini-1.0-pro",
    ]
    for nm in common:
        if nm not in tried:
            candidates.append(nm)
            tried.add(nm)

    # de-dup while preserving order
    unique: list[str] = []
    seen: set[str] = set()
    for nm in candidates:
        if nm and nm not in seen:
            unique.append(nm)
            seen.add(nm)

    return unique


def main() -> None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY secret")

    client = genai.Client(api_key=api_key)

    model_names = build_model_candidates(client)
    if not model_names:
        raise RuntimeError("No candidate models to try.")

    source_text = fetch_source_text(SOURCE_URL)
    prompt = f"{SYSTEM_INSTRUCTIONS}\n\nSOURCE:\n{source_text[:180000]}"

    last_err: Exception | None = None
    for model_name in model_names:
        try:
            response = client.models.generate_content(model=model_name, contents=prompt)
            raw = (getattr(response, "text", None) or "").strip()
            raw = extract_json_text(raw)
            data = json.loads(raw)
            if not json_is_valid_schedule(data):
                raise RuntimeError("JSON shape invalid.")

            with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"Success with model {model_name}. Wrote {OUTPUT_PATH} with {len(data)} rows.")
            return
        except Exception as e:
            last_err = e
            print(f"Model {model_name} failed: {e}", file=sys.stderr)

    raise RuntimeError(f"All models failed. Last error: {last_err}")


if __name__ == "__main__":
    main()
