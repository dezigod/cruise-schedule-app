import os
import json
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
        "text/html,application/xhtml+xml,application/xml;q=0.9," "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

def short_model_name(name: str | None) -> str | None:
    if not name:
        return None
    # Allow passing either "models/gemini-1.5-flash" or "gemini-1.5-flash"
    return name.split("/")[-1]

def get_supported_methods(model) -> list[str]:
    # SDK field names sometimes differ; check a few options.
    for attr in (
        "supportedGenerationMethods",
        "supported_generation_methods",
        "generation_methods",
        "generationMethods",
        "supported_methods",
        "supportedMethods",
    ):
        methods = getattr(model, attr, None)
        if methods:
            return [str(m) for m in methods]
    return []

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

def list_candidate_model_names(client: genai.Client) -> list[str]:
    models = list(client.models.list())

    def candidate(model) -> bool:
        nm = str(getattr(model, "name", "")).lower()
        methods = " ".join(get_supported_methods(model)).lower()

        if "embed" in nm or "embedding" in nm:
            return False
        if "embed" in methods or "embedding" in methods:
            return False

        return "gemini" in nm

    candidates: list[str] = []

    # If the user explicitly set a model name, keep it first.
    if ENV_MODEL_NAME:
        candidates.append(ENV_MODEL_NAME)

    for model in models:
        name = short_model_name(getattr(model, "name", None)) or getattr(model, "name", None)
        if not name:
            continue
        if candidate(model):
            candidates.append(name)

    # De-dupe while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for name in candidates:
        if name not in seen:
            seen.add(name)
            result.append(name)

    return result

def main() -> None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY secret")

    client = genai.Client(api_key=api_key)

    model_names = list_candidate_model_names(client)
    if not model_names:
        raise RuntimeError(
            "No candidate Gemini models found for this API key. "
            "Check API key access in Google AI Studio."
        )

    source_text = fetch_source_text(SOURCE_URL)
    prompt = f"{SYSTEM_INSTRUCTIONS}\n\nSOURCE:\n{source_text[:180000]}"

    last_error: Exception | None = None
    response = None

    for model_name in model_names:
        try:
            response = client.models.generate_content(model=model_name, contents=prompt)
            # If we got here, we succeeded.
            break
        except Exception as e:
            last_error = e
            continue

    if response is None:
        raise RuntimeError(
            "Could not generate content with any model name. Last error: %s"
            % (last_error or "Unknown")
        )

    raw = (getattr(response, "text", None) or "").strip()

    try:
        data = json.loads(raw)
    except Exception as e:
        raise RuntimeError(
            f"Gemini did not return valid JSON. First 400 chars:\n{raw[:400]}"
        ) from e

    if not json_is_valid_schedule(data):
        raise RuntimeError("JSON shape invalid (wrong keys).")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(data)} rows.")

if __name__ == "__main__":
    main()
