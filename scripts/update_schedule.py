import os
import sys
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

def get_supported_methods(model) -> set:
    for attr in (
        "supported_generation_methods",
        "supportedGenerationMethods",
        "supported_methods",
        "supportedMethods",
    ):
        v = getattr(model, attr, None)
        if v:
            try:
                return set(v)
            except Exception:
                return set()
    return set()

def model_name_from(model) -> str | None:
    # try multiple attributes
    name = getattr(model, "name", None) or getattr(model, "model", None) or getattr(model, "id", None)
    if name:
        return str(name)
    return None

def choose_models(client) -> list[str]:
    candidates: list[str] = []
    if ENV_MODEL_NAME:
        candidates.append(ENV_MODEL_NAME)
    for m in client.models.list():
        name = model_name_from(m)
        if not name:
            continue
        methods = get_supported_methods(m)
        if "generateContent" not in methods:
            continue
        lower_name = name.lower()
        if "embed" in lower_name or "embedding" in lower_name:
            continue
        short = name.split("/")[-1]
        candidates.append(short)
    out: list[str] = []
    seen: set[str] = set()
    for n in candidates:
        if n and n not in seen:
            out.append(n)
            seen.add(n)
    return out

def extract_json_text(raw: str) -> str:
    t = (raw or "").strip()
    # remove code fences ```json ... ```
    if t.startswith("```"):
        lines = t.splitlines()
        # drop any leading fence lines
        while lines and lines[0].lstrip().startswith("```"):
            lines.pop(0)
        # drop any trailing fence lines
        while lines and lines[-1].strip().startswith("```"):
            lines.pop()
        t = "\n".join(lines).strip()
    # remove json(...) wrapper
    if t.startswith("json(") and t.endswith(")"):
        t = t[len("json("):-1].strip()
    # drop anything before the first JSON bracket
    if t and t[0] not in "[{":
        idx = min((i for i in [t.find("{"), t.find("[") if t.find("[") != -1 else None] if i is not None), default=-1)
        if idx > 0:
            t = t[idx:]
    return t

def main() -> None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY secret")

    client = genai.Client(api_key=api_key)
    model_names = choose_models(client)
    if not model_names:
        raise RuntimeError("No models support generateContent.")

    source_text = fetch_source_text(SOURCE_URL)
    prompt = f"{SYSTEM_INSTRUCTIONS}\n\nSOURCE:\n{source_text[:180000]}"

    last_err: Exception | None = None
    for model_name in model_names:
        try:
            resp = client.models.generate_content(model=model_name, contents=prompt)
            raw = getattr(resp, "text", None)
            if raw is None and getattr(resp, "candidates", None):
                try:
                    raw = resp.candidates[0].content.parts[0].text
                except Exception:
                    raw = ""
            cleaned = extract_json_text(raw or "")
            data = json.loads(cleaned)
            if not json_is_valid_schedule(data):
                raise RuntimeError("JSON shape invalid.")
            with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Wrote {OUTPUT_PATH} with {len(data)} rows.")
            return
        except Exception as e:
            print(f"Model {model_name} failed: {e}", file=sys.stderr)
            last_err = e
            continue

    raise RuntimeError(f"All models failed. Last error: {last_err}")

if __name__ == "__main__":
    main()
