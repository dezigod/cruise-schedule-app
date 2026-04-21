#!/usr/bin/env python3
"""
Cruise schedule scraper for St. Thomas, USVI.
Source: https://www.vinow.com/cruise/ship-schedule/
Uses Gemini API for reliable structured extraction.
Output: schedule.json in repo root.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
import calendar
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import google.generativeai as genai

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SOURCE_URL = os.environ.get(
    "SOURCE_URL", "https://www.vinow.com/cruise/ship-schedule"
).rstrip("/")
MONTHS_PAST   = int(os.environ.get("MONTHS_PAST", "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))
REQUEST_RETRIES     = int(os.environ.get("REQUEST_RETRIES", "3"))
REQUEST_RETRY_DELAY = float(os.environ.get("REQUEST_RETRY_DELAY_SECONDS", "1.5"))
USE_ENV_PROXY = os.environ.get("USE_ENV_PROXY", "false").lower() == "true"

# ⚠️  If your GitHub secret has a different name, change "GEMINI_API_KEY" below.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# ⚠️  Change this if you want a different model (e.g. "gemini-1.5-pro").
GEMINI_MODEL = "gemini-2.0-flash"

OUTPUT_PATH    = Path(__file__).parent.parent / "schedule.json"
SCHEMA_VERSION = "1.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dock normalization
# ---------------------------------------------------------------------------
DOCK_KEYWORDS = {
    "CB":      ["crown bay", "crowne bay"],
    "WICO":    ["havensight", "wico", "west india company", "west india co"],
    "Harbor":  ["harbor", "harbour", "charlotte amalie"],
}

def normalize_dock(raw: str) -> str:
    if not raw:
        return "Unknown"
    lower = raw.strip().lower()
    for code, keywords in DOCK_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                return code
    return "Unknown"

# ---------------------------------------------------------------------------
# Month arithmetic (no dateutil)
# ---------------------------------------------------------------------------
def add_months(dt: datetime, n: int) -> datetime:
    total = dt.month - 1 + n
    year  = dt.year + total // 12
    month = total % 12 + 1
    day   = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)

def months_to_scrape() -> list[datetime]:
    now   = datetime.now(timezone.utc)
    start = add_months(now, -MONTHS_PAST)
    total = MONTHS_PAST + MONTHS_FUTURE + 1
    return [add_months(start, i) for i in range(total)]

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
})

def fetch_html(url: str) -> str | None:
    proxies = None
    if USE_ENV_PROXY:
        proxies = {
            "http":  os.environ.get("HTTP_PROXY"),
            "https": os.environ.get("HTTPS_PROXY"),
        }
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            r = SESSION.get(url, timeout=25, proxies=proxies, allow_redirects=True)
            log.info("  GET %s → HTTP %d", url, r.status_code)
            if r.status_code == 200:
                return r.text
            log.warning("  Non-200 on attempt %d: %d", attempt, r.status_code)
        except requests.RequestException as exc:
            log.warning("  Request error attempt %d: %s", attempt, exc)
        if attempt < REQUEST_RETRIES:
            time.sleep(REQUEST_RETRY_DELAY)
    return None

def build_month_url(dt: datetime) -> str:
    return f"{SOURCE_URL}/{dt.year}/{dt.month:02d}/"

# ---------------------------------------------------------------------------
# Extract readable text from HTML (what we send to Gemini)
# ---------------------------------------------------------------------------
def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # Remove noise
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "noscript", "iframe", "svg", "img"]):
        tag.decompose()

    # Preserve table structure as plain text
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            rows.append(" | ".join(cells))
        table.replace_with("\n".join(rows) + "\n")

    text = soup.get_text("\n", strip=True)

    # Collapse blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

# ---------------------------------------------------------------------------
# Gemini extraction
# ---------------------------------------------------------------------------
EXTRACTION_PROMPT = """\
You are a data extraction assistant. Below is the text content of a cruise ship schedule
page for St. Thomas, US Virgin Islands (from vinow.com).

Extract every cruise ship arrival listed and return ONLY a JSON array.
Do NOT include any explanation, markdown, or code fences — only the raw JSON array.

Each element in the array must be an object with these exact keys:
  "date"       — string, format YYYY-MM-DD (e.g. "2026-04-21")
  "name"       — string, the ship's name (e.g. "Norwegian Escape")
  "line"       — string, the cruise line name (e.g. "Norwegian Cruise Line"), or "" if unknown
  "passengers" — integer, the passenger count, or 0 if not listed
  "rawDock"    — string, the dock name exactly as it appears on the page, or "" if not listed
  "arrival"    — string, arrival time in HH:MM 24-hour format (e.g. "07:00"), or "" if not listed
  "departure"  — string, departure time in HH:MM 24-hour format (e.g. "17:00"), or "" if not listed

Rules:
- If the page lists no ships, return an empty array: []
- Do not invent data. If a field is missing, use "" or 0.
- Convert all times to 24-hour HH:MM format.
- Include every ship entry you can find, even if some fields are missing.
- The date must always be in YYYY-MM-DD format.

PAGE CONTENT:
{page_text}
"""

def extract_with_gemini(page_text: str, month_label: str) -> list[dict]:
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY is not set. Cannot use AI extraction.")
        return []

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL)

    # Trim to avoid token limits — ViNow pages are not huge, but be safe
    max_chars = 60_000
    if len(page_text) > max_chars:
        page_text = page_text[:max_chars]
        log.warning("  Page text trimmed to %d chars for Gemini", max_chars)

    prompt = EXTRACTION_PROMPT.format(page_text=page_text)

    log.info("  Sending %s to Gemini (%s)...", month_label, GEMINI_MODEL)

    for attempt in range(1, 4):
        try:
            response = model.generate_content(prompt)
            raw = response.text.strip()

            # Strip markdown fences if Gemini added them despite instructions
            raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
            raw = re.sub(r"\s*```$",          "", raw, flags=re.MULTILINE)
            raw = raw.strip()

            ships = json.loads(raw)
            if not isinstance(ships, list):
                raise ValueError(f"Expected list, got {type(ships)}")

            log.info("  Gemini returned %d ship records for %s", len(ships), month_label)
            return ships

        except json.JSONDecodeError as exc:
            log.warning("  JSON parse error attempt %d: %s", attempt, exc)
            log.debug("  Raw response: %s", raw[:500])
        except Exception as exc:
            log.warning("  Gemini error attempt %d: %s", attempt, exc)

        time.sleep(2)

    log.error("  All Gemini attempts failed for %s", month_label)
    return []

# ---------------------------------------------------------------------------
# Build day structure from flat ship list
# ---------------------------------------------------------------------------
def group_by_date(ships: list[dict]) -> list[dict]:
    days: dict[str, list[dict]] = {}

    for ship in ships:
        date = ship.get("date", "").strip()
        if not date:
            continue

        # Validate date format
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            log.warning("  Skipping invalid date: %s", date)
            continue

        name = ship.get("name", "").strip()
        if not name:
            continue

        raw_dock = ship.get("rawDock", "") or ""
        record = {
            "name":       name,
            "line":       ship.get("line", "") or "",
            "passengers": int(ship.get("passengers", 0) or 0),
            "dock":       normalize_dock(raw_dock),
            "rawDock":    raw_dock,
            "arrival":    ship.get("arrival", "") or "",
            "departure":  ship.get("departure", "") or "",
        }

        days.setdefault(date, []).append(record)

    return [{"date": d, "ships": days[d]} for d in sorted(days)]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    now    = datetime.now(timezone.utc)
    months = months_to_scrape()

    log.info(
        "Scraping %d months: %s → %s",
        len(months),
        months[0].strftime("%Y-%m"),
        months[-1].strftime("%Y-%m"),
    )

    if not GEMINI_API_KEY:
        log.error(
            "GEMINI_API_KEY environment variable is not set.\n"
            "Add it to your GitHub repository secrets and reference it\n"
            "in the workflow env block as:\n"
            "  GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}"
        )

    all_ships: list[dict] = []

    for month_dt in months:
        url        = build_month_url(month_dt)
        month_label = month_dt.strftime("%Y-%m")

        log.info("Fetching %s", url)
        html = fetch_html(url)

        if html is None:
            log.warning("  Skipping %s — fetch failed after %d attempts", url, REQUEST_RETRIES)
            continue

        page_text = html_to_text(html)
        log.info("  Extracted %d chars of readable text", len(page_text))

        if len(page_text) < 100:
            log.warning("  Very short page text for %s — page may be JS-rendered or empty", month_label)

        ships = extract_with_gemini(page_text, month_label)
        all_ships.extend(ships)

        # Polite delay between months
        time.sleep(1.0)

    # Group into days, deduplicate
    merged: dict[str, dict] = {}
    for day in group_by_date(all_ships):
        date_key = day["date"]
        if date_key not in merged:
            merged[date_key] = {"date": date_key, "ships": []}
        existing = {s["name"] for s in merged[date_key]["ships"]}
        for ship in day["ships"]:
            if ship["name"] not in existing:
                merged[date_key]["ships"].append(ship)
                existing.add(ship["name"])

    sorted_days = [merged[k] for k in sorted(merged)]

    # Counts
    total_ships = sum(len(d["ships"]) for d in sorted_days)
    total_pax   = sum(s["passengers"] for d in sorted_days for s in d["ships"])
    unknown_dock = sorted({
        s["name"]
        for d in sorted_days
        for s in d["ships"]
        if s["dock"] == "Unknown"
    })

    output = {
        "schemaVersion": SCHEMA_VERSION,
        "lastUpdated":   now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources":       ["vinow"],
        "counts": {
            "totalDays":             len(sorted_days),
            "totalShipCalls":        total_ships,
            "totalPassengers":       total_pax,
            "shipsWithUnknownDock":  unknown_dock,
        },
        "days": sorted_days,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")

    log.info(
        "Wrote %s — %d days, %d ship calls, %d passengers",
        OUTPUT_PATH,
        len(sorted_days),
        total_ships,
        total_pax,
    )

    if len(sorted_days) == 0:
        log.warning(
            "\nNo data was scraped. Check:\n"
            "  1. GEMINI_API_KEY is correctly set in GitHub Secrets\n"
            "  2. The secret name in the workflow matches the secret name in Settings\n"
            "  3. ViNow is returning HTML (check the GET lines above for HTTP 200)\n"
            "  4. The workflow log for Gemini error messages above\n"
        )

if __name__ == "__main__":
    main()
