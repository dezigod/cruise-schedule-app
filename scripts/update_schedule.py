#!/usr/bin/env python3
"""
Cruise schedule fetcher for St. Thomas, USVI.

Strategy: Use Gemini 2.0 Flash with Google Search grounding.
Gemini searches for each month's schedule itself — no direct HTTP
requests to ViNow, so GitHub Actions IP blocks are irrelevant.

Output: schedule.json in repo root.
"""

import calendar
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# ⚠️ Change "GEMINI_API_KEY" below if your GitHub secret has a different name.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# ⚠️ Change this if you want a different model.
GEMINI_MODEL = "gemini-2.5-flash"

MONTHS_PAST   = int(os.environ.get("MONTHS_PAST",   "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))

OUTPUT_PATH    = Path(__file__).parent.parent / "schedule.json"
SCHEMA_VERSION = "1.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

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
# Dock normalization
# ---------------------------------------------------------------------------

DOCK_KEYWORDS: dict[str, list[str]] = {
    "CB":     ["crown bay", "crowne bay"],
    "WICO":   ["havensight", "wico", "west india company", "west india co"],
    "Harbor": ["harbor", "harbour", "charlotte amalie"],
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
# Gemini prompt
# ---------------------------------------------------------------------------

SEARCH_PROMPT = """\
Search for the complete cruise ship arrival schedule for the port of \
St. Thomas, US Virgin Islands for {month_name} {year}.

Good sources include:
- vinow.com/cruise/ship-schedule
- The Virgin Islands Port Authority (vipa.vi)
- CruiseMapper, CruiseTimetables, or any reliable port schedule site

Return ONLY a raw JSON array. No explanation. No markdown. No code fences. \
Just the JSON array starting with [ and ending with ].

Each element must be an object with exactly these keys:
  "date"       — "YYYY-MM-DD" format, e.g. "2026-04-21"
  "name"       — the ship name as a string
  "line"       — the cruise line name, or "" if not found
  "passengers" — integer passenger count, or 0 if not found
  "rawDock"    — the dock name exactly as the source lists it, or ""
  "arrival"    — arrival time as "HH:MM" 24-hour, e.g. "07:00", or ""
  "departure"  — departure time as "HH:MM" 24-hour, e.g. "17:00", or ""

If no ships are listed for this month, return an empty array: []
Do not invent data. Only include ships you found from real sources.
"""

# ---------------------------------------------------------------------------
# Fetch one month via Gemini Search
# ---------------------------------------------------------------------------

def fetch_month(client: genai.Client, month_dt: datetime) -> list[dict]:
    month_name  = month_dt.strftime("%B")
    year        = month_dt.year
    month_label = month_dt.strftime("%Y-%m")

    prompt = SEARCH_PROMPT.format(month_name=month_name, year=year)
    log.info("  Searching for %s schedule via Gemini...", month_label)

    for attempt in range(1, 4):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.1,
                ),
            )

            raw = response.text.strip() if response.text else ""

            # Strip markdown fences if Gemini added them anyway
            raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
            raw = re.sub(r"\s*```$",           "", raw, flags=re.MULTILINE)
            raw = raw.strip()

            # Pull out just the JSON array if surrounded by text
            match = re.search(r"\[.*\]", raw, re.DOTALL)
            if match:
                raw = match.group(0)

            ships = json.loads(raw)

            if not isinstance(ships, list):
                raise ValueError(f"Expected JSON array, got {type(ships).__name__}")

            log.info("  Found %d ships for %s", len(ships), month_label)
            return ships

        except json.JSONDecodeError as exc:
            log.warning("  JSON parse error attempt %d: %s", attempt, exc)
            log.debug("  Raw response was: %.300s", raw if "raw" in dir() else "(none)")

        except Exception as exc:
            log.warning("  Gemini error attempt %d: %s", attempt, exc)

        if attempt < 3:
            time.sleep(4)

    log.error("  All %d attempts failed for %s", 3, month_label)
    return []

# ---------------------------------------------------------------------------
# Group flat ship list into day objects
# ---------------------------------------------------------------------------

def group_by_date(ships: list[dict]) -> list[dict]:
    days: dict[str, list[dict]] = {}

    for ship in ships:
        date = (ship.get("date") or "").strip()
        if not date:
            continue

        # Validate date format
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            log.warning("  Skipping invalid date %r", date)
            continue

        name = (ship.get("name") or "").strip()
        if not name:
            continue

        raw_dock = (ship.get("rawDock") or "").strip()
        record = {
            "name":       name,
            "line":       (ship.get("line")      or "").strip(),
            "passengers": int(ship.get("passengers") or 0),
            "dock":       normalize_dock(raw_dock),
            "rawDock":    raw_dock,
            "arrival":    (ship.get("arrival")   or "").strip(),
            "departure":  (ship.get("departure") or "").strip(),
        }
        days.setdefault(date, []).append(record)

    return [{"date": d, "ships": days[d]} for d in sorted(days)]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not GEMINI_API_KEY:
        log.error(
            "GEMINI_API_KEY environment variable is not set.\n"
            "  → Go to your repo Settings → Secrets → Actions\n"
            "  → Confirm a secret named GEMINI_API_KEY exists\n"
            "  → Confirm the workflow passes it: "
            "GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}"
        )
        raise SystemExit(1)

    now    = datetime.now(timezone.utc)
    months = months_to_scrape()

    log.info(
        "Fetching %d months via Gemini Search (%s → %s)",
        len(months),
        months[0].strftime("%Y-%m"),
        months[-1].strftime("%Y-%m"),
    )

    client    = genai.Client(api_key=GEMINI_API_KEY)
    all_ships: list[dict] = []

    for month_dt in months:
        ships = fetch_month(client, month_dt)
        all_ships.extend(ships)
        time.sleep(2)  # Stay well within rate limits

    # Group + deduplicate
    merged: dict[str, dict] = {}
    for day in group_by_date(all_ships):
        key = day["date"]
        if key not in merged:
            merged[key] = {"date": key, "ships": []}
        seen_names = {s["name"] for s in merged[key]["ships"]}
        for ship in day["ships"]:
            if ship["name"] not in seen_names:
                merged[key]["ships"].append(ship)
                seen_names.add(ship["name"])

    sorted_days = [merged[k] for k in sorted(merged)]

    total_ships = sum(len(d["ships"]) for d in sorted_days)
    total_pax   = sum(s["passengers"] for d in sorted_days for s in d["ships"])
    unknown_docks = sorted({
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
            "totalDays":            len(sorted_days),
            "totalShipCalls":       total_ships,
            "totalPassengers":      total_pax,
            "shipsWithUnknownDock": unknown_docks,
        },
        "days": sorted_days,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")

    log.info(
        "Done. Wrote %s — %d days, %d ship calls, %d passengers",
        OUTPUT_PATH,
        len(sorted_days),
        total_ships,
        total_pax,
    )

    if len(sorted_days) == 0:
        log.warning(
            "\nNo data returned by Gemini Search. Likely causes:\n"
            "  1. Google Search grounding is not enabled for your Gemini API key\n"
            "     → Check console.cloud.google.com or aistudio.google.com\n"
            "  2. The API key is invalid or has no quota remaining\n"
            "  3. Rate limiting — try reducing MONTHS_PAST/MONTHS_FUTURE in the workflow\n"
        )


if __name__ == "__main__":
    main()
