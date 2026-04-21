#!/usr/bin/env python3
"""
Cruise schedule scraper for St. Thomas, USVI.
Source: https://www.vinow.com/cruise/ship-schedule/
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

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
SOURCE_URL = os.environ.get(
    "SOURCE_URL", "https://www.vinow.com/cruise/ship-schedule"
).rstrip("/")
MONTHS_PAST = int(os.environ.get("MONTHS_PAST", "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))
REQUEST_RETRIES = int(os.environ.get("REQUEST_RETRIES", "3"))
REQUEST_RETRY_DELAY = float(os.environ.get("REQUEST_RETRY_DELAY_SECONDS", "1.5"))
USE_ENV_PROXY = os.environ.get("USE_ENV_PROXY", "false").lower() == "true"

OUTPUT_PATH = Path(__file__).parent.parent / "schedule.json"
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
    "CB": ["crown bay", "crown bay terminal", "crowne bay"],
    "WICO": ["havensight", "wico", "west india company", "west india co dock"],
    "Harbor": ["harbor", "harbour", "charlotte amalie harbor", "charlotte amalie"],
}


def normalize_dock(raw: str) -> str:
    if not raw:
        return "Unknown"
    lower = raw.strip().lower()
    for normalized, keywords in DOCK_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                return normalized
    return "Unknown"


# ---------------------------------------------------------------------------
# Month arithmetic (no dateutil)
# ---------------------------------------------------------------------------
def add_months(dt: datetime, n: int) -> datetime:
    total_months = dt.month - 1 + n
    year = dt.year + total_months // 12
    month = total_months % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def months_to_scrape() -> list[datetime]:
    now = datetime.now(timezone.utc)
    start = add_months(now, -MONTHS_PAST)
    total = MONTHS_PAST + MONTHS_FUTURE + 1
    return [add_months(start, i) for i in range(total)]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def fetch_html(url: str) -> str | None:
    proxies = None
    if USE_ENV_PROXY:
        proxies = {
            "http": os.environ.get("HTTP_PROXY"),
            "https": os.environ.get("HTTPS_PROXY"),
        }
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=25, proxies=proxies, allow_redirects=True)
            log.info("  GET %s → HTTP %d", url, resp.status_code)
            if resp.status_code == 200:
                return resp.text
            log.warning("  Non-200 on attempt %d: %d", attempt, resp.status_code)
        except requests.RequestException as exc:
            log.warning("  Request error attempt %d: %s", attempt, exc)
        if attempt < REQUEST_RETRIES:
            time.sleep(REQUEST_RETRY_DELAY)
    return None


# ---------------------------------------------------------------------------
# URL construction — ViNow supports /YYYY/MM/ paths
# ---------------------------------------------------------------------------
def build_month_url(base: str, dt: datetime) -> str:
    return f"{base}/{dt.year}/{dt.month:02d}/"


# ---------------------------------------------------------------------------
# Time normalization
# ---------------------------------------------------------------------------
def normalize_time(raw: str) -> str:
    raw = raw.strip()
    m = re.match(r"(\d{1,2}):(\d{2})\s*(am|pm)?", raw, re.IGNORECASE)
    if not m:
        return raw
    h, mn = int(m.group(1)), m.group(2)
    meridiem = (m.group(3) or "").lower()
    if meridiem == "pm" and h != 12:
        h += 12
    elif meridiem == "am" and h == 12:
        h = 0
    return f"{h:02d}:{mn}"


def extract_passengers(text: str) -> int:
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return 0
    val = int(digits)
    return val if 50 < val < 25000 else 0


# ---------------------------------------------------------------------------
# Page parser
# ---------------------------------------------------------------------------
MONTH_NAMES = (
    "january february march april may june "
    "july august september october november december"
).split()

DATE_PATTERN = re.compile(
    r"\b(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{1,2}),?\s*(\d{4})?\b",
    re.IGNORECASE,
)


def parse_date_text(text: str, fallback_year: int) -> str | None:
    m = DATE_PATTERN.search(text)
    if not m:
        return None
    month_name = m.group(1).capitalize()
    day = int(m.group(2))
    year = int(m.group(3)) if m.group(3) else fallback_year
    try:
        return datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y").strftime(
            "%Y-%m-%d"
        )
    except ValueError:
        return None


def is_dock_text(text: str) -> bool:
    lower = text.lower()
    return any(
        kw in lower
        for kw in ["crown bay", "havensight", "wico", "harbor", "harbour", "dock", "pier", "terminal"]
    )


def is_time_text(text: str) -> bool:
    return bool(re.match(r"^\d{1,2}:\d{2}", text.strip()))


def is_passenger_count(text: str) -> bool:
    cleaned = re.sub(r"[,\s]", "", text)
    return bool(re.match(r"^\d{3,5}$", cleaned))


def parse_ship_row(cells: list[str]) -> dict | None:
    """
    Given a list of cell text values from a table row,
    try to extract a ship record. Returns None if the row
    does not look like ship data.
    """
    if not cells or not cells[0]:
        return None

    name = cells[0].strip()

    # Skip header rows and obviously non-ship rows
    skip = {
        "ship", "vessel", "cruise ship", "line", "cruise line",
        "dock", "pier", "passengers", "pax", "arrival", "departure",
        "date", "", "n/a", "none",
    }
    if name.lower() in skip:
        return None

    # Name must look like a real ship name (has letters, not just digits)
    if not re.search(r"[a-zA-Z]{3,}", name):
        return None

    ship = {
        "name": name,
        "line": "",
        "passengers": 0,
        "dock": "Unknown",
        "rawDock": "",
        "arrival": "",
        "departure": "",
    }

    for cell in cells[1:]:
        cell = cell.strip()
        if not cell:
            continue

        if is_time_text(cell):
            t = normalize_time(cell)
            if not ship["arrival"]:
                ship["arrival"] = t
            elif not ship["departure"]:
                ship["departure"] = t

        elif is_passenger_count(cell):
            pax = extract_passengers(cell)
            if pax:
                ship["passengers"] = pax

        elif is_dock_text(cell):
            ship["rawDock"] = cell
            ship["dock"] = normalize_dock(cell)

        elif not ship["line"] and re.search(r"[a-zA-Z]{4,}", cell):
            # Second text column is likely the cruise line
            ship["line"] = cell

    return ship


def parse_month_page(html: str, year: int, month: int) -> list[dict]:
    """
    Parse one month's HTML from ViNow.
    Returns a list of day dicts: [{date, ships: [...]}, ...]
    """
    soup = BeautifulSoup(html, "lxml")
    days: dict[str, list[dict]] = {}
    current_date: str | None = None

    # --- Primary strategy: walk all table rows ---
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            cell_texts = [c.get_text(" ", strip=True) for c in cells]

            # Skip completely empty rows
            if not any(cell_texts):
                continue

            full_text = " ".join(cell_texts)

            # Is this a date header row?
            candidate_date = parse_date_text(full_text, year)
            if candidate_date:
                # Only accept dates in the target month/year
                parsed = datetime.strptime(candidate_date, "%Y-%m-%d")
                if parsed.year == year and parsed.month == month:
                    current_date = candidate_date
                continue

            if current_date is None:
                continue

            # Is this a ship row?
            ship = parse_ship_row(cell_texts)
            if ship:
                days.setdefault(current_date, []).append(ship)

    # --- Fallback strategy: headings + lists ---
    if not days:
        current_date = None
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "div"]):
            text = tag.get_text(" ", strip=True)
            if not text:
                continue

            candidate_date = parse_date_text(text, year)
            if candidate_date:
                parsed = datetime.strptime(candidate_date, "%Y-%m-%d")
                if parsed.year == year and parsed.month == month:
                    current_date = candidate_date
                continue

            if current_date is None:
                continue

            # Look for ship-like lines within this element
            for line in text.split("\n"):
                parts = [p.strip() for p in re.split(r"\s{2,}|\t+|,", line) if p.strip()]
                if len(parts) >= 2:
                    ship = parse_ship_row(parts)
                    if ship:
                        days.setdefault(current_date, []).append(ship)

    result = []
    for d in sorted(days.keys()):
        if days[d]:
            result.append({"date": d, "ships": days[d]})

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    now = datetime.now(timezone.utc)
    months = months_to_scrape()

    log.info(
        "Scraping %d months: %s → %s",
        len(months),
        months[0].strftime("%Y-%m"),
        months[-1].strftime("%Y-%m"),
    )

    all_days: list[dict] = []

    for month_dt in months:
        url = build_month_url(SOURCE_URL, month_dt)
        log.info("Fetching month %s", month_dt.strftime("%Y-%m"))
        html = fetch_html(url)

        if html is None:
            log.warning("Skipping %s — all fetch attempts failed", url)
            continue

        days = parse_month_page(html, month_dt.year, month_dt.month)
        ship_count = sum(len(d["ships"]) for d in days)
        log.info(
            "  Parsed %d days, %d ship calls for %s",
            len(days),
            ship_count,
            month_dt.strftime("%Y-%m"),
        )
        all_days.extend(days)

        # Polite crawl delay
        time.sleep(0.75)

    # Deduplicate by date (merge ships if same date appears from overlapping months)
    merged: dict[str, dict] = {}
    for day in all_days:
        date_key = day["date"]
        if date_key not in merged:
            merged[date_key] = {"date": date_key, "ships": []}
        # Avoid duplicate ships on the same date
        existing_names = {s["name"] for s in merged[date_key]["ships"]}
        for ship in day["ships"]:
            if ship["name"] not in existing_names:
                merged[date_key]["ships"].append(ship)
                existing_names.add(ship["name"])

    sorted_days = [merged[k] for k in sorted(merged)]

    # Build counts
    total_ships = sum(len(d["ships"]) for d in sorted_days)
    total_pax = sum(s["passengers"] for d in sorted_days for s in d["ships"])
    unknown_dock_ships = sorted(
        {
            s["name"]
            for d in sorted_days
            for s in d["ships"]
            if s["dock"] == "Unknown"
        }
    )

    output = {
        "schemaVersion": SCHEMA_VERSION,
        "lastUpdated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": ["vinow"],
        "counts": {
            "totalDays": len(sorted_days),
            "totalShipCalls": total_ships,
            "totalPassengers": total_pax,
            "shipsWithUnknownDock": unknown_dock_ships,
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
            "No data was scraped. Possible causes:\n"
            "  1. ViNow changed their HTML structure\n"
            "  2. The URL pattern /<year>/<month>/ is wrong for this site\n"
            "  3. Network issue in the runner\n"
            "Check the HTML by running locally and inspecting the soup output."
        )


if __name__ == "__main__":
    main()
