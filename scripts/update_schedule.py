#!/usr/bin/env python3
"""
Cruise schedule fetcher for St. Thomas, USVI.

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
from bs4 import BeautifulSoup, NavigableString

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SOURCE_URL = os.environ.get("SOURCE_URL", "https://www.vinow.com/cruise/ship-schedule").rstrip("/")
MONTHS_PAST = int(os.environ.get("MONTHS_PAST", "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))
REQUEST_RETRIES = int(os.environ.get("REQUEST_RETRIES", "4"))
REQUEST_BACKOFF_BASE_SECONDS = float(os.environ.get("REQUEST_BACKOFF_BASE_SECONDS", "1.5"))
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "60"))

OUTPUT_PATH = Path(__file__).parent.parent / "schedule.json"
SCHEMA_VERSION = "1.1.0"
R_JINA_PREFIX = "https://r.jina.ai/http://"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.vinow.com/cruise/ship-schedule/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

DAY_HEADING_RE = re.compile(
    r"^\s*(?P<month>[A-Za-z]{3,}\.?)\s+(?P<day>\d+)(?:st|nd|rd|th)\b",
    re.IGNORECASE,
)
SHIP_RE = re.compile(
    r"^(?P<ship>.+?)\s+(?P<passengers>\d[\d,]*)\s+Guests\b(?:\s*\((?P<line>.+?)\))?",
    re.IGNORECASE,
)
TIME_RE = re.compile(
    r"^\s*(?P<dock>.*?)\s*\((?P<arr>[^-]+?)\s*-\s*(?P<dep>.+?)\)\s*$",
    re.IGNORECASE,
)
MONTH_MAP = {
    "jan": 1,
    "jan.": 1,
    "feb": 2,
    "feb.": 2,
    "mar": 3,
    "mar.": 3,
    "apr": 4,
    "apr.": 4,
    "may": 5,
    "jun": 6,
    "jun.": 6,
    "jul": 7,
    "jul.": 7,
    "aug": 8,
    "aug.": 8,
    "sep": 9,
    "sep.": 9,
    "sept": 9,
    "sept.": 9,
    "oct": 10,
    "oct.": 10,
    "nov": 11,
    "nov.": 11,
    "dec": 12,
    "dec.": 12,
}


# ---------------------------------------------------------------------------
# Month arithmetic (no dateutil)
# ---------------------------------------------------------------------------

def add_months(dt: datetime, n: int) -> datetime:
    total = dt.month - 1 + n
    year = dt.year + total // 12
    month = total % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def months_to_scrape() -> list[datetime]:
    now = datetime.now(timezone.utc)
    start = add_months(now, -MONTHS_PAST)
    total = MONTHS_PAST + MONTHS_FUTURE + 1
    return [add_months(start, i) for i in range(total)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def month_to_number(token: str) -> int | None:
    return MONTH_MAP.get(token.strip().lower())


def to_24h(time_str: str) -> str:
    return datetime.strptime(time_str.strip(), "%I:%M %p").strftime("%H:%M")


DOCK_KEYWORDS: dict[str, list[str]] = {
    "CB": ["crown bay", "crowne bay"],
    "WICO": ["havensight", "wico", "west india company", "west india co"],
    "Harbor": ["harbor", "harbour", "charlotte amalie"],
}


def normalize_dock(raw: str) -> str:
    if not raw:
        return "Unknown"
    lower = raw.lower()
    for code, keywords in DOCK_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                return code
    return "Unknown"


def is_cloudflare_page(html: str) -> bool:
    lower = html.lower()
    return (
        "just a moment..." in lower
        or "cf-chl-" in lower
        or "challenges.cloudflare.com" in lower
        or "enable javascript and cookies to continue" in lower
    )


def has_expected_structure(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return bool(
        soup.find("h3")
        and re.search(r"\bguests\b", soup.get_text(" ", strip=True), re.IGNORECASE)
    )


def build_month_url(year: int, month: int) -> str:
    return f"{SOURCE_URL}/{month}-{year}/"


def build_proxy_url(url: str) -> str:
    stripped = url.removeprefix("https://").removeprefix("http://")
    return f"{R_JINA_PREFIX}{stripped}"


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)
    return session


# ---------------------------------------------------------------------------
# Fetch one month
# ---------------------------------------------------------------------------

def fetch_html(session: requests.Session, url: str) -> str:
    errors: list[str] = []
    proxy_url = build_proxy_url(url)

    for attempt in range(1, REQUEST_RETRIES + 1):
        delay_seconds = REQUEST_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            html = response.text

            if is_cloudflare_page(html) or not has_expected_structure(html):
                raise ValueError("direct request returned Cloudflare or unexpected HTML")

            return html
        except Exception as exc:
            errors.append(f"direct attempt {attempt}: {exc}")
            log.warning("Direct fetch failed for %s on attempt %d: %s", url, attempt, exc)

        try:
            proxy_response = session.get(proxy_url, timeout=REQUEST_TIMEOUT_SECONDS)
            proxy_response.raise_for_status()
            proxy_html = proxy_response.text

            if is_cloudflare_page(proxy_html) or not has_expected_structure(proxy_html):
                raise ValueError("proxy request returned Cloudflare or unexpected HTML")

            log.info("Using r.jina.ai fallback for %s", url)
            return proxy_html
        except Exception as exc:
            errors.append(f"proxy attempt {attempt}: {exc}")
            log.warning("Proxy fetch failed for %s on attempt %d: %s", url, attempt, exc)

        if attempt < REQUEST_RETRIES:
            time.sleep(delay_seconds)

    raise RuntimeError(
        f"Unable to fetch usable HTML for {url} after {REQUEST_RETRIES} attempts. "
        f"Last errors: {' | '.join(errors[-2:])}"
    )


def scrape_month(session: requests.Session, month_dt: datetime) -> list[dict]:
    year = month_dt.year
    month = month_dt.month
    month_label = month_dt.strftime("%Y-%m")
    url = build_month_url(year, month)

    log.info("Fetching %s from %s", month_label, url)
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []

    for h3 in soup.find_all("h3"):
        heading = h3.get_text(separator=" ", strip=True)
        match = DAY_HEADING_RE.match(heading)
        if not match:
            continue

        heading_month = month_to_number(match.group("month"))
        if heading_month and heading_month != month:
            continue

        day = int(match.group("day"))
        date_str = f"{year:04d}-{month:02d}-{day:02d}"

        block_lines: list[str] = []
        for sib in h3.next_siblings:
            if isinstance(sib, NavigableString):
                continue
            if getattr(sib, "name", None) in {"h2", "h3"}:
                break
            text = sib.get_text("\n", strip=True)
            if text:
                block_lines.extend([ln.strip() for ln in re.split(r"[\r\n]+", text) if ln.strip()])

        i = 0
        while i < len(block_lines):
            ship_line = block_lines[i]
            ship_match = SHIP_RE.match(ship_line)
            if not ship_match:
                i += 1
                continue

            ship = ship_match.group("ship").strip()
            passengers = int(ship_match.group("passengers").replace(",", "")) if ship_match.group("passengers") else 0
            cruise_line = ship_match.group("line").strip() if ship_match.group("line") else ""

            j = i + 1
            found_time = False
            while j < len(block_lines):
                time_match = TIME_RE.match(block_lines[j])
                if time_match:
                    raw_dock = time_match.group("dock").strip()
                    items.append(
                        {
                            "date": date_str,
                            "name": ship,
                            "line": cruise_line,
                            "passengers": passengers,
                            "rawDock": raw_dock,
                            "arrival": to_24h(time_match.group("arr")),
                            "departure": to_24h(time_match.group("dep")),
                        }
                    )
                    found_time = True
                    break
                j += 1

            i = j + 1 if found_time else i + 1

    log.info("Found %d ships for %s", len(items), month_label)
    return items


# ---------------------------------------------------------------------------
# Group flat ship list into day objects
# ---------------------------------------------------------------------------

def group_by_date(ships: list[dict]) -> list[dict]:
    days: dict[str, list[dict]] = {}

    for ship in ships:
        date = (ship.get("date") or "").strip()
        if not date:
            continue

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
            "name": name,
            "line": (ship.get("line") or "").strip(),
            "passengers": int(ship.get("passengers") or 0),
            "dock": normalize_dock(raw_dock),
            "rawDock": raw_dock,
            "arrival": (ship.get("arrival") or "").strip(),
            "departure": (ship.get("departure") or "").strip(),
        }
        days.setdefault(date, []).append(record)

    return [{"date": d, "ships": days[d]} for d in sorted(days)]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    now = datetime.now(timezone.utc)
    months = months_to_scrape()

    log.info(
        "Fetching %d months from ViNow (%s → %s)",
        len(months),
        months[0].strftime("%Y-%m"),
        months[-1].strftime("%Y-%m"),
    )

    session = build_session()
    all_ships: list[dict] = []

    for month_dt in months:
        ships = scrape_month(session, month_dt)
        all_ships.extend(ships)
        time.sleep(2)

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
    total_pax = sum(s["passengers"] for d in sorted_days for s in d["ships"])
    unknown_docks = sorted({
        s["name"]
        for d in sorted_days
        for s in d["ships"]
        if s["dock"] == "Unknown"
    })

    output = {
        "schemaVersion": SCHEMA_VERSION,
        "lastUpdated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": ["vinow"],
        "counts": {
            "totalDays": len(sorted_days),
            "totalShipCalls": total_ships,
            "totalPassengers": total_pax,
            "shipsWithUnknownDock": unknown_docks,
        },
        "days": sorted_days,
    }

    if len(sorted_days) == 0:
        log.warning(
            "\nNo data returned by scraper. Preserving existing schedule.json.\n"
            "The existing file will not be overwritten with empty data.\n"
        )
        raise SystemExit(1)

    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")

    log.info(
        "Done. Wrote %s — %d days, %d ship calls, %d passengers",
        OUTPUT_PATH,
        len(sorted_days),
        total_ships,
        total_pax,
    )


if __name__ == "__main__":
    main()
