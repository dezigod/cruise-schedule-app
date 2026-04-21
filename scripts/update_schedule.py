import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup, NavigableString

OUTPUT_PATH = Path("schedule.json")
SOURCE_URL = os.environ.get("SOURCE_URL", "https://www.vinow.com/cruise/ship-schedule").rstrip("/")

MONTHS_PAST = int(os.environ.get("MONTHS_PAST", "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))
REQUEST_RETRIES = int(os.environ.get("REQUEST_RETRIES", "3"))
REQUEST_RETRY_DELAY_SECONDS = float(os.environ.get("REQUEST_RETRY_DELAY_SECONDS", "1.5"))
USE_ENV_PROXY = os.environ.get("USE_ENV_PROXY", "false").strip().lower() in {"1", "true", "yes"}
R_JINA_PREFIX = "https://r.jina.ai/http://"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

DAY_HEADING_RE = re.compile(
    r"^\s*(?P<month>[A-Za-z]{3,}\.?)+\s+(?P<day>\d+)(?:st|nd|rd|th)\b",
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


def log(message: str) -> None:
    print(f"[scraper] {message}")


def month_to_number(token: str) -> int | None:
    return MONTH_MAP.get(token.strip().lower())


def to_24h(time_str: str) -> str:
    dt = datetime.strptime(time_str.strip(), "%I:%M %p")
    return dt.strftime("%H:%M")


def normalize_port_name(dock: str) -> str:
    d = dock.strip().lower()
    if "havensight" in d or "wico" in d:
        return "Havensight"
    if "crown" in d or d == "cb":
        return "Crown Bay"
    return dock.strip() or "Unknown"


def normalize_dock_code(dock: str) -> str:
    d = dock.strip().lower()
    if "havensight" in d or "wico" in d:
        return "WICO"
    if "crown" in d or d == "cb":
        return "CB"
    if "harbor" in d or "anchorage" in d:
        return "Harbor"
    return "Unknown"


def island_from_dock(dock: str) -> str:
    dl = dock.lower()
    if "cruz bay" in dl or "st john" in dl:
        return "St. John"
    if "st. croix" in dl or "st croix" in dl:
        return "St. Croix"
    return "St. Thomas"


def calculate_crowd_score(total_passengers: int) -> int:
    if total_passengers <= 2000:
        return 1
    if total_passengers <= 5000:
        return 3
    if total_passengers <= 8000:
        return 5
    if total_passengers <= 12000:
        return 7
    if total_passengers <= 16000:
        return 8
    return 10


def iter_months(start_year: int, start_month: int, count: int):
    year = start_year
    month = start_month
    for _ in range(count):
        yield year, month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1


def fetch_html(session: requests.Session, url: str) -> str:
    from time import sleep

    errors: list[str] = []
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = session.get(url, timeout=60)
            if response.status_code == 403:
                jurl = f"{R_JINA_PREFIX}{url.replace('https://', '').replace('http://', '')}"
                response = session.get(jurl, timeout=60)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            errors.append(str(exc))
            if not session.trust_env:
                try:
                    proxy_session = requests.Session()
                    proxy_session.trust_env = True
                    proxy_session.headers.update(BROWSER_HEADERS)
                    response = proxy_session.get(url, timeout=60)
                    if response.status_code == 403:
                        jurl = f"{R_JINA_PREFIX}{url.replace('https://', '').replace('http://', '')}"
                        response = proxy_session.get(jurl, timeout=60)
                    response.raise_for_status()
                    return response.text
                except requests.RequestException as proxy_exc:
                    errors.append(f"proxy-attempt: {proxy_exc}")
            if attempt < REQUEST_RETRIES:
                log(f"request error retry {attempt}/{REQUEST_RETRIES} for {url}: {exc}")
                sleep(REQUEST_RETRY_DELAY_SECONDS)

    raise requests.RequestException(
        f"Unable to fetch {url} after {REQUEST_RETRIES} attempts. Last error: {errors[-1] if errors else 'unknown'}"
    )


def scrape_month(session: requests.Session, year: int, month: int) -> list[dict[str, Any]]:
    url = f"{SOURCE_URL}/{month}-{year}/"
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")

    items: list[dict[str, Any]] = []

    for h3 in soup.find_all("h3"):
        heading = h3.get_text(separator=" ", strip=True)
        m = DAY_HEADING_RE.match(heading)
        if not m:
            continue

        heading_month = month_to_number(m.group("month"))
        if heading_month and heading_month != month:
            continue

        day = int(m.group("day"))
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
            m_ship = SHIP_RE.match(ship_line)
            if not m_ship:
                i += 1
                continue

            ship = m_ship.group("ship").strip()
            passengers = int(m_ship.group("passengers").replace(",", "")) if m_ship.group("passengers") else 0
            cruise_line = m_ship.group("line").strip() if m_ship.group("line") else ""

            j = i + 1
            found_time = False
            while j < len(block_lines):
                m_time = TIME_RE.match(block_lines[j])
                if m_time:
                    raw_dock = m_time.group("dock").strip()
                    arrival = to_24h(m_time.group("arr"))
                    departure = to_24h(m_time.group("dep"))
                    dock = normalize_dock_code(raw_dock)

                    items.append(
                        {
                            "date": date_str,
                            "ship": ship,
                            "line": cruise_line,
                            "arrival": arrival,
                            "departure": departure,
                            "passengers": passengers,
                            "island": island_from_dock(raw_dock),
                            "port": normalize_port_name(raw_dock),
                            "dock": dock,
                            "rawDock": raw_dock or "Unknown",
                        }
                    )
                    found_time = True
                    break
                j += 1

            i = j + 1 if found_time else i + 1

    return items


def build_schedule(items: list[dict[str, Any]]) -> dict[str, Any]:
    grouped_by_date: dict[str, dict[str, Any]] = {}
    unknown_docks: set[str] = set()

    for item in items:
        date = item["date"]
        if date not in grouped_by_date:
            grouped_by_date[date] = {
                "date": date,
                "totalShips": 0,
                "totalPassengers": 0,
                "ports": defaultdict(list),
            }

        if item["dock"] == "Unknown":
            unknown_docks.add(item["ship"])

        day = grouped_by_date[date]
        ship_obj = {
            "name": item["ship"],
            "line": item["line"],
            "arrival": item["arrival"],
            "departure": item["departure"],
            "passengers": item["passengers"],
            "island": item["island"],
            "dock": item["dock"],
            "rawDock": item["rawDock"],
        }

        day["ports"][item["port"]].append(ship_obj)
        day["totalShips"] += 1
        day["totalPassengers"] += item["passengers"]

    days: list[dict[str, Any]] = []
    total_calls = 0
    total_pax = 0

    for date in sorted(grouped_by_date.keys()):
        day = grouped_by_date[date]
        ports = []
        for port_name, ships in sorted(day["ports"].items()):
            ships.sort(key=lambda s: (s["arrival"], s["name"]))
            ports.append({"name": port_name, "ships": ships})

        total_calls += day["totalShips"]
        total_pax += day["totalPassengers"]

        days.append(
            {
                "date": day["date"],
                "totalShips": day["totalShips"],
                "totalPassengers": day["totalPassengers"],
                "crowdScore": calculate_crowd_score(day["totalPassengers"]),
                "ports": ports,
            }
        )

    return {
        "schemaVersion": "1.1.0",
        "lastUpdated": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "sources": ["vinow"],
        "counts": {
            "totalDays": len(days),
            "totalShipCalls": total_calls,
            "totalPassengers": total_pax,
            "shipsWithUnknownDock": sorted(unknown_docks),
        },
        "days": days,
    }


def main() -> None:
    now = datetime.now(timezone.utc) - timedelta(hours=4)

    start_year = now.year
    start_month = now.month
    for _ in range(MONTHS_PAST):
        if start_month == 1:
            start_month = 12
            start_year -= 1
        else:
            start_month -= 1

    total_months = MONTHS_PAST + 1 + MONTHS_FUTURE

    session = requests.Session()
    session.trust_env = USE_ENV_PROXY
    session.headers.update(BROWSER_HEADERS)

    all_items: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()

    for y, m in iter_months(start_year, start_month, total_months):
        log(f"Fetching {m}-{y} from ViNow...")
        try:
            month_items = scrape_month(session, y, m)
        except requests.RequestException as exc:
            log(f"  {m}-{y}: fetch failed ({exc})")
            continue
        added = 0
        for item in month_items:
            key = (item["date"], item["ship"], item["arrival"], item["departure"], item["rawDock"])
            if key in seen:
                continue
            seen.add(key)
            all_items.append(item)
            added += 1
        log(f"  {m}-{y}: parsed {len(month_items)} items, {added} new")

    if not all_items:
        if OUTPUT_PATH.exists():
            log("No fresh items fetched; keeping existing schedule.json unchanged")
            return
        raise RuntimeError("No schedule items fetched and no existing schedule.json to keep")

    all_items.sort(key=lambda x: (x["date"], x["port"], x["arrival"], x["ship"]))

    schedule = build_schedule(all_items)
    OUTPUT_PATH.write_text(json.dumps(schedule, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    c = schedule["counts"]
    log(
        f"Wrote schedule.json: {c['totalDays']} days, {c['totalShipCalls']} ship calls, "
        f"{c['totalPassengers']:,} passengers, sources={schedule['sources']}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[scraper] FAILED: {exc}", file=sys.stderr)
        raise
