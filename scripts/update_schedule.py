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
    "jan": 1, "jan.": 1,
    "feb": 2, "feb.": 2,
    "mar": 3, "mar.": 3,
    "apr": 4, "apr.": 4,
    "may": 5,
    "jun": 6, "jun.": 6,
    "jul": 7, "jul.": 7,
    "aug": 8, "aug.": 8,
    "sep": 9, "sep.": 9, "sept": 9, "sept.": 9,
    "oct": 10, "oct.": 10,
    "nov": 11, "nov.": 11,
    "dec": 12, "dec.": 12,
}


def log(message: str) -> None:
    print(f"[scraper] {message}")


def month_to_number(token: str) -> int | None:
    return MONTH_MAP.get(token.strip().lower())


def to_24h(time_str: str) -> str:
    return datetime.strptime(time_str.strip(), "%I:%M %p").strftime("%H:%M")


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
    year, month = start_year, start_month
    for _ in range(count):
        yield year, month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1


def fetch_html(session: requests.Session, url: str) -> str:
    from time import sleep

    for attempt in range(REQUEST_RETRIES):
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 403:
                r = session.get(f"{R_JINA_PREFIX}{url.replace('https://','').replace('http://','')}", timeout=60)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if attempt < REQUEST_RETRIES - 1:
                log(f"retry {attempt+1}: {e}")
                sleep(REQUEST_RETRY_DELAY_SECONDS)
            else:
                raise


def scrape_month(session, year, month):
    url = f"{SOURCE_URL}/{month}-{year}/"
    soup = BeautifulSoup(fetch_html(session, url), "html.parser")

    items = []

    for h3 in soup.find_all("h3"):
        m = DAY_HEADING_RE.match(h3.get_text(" ", strip=True))
        if not m:
            continue

        day = int(m.group("day"))
        date_str = f"{year:04d}-{month:02d}-{day:02d}"

        lines = []
        for sib in h3.next_siblings:
            if isinstance(sib, NavigableString):
                continue
            if getattr(sib, "name", None) in {"h2", "h3"}:
                break
            txt = sib.get_text("\n", strip=True)
            if txt:
                lines += [l.strip() for l in re.split(r"[\r\n]+", txt) if l.strip()]

        i = 0
        while i < len(lines):
            m_ship = SHIP_RE.match(lines[i])
            if not m_ship:
                i += 1
                continue

            ship = m_ship.group("ship").strip()
            passengers = int(m_ship.group("passengers").replace(",", "")) if m_ship.group("passengers") else 0
            line = m_ship.group("line") or ""

            j = i + 1
            while j < len(lines):
                m_time = TIME_RE.match(lines[j])
                if m_time:
                    raw = m_time.group("dock")
                    items.append({
                        "date": date_str,
                        "ship": ship,
                        "line": line,
                        "arrival": to_24h(m_time.group("arr")),
                        "departure": to_24h(m_time.group("dep")),
                        "passengers": passengers,
                        "island": island_from_dock(raw),
                        "port": normalize_port_name(raw),
                        "dock": normalize_dock_code(raw),
                        "rawDock": raw,
                    })
                    break
                j += 1

            i = j + 1

    return items


def build_schedule(items):
    grouped = {}
    unknown = set()

    for i in items:
        d = grouped.setdefault(i["date"], {"totalShips": 0, "totalPassengers": 0, "ports": defaultdict(list)})
        if i["dock"] == "Unknown":
            unknown.add(i["ship"])

        d["ports"][i["port"]].append({
            "name": i["ship"],
            "line": i["line"],
            "arrival": i["arrival"],
            "departure": i["departure"],
            "passengers": i["passengers"],
            "island": i["island"],
            "dock": i["dock"],
            "rawDock": i["rawDock"],
        })

        d["totalShips"] += 1
        d["totalPassengers"] += i["passengers"]

    days = []
    total_calls = total_pax = 0

    for date in sorted(grouped):
        d = grouped[date]

        ports = []
        for p, ships in sorted(d["ports"].items()):
            ships.sort(key=lambda s: (s["arrival"], s["name"]))
            ports.append({"name": p, "ships": ships})

        total_calls += d["totalShips"]
        total_pax += d["totalPassengers"]

        days.append({
            "date": date,
            "totalShips": d["totalShips"],
            "totalPassengers": d["totalPassengers"],
            "crowdScore": calculate_crowd_score(d["totalPassengers"]),
            "ports": ports,
        })

    return {
        "schemaVersion": "1.1.0",
        "lastUpdated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sources": ["vinow"],
        "counts": {
            "totalDays": len(days),
            "totalShipCalls": total_calls,
            "totalPassengers": total_pax,
            "shipsWithUnknownDock": sorted(unknown),
        },
        "days": days,
    }


def main():
    now = datetime.now(timezone.utc) - timedelta(hours=4)

    start_year, start_month = now.year, now.month
    for _ in range(MONTHS_PAST):
        start_month -= 1
        if start_month == 0:
            start_month = 12
            start_year -= 1

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    items = []
    seen = set()

    for y, m in iter_months(start_year, start_month, MONTHS_PAST + 1 + MONTHS_FUTURE):
        log(f"{m}-{y}")
        try:
            for i in scrape_month(session, y, m):
                key = (i["date"], i["ship"], i["arrival"], i["departure"], i["rawDock"])
                if key not in seen:
                    seen.add(key)
                    items.append(i)
        except Exception as e:
            log(f"fail {m}-{y}: {e}")

    if not items:
        raise RuntimeError("No data fetched")

    items.sort(key=lambda x: (x["date"], x["port"], x["arrival"], x["ship"]))

    schedule = build_schedule(items)
    OUTPUT_PATH.write_text(json.dumps(schedule, indent=2), encoding="utf-8")

    log("Done")


if __name__ == "__main__":
    main()