import os
import json
import re
import sys
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

DEFAULT_SOURCE_URL = "https://www.vinow.com/cruise/ship-schedule"
SOURCE_URL = os.environ.get("SOURCE_URL", DEFAULT_SOURCE_URL).rstrip("/")
OUTPUT_PATH = "schedule.json"

MONTHS_PAST = int(os.environ.get("MONTHS_PAST", "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))

R_JINA_PREFIX = "https://r.jina.ai/"

HEADERS = {
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

DAY_HEADING_RE = re.compile(
    r"^\s*(?P<month>[A-Za-z]{3,}\.?)\s+(?P<day>\d+)(?:st|nd|rd|th)\b",
    re.IGNORECASE,
)

SHIP_RE = re.compile(r"^(?P<ship>.+?)\s+\d[\d,]*\s+Guests\b", re.IGNORECASE)

TIME_RE = re.compile(
    r"^(?P<dock>.*?)\s*\((?P<arr>[^-]+?)\s*-\s*(?P<dep>.+?)\)\s*$",
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

def month_to_number(token: str) -> int | None:
    return MONTH_MAP.get(token.strip().lower())

def to_24h(time_str: str) -> str:
    dt = datetime.strptime(time_str.strip(), "%I:%M %p")
    return dt.strftime("%H:%M")

def island_from_dock(dock: str) -> str:
    dl = dock.lower()
    if "cruz bay" in dl:
        return "St. John"
    if "st. croix" in dl:
        return "St. Croix"
    return "St. Thomas"

def fetch_html(url: str) -> str:
    sess = requests.Session()
    sess.headers.update(HEADERS)

    resp = sess.get(url, timeout=60)
    if resp.status_code == 403:
        resp = sess.get(f"{R_JINA_PREFIX}{url}", timeout=60)

    resp.raise_for_status()
    return resp.text

def iter_months(start_year: int, start_month: int, count: int):
    year = start_year
    month = start_month
    for _ in range(count):
        yield year, month
        month += 1
        if month == 13:
            month = 1
            year += 1

def parse_month_text(text: str, year: int, month: int) -> list[dict]:
    items: list[dict] = []

    current_date: str | None = None
    current_ship: str | None = None

    for raw in re.split(r"\r?\n", text):
        line = raw.strip()
        if not line:
            continue

        m = DAY_HEADING_RE.match(line)
        if m:
            heading_month = month_to_number(m.group("month"))
            if heading_month and heading_month != month:
                continue

            day = int(m.group("day"))
            current_date = f"{year:04d}-{month:02d}-{day:02d}"
            current_ship = None
            continue

        m_ship = SHIP_RE.match(line)
        if m_ship:
            current_ship = m_ship.group("ship").strip()
            continue

        m_time = TIME_RE.match(line)
        if m_time and current_date and current_ship:
            dock = m_time.group("dock").strip()
            try:
                arrival = to_24h(m_time.group("arr"))
                departure = to_24h(m_time.group("dep"))
            except ValueError:
                current_ship = None
                continue

            items.append(
                {
                    "date": current_date,
                    "island": island_from_dock(dock),
                    "ship": current_ship,
                    "dock": dock,
                    "arrival": arrival,
                    "departure": departure,
                }
            )
            current_ship = None

    return items

def scrape_month(year: int, month: int) -> list[dict]:
    url = f"{SOURCE_URL}/{month}-{year}/"
    html = fetch_html(url)
    text = BeautifulSoup(html, "html.parser").get_text("\n", strip=False)
    return parse_month_text(text, year, month)

def main():
    now = datetime.utcnow() - timedelta(hours=4)

    start_year = now.year
    start_month = now.month

    for _ in range(MONTHS_PAST):
        if start_month == 1:
            start_month = 12
            start_year -= 1
        else:
            start_month -= 1

    total_months = MONTHS_PAST + 1 + MONTHS_FUTURE

    all_items: list[dict] = []
    seen: set[tuple] = set()

    for y, m in iter_months(start_year, start_month, total_months):
        for item in scrape_month(y, m):
            key = (
                item["date"],
                item["island"],
                item["dock"],
                item["ship"],
                item["arrival"],
                item["departure"],
            )
            if key in seen:
                continue
            seen.add(key)
            all_items.append(item)

    all_items.sort(key=lambda x: (x["date"], x["arrival"], x["ship"]))

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(all_items)} items")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("Scraper failed:", file=sys.stderr)
        raise
