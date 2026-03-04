import os
import json
import re
import sys
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup, NavigableString

DEFAULT_SOURCE_URL = "https://www.vinow.com/cruise/ship-schedule"
SOURCE_URL = os.environ.get("SOURCE_URL", DEFAULT_SOURCE_URL).rstrip("/")
OUTPUT_PATH = "schedule.json"

# How far back and forward to scrape
MONTHS_PAST = int(os.environ.get("MONTHS_PAST", "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))

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

DAY_HEADING_RE = re.compile(
    r"^\s*(?P<month>[A-Za-z]{3,}\.?)\s+(?P<day>\d+)(?:st|nd|rd|th)\b",
    re.IGNORECASE,
)

SHIP_RE = re.compile(r"^\s*(?P<ship>.+?)\s+(\d[\d,]*\s+Guests?)?\s*$")
DOCK_RE = re.compile(r"^\s*(?P<dock>.+?)\s+\((?P<time>.+?)\)\s*$")

TIME_RE = re.compile(
    r"^\s*(?P<start>\d{1,2}:\d{2}\s*[AP]M)\s*-\s*(?P<end>\d{1,2}:\d{2}\s*[AP]M)\s*$",
    re.IGNORECASE,
)

MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

def month_to_number(month: str) -> int | None:
    m = month.lower().strip().rstrip(".")
    return MONTH_MAP.get(m)

def to_24h(time_str: str) -> str:
    dt = datetime.strptime(time_str.strip(), "%I:%M %p")
    return dt.strftime("%H:%M")

def infer_island(dock: str) -> str:
    d = dock.lower()
    if "cruz bay" in d or "st. john" in d:
        return "St. John"
    if "st. croix" in d:
        return "St. Croix"
    return "St. Thomas"

def fetch_html(url: str) -> str:
    sess = requests.Session()
    sess.headers.update(BROWSER_HEADERS)

    resp = sess.get(url, timeout=60)
    if resp.status_code == 403:
        resp = sess.get(f"{R_JINA_PREFIX}{url}", timeout=60)

    resp.raise_for_status()
    return resp.text

def months_to_scrape(now: datetime) -> list[tuple[int, int]]:
    start_year = now.year
    start_month = now.month

    months: list[tuple[int, int]] = [(start_year, start_month)]

    y, m = start_year, start_month
    for _ in range(MONTHS_PAST):
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1
        months.append((y, m))

    y, m = start_year, start_month
    for _ in range(MONTHS_FUTURE):
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
        months.append((y, m))

    return months

def scrape_month(year: int, month: int) -> list[dict]:
    url = f"{SOURCE_URL}/{month}-{year}/"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    items: list[dict] = []

    for h3 in soup.find_all("h3"):
        heading = h3.get_text(" ", strip=True)
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
            text = None
            if isinstance(sib, NavigableString):
                text = str(sib)
            else:
                if getattr(sib, "name", None) in {"h2", "h3"}:
                    break
                text = sib.get_text("\n", strip=False)

            if not text:
                continue

            for ln in re.split(r"[\r\n]+", text):
                ln = ln.strip()
                if ln:
                    block_lines.append(ln)

        i = 0
        while i < len(block_lines):
            ship_line = block_lines[i]
            m_ship = SHIP_RE.match(ship_line)
            if not m_ship:
                i += 1
                continue

            ship = m_ship.group("ship").strip()

            if i + 1 >= len(block_lines):
                break

            dock_line = block_lines[i + 1]
            m_dock = DOCK_RE.match(dock_line)
            if not m_dock:
                i += 1
                continue

            dock = m_dock.group("dock").strip()
            time_range = m_dock.group("time")

            m_time = TIME_RE.match(time_range)
            if not m_time:
                i += 2
                continue

            arrival = to_24h(m_time.group("start"))
            departure = to_24h(m_time.group("end"))
            island = infer_island(dock)

            items.append(
                {
                    "date": date_str,
                    "island": island,
                    "ship": ship,
                    "dock": dock,
                    "arrival": arrival,
                    "departure": departure,
                }
            )

            i += 2

    return items

def main() -> None:
    # America/St_Thomas is UTC-4; approximate for month window
    now = datetime.utcnow() - timedelta(hours=4)
    month_list = months_to_scrape(now)

    seen = set()
    all_items: list[dict] = []

    for year, month in month_list:
        try:
            for item in scrape_month(year, month):
                key = (item["date"], item["ship"], item["dock"], item["arrival"], item["departure"])
                if key in seen:
                    continue
                seen.add(key)
                all_items.append(item)
        except Exception as e:
            print(f"Failed to scrape {month}-{year}: {e}", file=sys.stderr)

    all_items.sort(key=lambda x: x["date"])

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(all_items)} items")

if __name__ == "__main__":
    main()
