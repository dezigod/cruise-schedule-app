import os
import re
import json
import requests
import pandas as pd
from datetime import datetime

DEFAULT_SOURCE_URL = "https://www.vinow.com/cruise/ship-schedule/"
SOURCE_URL = os.environ.get("SOURCE_URL", DEFAULT_SOURCE_URL).rstrip("/")
OUTPUT_PATH = "schedule.json"

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

MONTHS_PAST = int(os.environ.get("MONTHS_PAST", "6"))
MONTHS_FUTURE = int(os.environ.get("MONTHS_FUTURE", "12"))


# Docks include CB, WICO, Havensight, Crown Bay, etc.
DOCK_ISLAND = {
    "wico": "St. Thomas",
    "cb": "St. Thomas",
    "crownbay": "St. Thomas",
    "havensight": "St. Thomas",

    "cruzbay": "St. John",

    "stcroix": "St. Croix",
    "frederiksted": "St. Croix",
    "gallowsbay": "St. Croix",
    "christiansted": "St. Croix",
}

MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())


def _short_error(err: Exception, max_chars: int = 300) -> str:
    msg = f"{type(err).__name__}: {err}"
    return msg[:max_chars]


def iter_months(now=None, months_past=MONTHS_PAST, months_future=MONTHS_FUTURE):
    if now is None:
        now = datetime.utcnow()

    year = now.year
    month = now.month

    # Back up to start month
    m = month
    y = year
    for _ in range(months_past):
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    for _ in range(months_past + months_future + 1):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def fetch_source_html(url: str) -> str:
    sess = requests.Session()
    sess.headers.update(BROWSER_HEADERS)

    resp = sess.get(url, timeout=45)
    if resp.status_code == 403:
        resp = sess.get(f"{R_JINA_PREFIX}{url}", timeout=45)

    resp.raise_for_status()
    return resp.text


def _pick_col(cols, candidates):
    for c in candidates:
        for col in cols:
            if c in col:
                return col
    return None


def _parse_date_to_iso(value: str) -> str:
    dt = pd.to_datetime(value, errors="coerce", infer_datetime_format=True)
    if pd.isna(dt):
        raise ValueError(f"Could not parse date: {value!r}")
    return dt.strftime("%Y-%m-%d")


def _parse_time_hhmm(value: str) -> str:
    s = str(value).strip()
    if not s or s.lower() in {"nan", "na", "n/a", "-"}:
        raise ValueError(f"Missing/invalid time: {value!r}")

    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        m = re.match(r"^(\d{1,2}):(\d{2})$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
        raise ValueError(f"Could not parse time: {s!r}")

    return dt.strftime("%H:%M")


def extract_schedule_from_tables(html: str) -> list[dict]:
    # If there are no tables, this should throw and the caller will fallback to markdown parsing.
    tables = pd.read_html(html)

    rows_out: list[dict] = []

    for df in tables:
        if df.empty:
            continue

        df.columns = [_norm(c) for c in df.columns]
        cols = list(df.columns)

        date_col = _pick_col(cols, ["date", "day"])
        ship_col = _pick_col(cols, ["ship", "vessel"])
        island_col = _pick_col(cols, ["island", "port"])
        dock_col = _pick_col(cols, ["dock", "pier", "berth"])
        arr_col = _pick_col(cols, ["arrival", "arrive", "arr"])
        dep_col = _pick_col(cols, ["departure", "depart", "dep", "sail"])

        must_have = [date_col, ship_col, dock_col, arr_col, dep_col]
        if any(c is None for c in must_have):
            continue

        if island_col is None:
            continue

        for _, r in df.iterrows():
            try:
                date_iso = _parse_date_to_iso(r[date_col])
                island = str(r[island_col]).strip()
                ship = str(r[ship_col]).strip()
                dock = str(r[dock_col]).strip()
                arrival = _parse_time_hhmm(r[arr_col])
                departure = _parse_time_hhmm(r[dep_col])

                if not island or island.lower() == "nan":
                    continue
                if not ship or ship.lower() == "nan":
                    continue
                if not dock or dock.lower() == "nan":
                    continue

                rows_out.append(
                    {
                        "date": date_iso,
                        "island": island,
                        "ship": ship,
                        "dock": dock,
                        "arrival": arrival,
                        "departure": departure,
                    }
                )
            except Exception:
                continue

    if not rows_out:
        raise RuntimeError("No schedule rows extracted from HTML tables")

    rows_out.sort(key=lambda x: (x["date"], x["island"], x["arrival"], x["ship"]))
    return rows_out


def parse_markdown_text(text: str, month: int, year: int) -> list[dict]:
    # VINOW pages (especially via r.jina.ai) are mostly markdown text.
    rows = []

    heading_re = re.compile(r"^###\s+([A-Za-z]{3})\.?\s+(\d{1,2})")
    dock_time_re = re.compile(
        r"^(?P<dock>[A-Za-z0-9 .]+)\s*\(\s*(?P<arr>[^-]+?)\s*-\s*(?P<dep>[^)]+?)\s*\)$"
    )

    lines = text.splitlines()
    day = None
    ship_line = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        m = heading_re.match(line)
        if m:
            abbr = m.group(1).lower()
            d = int(m.group(2))
            abbr_num = MONTH_MAP.get(abbr)
            # Prefer the URL month/year, but only use headings if URL parsing isn't available.
            # We don't override month/year here.
            day = d
            ship_line = None
            continue

        dtm = dock_time_re.match(line)
        if dtm and day is not None and ship_line:
            dock = dtm.group("dock").strip()
            arrival = dtm.group("arr").strip()
            departure = dtm.group("dep").strip()

            # Normalize times
            try:
                arrival_hhmm = _parse_time_hhmm(arrival)
                departure_hhmm = _parse_time_hhmm(departure)
            except Exception:
                ship_line = None
                continue

            dock_key = _norm(dock)
            island = DOCK_ISLAND.get(dock_key, "Unknown")

            rows.append(
                {
                    "date": f"{year:04d}-{month:02d}-{day:02d}",
                    "island": island,
                    "ship": ship_line,
                    "dock": dock,
                    "arrival": arrival_hhmm,
                    "departure": departure_hhmm,
                }
            )
            ship_line = None
            continue

        # ship_line
        if day is not None and ship_line is None and len(line) > 0:
            ship_line = line

    return rows


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


def dedupe_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for r in rows:
        key = (r["date"], r["island"], r["ship"], r["dock"], r["arrival"], r["departure"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    out.sort(key=lambda x: (x["date"], x["island"], x["arrival"], x["ship"]))
    return out


def extract_passenger_count(ship_name: str) -> tuple[str, int]:     m = re.search(r"^(.*?)\s+([\d,]+)\s+Guests?$", ship_name.strip(), re.IGNORECASE)     if m:         clean_name = m.group(1).strip()         passengers = int(m.group(2).replace(",", ""))         return clean_name, passengers     return ship_name.strip(), 0   def normalize_port_name(dock: str) -> str:     dock_key = _norm(dock)     if dock_key in {"wico", "havensight"}:         return "Havensight"     if dock_key in {"cb", "crownbay"}:         return "Crown Bay"     return dock.strip()   def calculate_crowd_score(total_passengers: int) -> int:     if total_passengers <= 2000:         return 1     if total_passengers <= 5000:         return 3     if total_passengers <= 8000:         return 5     if total_passengers <= 12000:         return 7     if total_passengers <= 16000:         return 8     return 10   def transform_rows_to_grouped_schedule(rows: list[dict]) -> dict:     grouped_by_date: dict[str, dict] = {}      for row in rows:         date = row["date"]         port_name = normalize_port_name(row["dock"])         ship_name, passengers = extract_passenger_count(row["ship"])          if date not in grouped_by_date:             grouped_by_date[date] = {                 "date": date,                 "totalShips": 0,                 "totalPassengers": 0,                 "crowdScore": 0,                 "ports": {}             }          day_entry = grouped_by_date[date]          if port_name not in day_entry["ports"]:             day_entry["ports"][port_name] = {                 "name": port_name,                 "ships": []             }          day_entry["ports"][port_name]["ships"].append({             "name": ship_name,             "arrival": row["arrival"],             "departure": row["departure"],             "passengers": passengers,             "island": row["island"],             "rawDock": row["dock"],         })          day_entry["totalShips"] += 1         day_entry["totalPassengers"] += passengers      days: list[dict] = []      for date in sorted(grouped_by_date.keys()):         day_entry = grouped_by_date[date]          day_entry["crowdScore"] = calculate_crowd_score(day_entry["totalPassengers"])          ports = list(day_entry["ports"].values())         ports.sort(key=lambda p: p["name"])          for port in ports:             port["ships"].sort(key=lambda s: (s["arrival"], s["name"]))          days.append({             "date": day_entry["date"],             "totalShips": day_entry["totalShips"],             "totalPassengers": day_entry["totalPassengers"],             "crowdScore": day_entry["crowdScore"],             "ports": ports,         })      return {         "lastUpdated": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",         "days": days,     }   def main() -> None:
    all_rows: list[dict] = []

    for year, month in iter_months():
        url = f"{SOURCE_URL}/{month}-{year}/"
        try:
            html = fetch_source_html(url)
        except Exception as fetch_err:
            print(f"Skipping {url} because fetch failed: {_short_error(fetch_err)}", flush=True)
            continue

        # Try HTML table extraction; fallback to markdown parsing.
        try:
            rows = extract_schedule_from_tables(html)
        except Exception:
            # It's probably markdown (r.jina.ai), so parse as text.
            rows = parse_markdown_text(html, month=month, year=year)

        all_rows.extend(rows)

    all_rows = dedupe_rows(all_rows)

    if not json_is_valid_schedule(all_rows):
        raise RuntimeError("Internal error: extracted JSON shape invalid.")

    final_schedule = transform_rows_to_grouped_schedule(all_rows)  with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(final_schedule, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(all_rows)} rows. Updated: {datetime.utcnow().isoformat()}Z", flush=True)


if __name__ == "__main__":
    main()
