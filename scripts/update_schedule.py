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
        "text/html,application/xhtml+xml,application/xml;q=0.9,"\
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}


def fetch_source_html(url: str) -> str:
    sess = requests.Session()
    sess.headers.update(BROWSER_HEADERS)

    resp = sess.get(url, timeout=45)
    if resp.status_code == 403:
        resp = sess.get(f"{R_JINA_PREFIX}{url}", timeout=45)

    resp.raise_for_status()
    return resp.text


def _norm_col(col: str) -> str:
    return re.sub(r"\s+", " ", str(col).strip().lower())


def _pick_col(cols, candidates):
    # pick first column containing any substring
    for col in cols:
        lc = col.lower()
        for c in candidates:
            if c in lc:
                return col
    return None


def _parse_date_to_iso(value) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Could not parse date: {value!r}")
    return dt.strftime("%Y-%m-%d")


def _parse_time_hhmm(value) -> str:
    s = str(value).strip()
    if not s or s.lower() in {"nan", "na", "n/a", "-"}:
        raise ValueError(f"Missing/invalid time: {value!r}")

    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        # allow HH:MM
        m = re.match(r"^(\d{1,2}):(\d{2})$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"

        raise ValueError(f"Could not parse time: {s!r}")

    return dt.strftime("%H:%M")


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


def extract_schedule_from_tables(html: str) -> list[dict]:
    tables = pd.read_html(html)
    rows_out: list[dict] = []

    for df in tables:
        if df.empty:
            continue

        df.columns = [_norm_col(c) for c in df.columns]
        cols = list(df.columns)

        date_col = _pick_col(cols, ["date", "day"])
        island_col = _pick_col(cols, ["island", "port"])
        ship_col = _pick_col(cols, ["ship", "vessel"])
        dock_col = _pick_col(cols, ["dock", "pier", "berth"])
        arr_col = _pick_col(cols, ["arrival", "arrive", "arr"])
        dep_col = _pick_col(cols, ["departure", "depart", "dep", "sail"])

        required_cols = [date_col, island_col, ship_col, dock_col, arr_col, dep_col]
        if any(c is None for c in required_cols):
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
        raise RuntimeError(
            "No schedule rows extracted from HTML tables. "
            "VINOW may have changed the layout, or the proxy returned non-table content."
        )

    rows_out.sort(key=lambda x: (x["date"], x["island"], x["arrival"], x["ship"]))
    return rows_out


def main() -> None:
    html = fetch_source_html(SOURCE_URL)
    data = extract_schedule_from_tables(html)

    if not json_is_valid_schedule(data):
        raise RuntimeError("Extracted JSON shape invalid")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH} with {len(data)} rows. Updated: {datetime.utcnow().isoformat()}Z")


if __name__ == "__main__":
    main()
