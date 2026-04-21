# cruise-schedule-app

Daily-generated `schedule.json` feed for St. Thomas cruise arrivals.

## Data pipeline

- **Primary source:** ViNow ship schedule (`https://www.vinow.com/cruise/ship-schedule/`).
- **Parser strategy:** month-by-month page fetch and structured extraction of ship, guests, dock, and times.
- **Output target:** `schedule.json` in repo root (committed by GitHub Actions).

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/update_schedule.py
```

## Environment variables

- `SOURCE_URL` (default `https://www.vinow.com/cruise/ship-schedule`)
- `MONTHS_PAST` (default `6`)
- `MONTHS_FUTURE` (default `12`)
- `REQUEST_RETRIES` (default `3`)
- `REQUEST_RETRY_DELAY_SECONDS` (default `1.5`)

## JSON schema

Top-level format (schema version `1.x`):

```json
{
  "schemaVersion": "1.1.0",
  "lastUpdated": "2026-04-21T00:00:00Z",
  "sources": ["vinow"],
  "counts": {
    "totalDays": 0,
    "totalShipCalls": 0,
    "totalPassengers": 0,
    "shipsWithUnknownDock": []
  },
  "days": []
}
```

Each ship includes:
- `dock`: normalized (`WICO`, `CB`, `Harbor`, or `Unknown`)
- `rawDock`: raw dock text from ViNow

## GitHub Actions

Workflow: `.github/workflows/update-schedule.yml`

- Runs daily and on manual dispatch.
- Rebuilds `schedule.json` from ViNow.
- Auto-commits only when JSON changed.

## Public feed URL

```text
https://raw.githubusercontent.com/<your-gh-username>/cruise-schedule-app/main/schedule.json
```
