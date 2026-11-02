# cruise-schedule-app

CrowdCast's official WICO/VIPA calendar and preserved legacy `schedule.json`.

## Active automatic delivery

CrowdCast uses `official-cruise-calendar-v1.json` for official listings. The
GitHub-hosted updater checks whether collection is due every 15 minutes at minutes 7, 22, 37 and 52 UTC,
and makes official-source requests only after six hours since its last success
or collection attempt. Failed attempts do not advance source freshness.
GitHub may delay or drop scheduled runs. No Mac is required. All workflows use
standard Ubuntu runners in this public repository, which do not consume the
account's private Actions-minute allowance.

The legacy `Update Cruise Schedule` workflow is administratively disabled as of
October 15, 2026. It was attempting Gemini extraction when VINow parsing failed,
and its source-use questions remain unresolved. Its feed, source, credentials and
history are preserved; it must not be re-enabled without a separate review. The
sections below about that legacy parser describe its code, not active automation.

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
- `USE_ENV_PROXY` (default `false`; set to `true` only if your proxy permits ViNow)

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


## Automatic official cruise calendar

The separate `official-cruise-calendar-v1.json` feed contains unreviewed WICO/VIPA listings with provenance, source-check timestamps, missing capacities, conflict dates and held dates. It does not establish complete coverage, actual arrivals, actual crowds, or human-approved zero-call dates. The existing `schedule.json` feed is unchanged.

`Update Official Cruise Calendar` runs on GitHub-hosted Ubuntu every 15 minutes at minutes 7, 22, 37 and 52 UTC and supports manual dispatch. Its collector enforces a minimum of six hours between collection attempts, including partial failures; earlier attempts report `deferred` and skip publication without changing source timestamps or the published feed. Provider refusals, backoff and real collection errors still fail visibly. Quarter-hour scheduling provides another opportunity after a delayed run without increasing the authorized source-collection rate. GitHub scheduling may be delayed. The job discovers the current WICO fiscal and monthly schedules and the current VIPA St. Thomas fiscal schedule from their official pages. Ambiguous revisions and structural parsing failures stop publication; monthly total discrepancies hold the entire affected month. Provider backoff metadata is persisted on the dedicated collection-state branch. Raw PDFs and detailed parsing records stay in ephemeral runner storage and are not committed or uploaded as artifacts.

After validation, the job saves the derived calendar and its pending delivery record together on the `collection-state` branch. If publication to `main` is interrupted, the next scheduled check can publish that exact calendar with its original timestamps. A concurrent edit to the published calendar takes precedence and the staged copy stays in history for review. If an already-listed WICO monthly schedule disappears during its month, the collector keeps the last valid calendar.

The app retains its last valid copy and labels source data stale after 12 hours. Ship capacities expire after 365 days and require new source validation. The calendar window ends at the current fiscal year's September 30. The delivery monitors check the app payload, both fiscal schedules, the current WICO monthly schedule, each document's source-check time, recorded provider failures and any publication pending for over 30 minutes. Their run logs include the observed source timestamps.

To pause collection, disable the `Update Official Cruise Calendar` workflow in Actions. To retry, use Run workflow after the six-hour interval or provider backoff expires. Use the workflow's failure logs to review changed source formats; do not bypass refusals or manufacture clean data. Source-use authorization is owner-attested, not independently verified with the providers. No human review stamps are generated by automation.

## Feed reliability and alerts (November 2, 2026)

Native GitHub schedules are enabled. Outage collection also retains its existing
Cloudflare five-minute dispatch timer; the durable collector throttle prevents
duplicate source polls. Its dedicated dispatch token is documented to expire
January 13, 2027 and must be renewed; native GitHub scheduling is a fallback.

`Monitor delivered feed` runs four times an hour and after collector jobs. It
checks the actual public feed: cruise document checks must be within 12 hours;
outage collection and WAPA publication must be within 30 minutes. Missing,
invalid or future timestamps and failed collection status are incidents. It
opens one `Feed freshness alert` issue mentioning @dezigod and closes it after
verified recovery. Enable GitHub email/push notifications for mentions; opening
an issue does not prove delivery to an inbox or device. A separate Codex watcher
checks both feeds to catch missing GitHub jobs while the desktop scheduler is
available. Neither mechanism is an availability guarantee.

Transient source failures retry on later authorized collection attempts, honoring
Retry-After and backoff. Access refusals stay stopped for manual resolution.
Previous valid data and original source timestamps are preserved. Do not reset
state or relabel old data as fresh to clear an alert.

Cruise state is now persisted as allowlisted `state.json` on the dedicated
`collection-state` branch, using expected blob SHAs for updates. A remote
throttle reservation must succeed before source requests. Missing or corrupt
state fails closed, and no disposable Actions cache is used for this state.
The initial state was migrated from the latest verified published source
checks and reviewed successful-run provider status. Never delete this branch
to bypass a refusal. Raw PDFs remain temporary and private.

## Native schedule investigation — November 2, 2026

The workflow was active on main, Actions enabled, and its associated account
active, but no native scheduled events appeared after resumption. Manual
collection and publication succeeded. GitHub reported Actions operational.
These observations isolate the missing event to scheduler delivery; they do
not establish GitHub's internal cause. The cron was changed to four off-hour
checks per hour (7, 22, 37, 52) by the active repository owner. The durable
six-hour collection interval and provider backoff remain authoritative.
A native event=schedule run is required before claiming recurrence verified.

## Visit quality and capacity enrichment — November 2, 2026

The feed now merges identical vessel/date/berth/time/status rows, retaining every
source candidate in top-level `visits`. Different Crown Bay berths, different
ports, and planned-time discrepancies remain explicit conflicts; source majority
is not approval. `visits[].conflictReasons` distinguishes these cases, and
`reviewFlags` includes monthly-total discrepancies. No source discrepancy is
silently corrected and no complete-day coverage is generated.

Visits carry a source-derived status: listed, sourceCancelled, sourceTentative,
sourceRescheduled, needsReview or conflicting. Source status does not verify
actual arrival. Cancellation/tentative/rescheduled rows stay in `visits` and
heldDays but are withheld from legacy active `ships` arrays; disagreements
between a listed and status-bearing variant remain blocked in older clients.

`guestCapacity` and `capacityMeaning` explicitly describe double-occupancy
capacity. Legacy `passengers` is retained with the same value for compatibility.
Only sourced, unexpired double-occupancy values are usable. Operator evidence
is linked in each row's existing `listingDetails` (basis/source/check time);
source document checks and revision metadata are also supplied when known.

The current review adds eight explicitly sourced operator records. Celebrity
Constellation/Eclipse pages with an unspecified occupancy basis, missing Xcel
basis evidence and the pre-existing disputed Carnival Vista capacity remain
unresolved. No capacity is inferred from sister ships or nominal cabin counts.

The enriched publication reuses the last verified source rows and matching
local originals. It preserves the original source-check timestamps and does not
contact providers or advance collection state. Subsequent authorized collection
uses the same enriched exporter. Raw source PDFs remain outside the repo.
The final serialized feed is bounded to the existing iPhone 3 MB transport cap.
