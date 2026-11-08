# Historical published cruise listings

The previous collector exported only `today` through the end of the fiscal year. Each successful refresh therefore dropped older dates. The app's bundled historical sources were not advertised by the hosted feed.

This repair publishes yearly `cruise-archive-YYYY.json` files and adds `archiveYears` to the current feed. Recovery combines the app's provenance-bearing WICO/VIPA historical exports with successive versions of the hosted official calendar in Git history, in oldest-to-newest order. Recovered records are schedules, not evidence of actual arrivals, departure or passenger attendance. Missing dates remain unknown.

Initial recovery contains 175/558/396 structured visit records for 2025/2026/2027. Legacy snapshots predate structured visit records; payloads contain 184/598/396 distinct date-and-ship-name labels respectively (source naming variants are not claimed as unique actual visits). Records span October 1–December 31, 2025; January 1–December 31, 2026; January 1–September 30, 2027. These totals include future scheduled records, not just past visits. Actual source records do not cover every date in these ranges.

On later collections, `calendar_archive.py` merges the existing archives, previous published feed, and newly collected feed. New explicit records replace that whole date, including cancellation/status records. A date absent from a rolling feed does not erase its earlier record or imply zero calls. Document hashes and candidate provenance remain attached. Revisions remain recoverable from Git history; current archive files present the latest explicit date snapshot.

The durable delivery stages archives and the feed together, and publishes them in one Git commit. Hash checks prevent lost-response retries or concurrent changes from silently losing data. No new provider traffic is needed for archival. Source backoff/refusal controls remain unchanged. The existing iOS 3 MB / 24-document per-year safety limits fail closed rather than truncate records; exceeding them requires reviewed partitioning/schema work.

108 backend tests passed locally, including roll-off retention, per-year scope, repeat stability, exact provenance references, cancellation replacement, and atomic publication/retry. The normal dirty developer checkout was not modified.

## FY2025 recovery and three-calendar-year discovery (November 8, 2026)

Recovered the authoritative WICO PDF linked by its August 27, 2024 release:
https://wico-ltd.com/wico-releases-2025-ship-schedule/
https://wico-ltd.com/wp-content/uploads/2024/08/October-2024-to-September-2025-Ship-Schedule.pdf
SHA256 `3de04699a314ee061343cdadbab0b90c0ce3edb021bc82c4ba8ef8935591462c`.

The 24-page landscape schedule has a May 16, 2024 printed date and covers October 2024–September 2025. It is an early published plan, not the final schedule or proof of actual arrivals. A separate deterministic layout profile handles its spaced dates, nationality column, source routes and times. The PDF was visually checked against extracted rows. January–September 2025 contributes 310 St. Thomas scheduled visit records, with January 29, March 25, March 26 and April 2 conservatively held for source review (blank/TBA route fields stay unavailable; two suspicious printed one-minute stays are preserved but flagged, not corrected by guessing). St. John is excluded. Existing October–December 2025 records are preserved. Combined 2025 now has 485 structured visit records and source rows across all twelve months; missing dates remain unknown. 2026 has source rows across all twelve months but is not certified complete. Official current listings expose no FY2028 schedule, so October–December 2027 remains unavailable.

Optional fiscal-year discovery is **backfill only**. It examines links on the same already-fetched official pages, within fiscal years needed for the previous/current/next calendar years; it skips current required sources and already-archived URLs. It does not guess URLs, add polling cadence, or bypass source backoff/refusals. Optional absent/ambiguous/invalid years do not replace today's feed. Supplements are merged before retained archives and the active feed, so older annual sources cannot overwrite later monthly/revised dates.

**Freshness limitation:** optional snapshots do not recheck archived URLs or automatically replace existing dates with new optional annual revisions. Such dates remain archived source snapshots until the normal current-fiscal collector covers them or a reviewed reconciliation occurs. The six-hour source-refresh limit applies to active sources, not a promise that every future archive is refreshed every six hours. No claim of all-island or complete-day coverage is made.

111 backend tests passed, including the landscape columns/inherited date/St. John separation, bounded optional discovery, and existing-date precedence.
