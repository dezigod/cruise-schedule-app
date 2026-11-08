# Historical published cruise listings

The previous collector exported only `today` through the end of the fiscal year. Each successful refresh therefore dropped older dates. The app's bundled historical sources were not advertised by the hosted feed.

This repair publishes yearly `cruise-archive-YYYY.json` files and adds `archiveYears` to the current feed. Recovery combines the app's provenance-bearing WICO/VIPA historical exports with successive versions of the hosted official calendar in Git history, in oldest-to-newest order. Recovered records are schedules, not evidence of actual arrivals, departure or passenger attendance. Missing dates remain unknown.

Initial recovery contains 175/558/396 structured visit records for 2025/2026/2027. Legacy snapshots predate structured visit records; payloads contain 184/598/396 distinct date-and-ship-name labels respectively (source naming variants are not claimed as unique actual visits). Records span October 1–December 31, 2025; January 1–December 31, 2026; January 1–September 30, 2027. These totals include future scheduled records, not just past visits. Actual source records do not cover every date in these ranges.

On later collections, `calendar_archive.py` merges the existing archives, previous published feed, and newly collected feed. New explicit records replace that whole date, including cancellation/status records. A date absent from a rolling feed does not erase its earlier record or imply zero calls. Document hashes and candidate provenance remain attached. Revisions remain recoverable from Git history; current archive files present the latest explicit date snapshot.

The durable delivery stages archives and the feed together, and publishes them in one Git commit. Hash checks prevent lost-response retries or concurrent changes from silently losing data. No new provider traffic is needed for archival. Source backoff/refusal controls remain unchanged. The existing iOS 3 MB / 24-document per-year safety limits fail closed rather than truncate records; exceeding them requires reviewed partitioning/schema work.

108 backend tests passed locally, including roll-off retention, per-year scope, repeat stability, exact provenance references, cancellation replacement, and atomic publication/retry. The normal dirty developer checkout was not modified.
