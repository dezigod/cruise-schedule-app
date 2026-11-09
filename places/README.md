# Visitor source freshness

Runs daily at 10:43 UTC (06:43 AST) via GitHub Actions, plus manual dispatch. Public standard runners, Python standard library, no AI calls or new paid service. GitHub scheduling can be delayed; the queue records actual attempts and successful checks. Inspect the latest run before relying on coverage.

This is a **review queue, not a fact publisher**. It never writes Firebase or app resources. Twelve priority official/operator pages cover admission, hours, facilities and possible closure notices. Initial captures require review. Page or image-URL changes stay pending until reviewed; unchanged pages also require another review after 30 days. No automatic hash acceptance. Failures retain prior successful-check timestamps and flag review. A successful page fetch does not mean its stated prices/hours are current.

## Review procedure
1. Open each source from REVIEW_QUEUE.md and compare current relevant facts to the app's reviewed visitor data. Read dates, exceptions, and the actual schedule images. Resolve conflicting official pages before publishing.
2. If facts changed, separately update the reviewed visitor record with source URL, evidence date, effective dates, and reviewer notes through the normal reviewed-data/Firebase process. Do not infer missing values or turn capacity into attendance.
3. Only after checking, set that source's `reviewedHash` to its `observedHash` and `reviewedAt` to the actual UTC ISO timestamp in check-state.json, with a reviewer note in the commit. These fields are never set by the collector.
4. Rerun the workflow; unchanged reviewed hashes clear pending state until their next 30-day review. Source errors remain visible.

## Limits and safeguards
Robots restrictions, access denials, unknown robots failures, challenges, cross-host redirects and oversized responses are not bypassed. Two-second spacing, 20-second request timeout, 2MB limit. Public state stores only hashes/status/timestamps, not page copies or personal information. Whole-page changes can cause false positives; this is intentional review-first behavior. Images replaced at the same URL, JavaScript-only data and unchanged but inaccurate published facts can escape fingerprint detection. Monthly manual review is required, especially Paradise Point's image timetable. Official ownership does not guarantee accuracy. The starting list does not cover all 80 places or safety advisories.

## Test / run
`python3 -m unittest discover -s places -p 'test_*.py' -v`

`python3 places/check_sources.py`

Outputs: `places/check-state.json` and `places/REVIEW_QUEUE.md`; each workflow also adds the queue to its job summary. No emails or issues are sent automatically.

Coral World admission and operating-day pages are checked separately from its FAQ. Preserve age/residency conditions and seasonal exceptions; do not apply undated closure notices as current closures.
