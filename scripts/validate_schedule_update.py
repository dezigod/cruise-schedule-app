#!/usr/bin/env python3
"""
Validate a freshly generated schedule.json before GitHub Actions commits it.

The validator is intentionally conservative: if the candidate looks broken and
the previous schedule had real ship data, restore the previous file and exit 0.
That keeps the workflow green while preventing a known-good schedule from being
replaced by an empty or malformed one.
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any


def load_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        parsed = json.loads(path.read_text())
    except Exception as exc:
        return None, f"{path} is not valid JSON: {exc}"

    if not isinstance(parsed, dict):
        return None, f"{path} must contain a top-level JSON object"

    return parsed, None


def summarize(schedule: dict[str, Any]) -> dict[str, Any]:
    days = schedule.get("days") if isinstance(schedule.get("days"), list) else []
    total_ship_calls = 0
    months: set[str] = set()
    dates: set[str] = set()

    for day in days:
        if not isinstance(day, dict):
            continue
        date = day.get("date")
        if isinstance(date, str) and len(date) >= 7:
            dates.add(date)
            months.add(date[:7])
        ships = day.get("ships")
        if isinstance(ships, list):
            total_ship_calls += len(ships)

    return {
        "days": len(days),
        "dates": len(dates),
        "months": len(months),
        "ship_calls": total_ship_calls,
    }


def validate_structure(schedule: dict[str, Any], label: str) -> list[str]:
    reasons: list[str] = []

    for key in ("schemaVersion", "lastUpdated", "sources", "counts", "days"):
        if key not in schedule:
            reasons.append(f"{label} is missing top-level key {key!r}")

    if not isinstance(schedule.get("schemaVersion"), str):
        reasons.append(f"{label}.schemaVersion must be a string")

    if not isinstance(schedule.get("sources"), list):
        reasons.append(f"{label}.sources must be a list")

    counts = schedule.get("counts")
    if not isinstance(counts, dict):
        reasons.append(f"{label}.counts must be an object")
        counts = {}

    days = schedule.get("days")
    if not isinstance(days, list):
        reasons.append(f"{label}.days must be a list")
        return reasons

    computed_ship_calls = 0
    seen_dates: set[str] = set()

    for index, day in enumerate(days):
        if not isinstance(day, dict):
            reasons.append(f"{label}.days[{index}] must be an object")
            continue

        date = day.get("date")
        if not isinstance(date, str):
            reasons.append(f"{label}.days[{index}].date must be a string")
        else:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                reasons.append(f"{label}.days[{index}].date must be YYYY-MM-DD")
            if date in seen_dates:
                reasons.append(f"{label}.days contains duplicate date {date!r}")
            seen_dates.add(date)

        ships = day.get("ships")
        if not isinstance(ships, list):
            reasons.append(f"{label}.days[{index}].ships must be a list")
            continue

        if len(ships) == 0:
            reasons.append(f"{label}.days[{index}].ships must not be empty")

        computed_ship_calls += len(ships)
        for ship_index, ship in enumerate(ships):
            ship_label = f"{label}.days[{index}].ships[{ship_index}]"
            if not isinstance(ship, dict):
                reasons.append(f"{ship_label} must be an object")
                continue
            if not isinstance(ship.get("name"), str) or not ship["name"].strip():
                reasons.append(f"{ship_label}.name must be a non-empty string")
            if not isinstance(ship.get("passengers"), int) or ship["passengers"] < 0:
                reasons.append(f"{ship_label}.passengers must be a non-negative integer")
            for time_key in ("arrival", "departure"):
                value = ship.get(time_key)
                if not isinstance(value, str):
                    reasons.append(f"{ship_label}.{time_key} must be a string")

    if counts.get("totalDays") != len(days):
        reasons.append(
            f"{label}.counts.totalDays is {counts.get('totalDays')!r}, expected {len(days)}"
        )

    if counts.get("totalShipCalls") != computed_ship_calls:
        reasons.append(
            f"{label}.counts.totalShipCalls is {counts.get('totalShipCalls')!r}, "
            f"expected {computed_ship_calls}"
        )

    return reasons


def validate_candidate(previous_path: Path, candidate_path: Path) -> tuple[bool, list[str]]:
    previous, previous_error = load_json(previous_path)
    candidate, candidate_error = load_json(candidate_path)

    if candidate_error:
        return False, [candidate_error]

    assert candidate is not None

    reasons = validate_structure(candidate, "candidate")
    if reasons:
        return False, reasons

    previous_summary: dict[str, Any] = {"days": 0, "dates": 0, "months": 0, "ship_calls": 0}
    if previous_error:
        print(f"Schedule validation warning: could not inspect previous schedule: {previous_error}")
    elif previous is not None:
        previous_summary = summarize(previous)

    candidate_summary = summarize(candidate)
    previous_size = previous_path.stat().st_size if previous_path.exists() else 0
    candidate_size = candidate_path.stat().st_size if candidate_path.exists() else 0

    print(
        "Previous schedule: "
        f"{previous_summary['days']} days, "
        f"{previous_summary['months']} months, "
        f"{previous_summary['ship_calls']} ship calls, "
        f"{previous_size} bytes"
    )
    print(
        "Candidate schedule: "
        f"{candidate_summary['days']} days, "
        f"{candidate_summary['months']} months, "
        f"{candidate_summary['ship_calls']} ship calls, "
        f"{candidate_size} bytes"
    )

    previous_had_ships = previous_summary["ship_calls"] > 0

    if previous_had_ships and candidate_summary["ship_calls"] == 0:
        reasons.append("candidate has zero ship calls while previous schedule had ship data")

    if previous_had_ships and previous_summary["days"] >= 6:
        minimum_days = max(1, previous_summary["days"] // 2)
        if candidate_summary["days"] < minimum_days:
            reasons.append(
                f"candidate dropped from {previous_summary['days']} days to "
                f"{candidate_summary['days']} days"
            )

    if previous_had_ships and previous_summary["months"] >= 2:
        minimum_months = max(1, previous_summary["months"] // 2)
        if candidate_summary["months"] < minimum_months:
            reasons.append(
                f"candidate dropped from {previous_summary['months']} months to "
                f"{candidate_summary['months']} months"
            )

    if previous_had_ships and previous_size >= 1000:
        minimum_size = max(500, previous_size // 4)
        if candidate_size < minimum_size:
            reasons.append(
                f"candidate file is suspiciously small: {candidate_size} bytes "
                f"vs previous {previous_size} bytes"
            )

    return len(reasons) == 0, reasons


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--previous", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--restore-on-reject", action="store_true")
    args = parser.parse_args()

    accepted, reasons = validate_candidate(args.previous, args.candidate)

    if accepted:
        print("Schedule validation accepted candidate schedule.json")
        return 0

    print("Schedule validation rejected candidate schedule.json:")
    for reason in reasons:
        print(f"- {reason}")

    if args.restore_on_reject:
        shutil.copyfile(args.previous, args.candidate)
        print(f"Restored previous schedule from {args.previous} to {args.candidate}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
