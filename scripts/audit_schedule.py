#!/usr/bin/env python3
"""Offline quality audit. Missing dates are unknown, never zero arrivals."""
from __future__ import annotations
import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PORTS = {
    "wico": ("St. Thomas", "WICO"), "havensight": ("St. Thomas", "WICO"),
    "cb": ("St. Thomas", "CB"), "crown bay": ("St. Thomas", "CB"),
    "amss": ("St. Thomas", "CB"), "amns": ("St. Thomas", "CB"), "amhp": ("St. Thomas", "CB"),
    "st. thomas": ("St. Thomas", None), "st. croix": ("St. Croix", None),
    "cruz bay": ("St. John", "Cruz Bay"), "stj": ("St. John", "Cruz Bay"),
    "st. john": ("St. John", None), "ianch": ("St. Thomas", None), "oanch": ("St. Thomas", None),
}

def key(value):
    return " ".join(str(value or "").lower().split())

def location(ship):
    candidates = [PORTS.get(key(ship.get(field))) for field in ("rawDock", "dock", "island")]
    candidates = [c for c in candidates if c]
    islands = {c[0] for c in candidates}
    docks = {c[1] for c in candidates if c[1]}
    conflict = len(islands) > 1 or len(docks) > 1
    return (next(iter(islands)) if len(islands) == 1 and not conflict else "Unknown", conflict)

def parse_timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except (AttributeError, TypeError, ValueError):
        return None

def quality_report(schedule, today=None, horizon=90):
    today = today or datetime.now(timezone.utc).date()
    groups = defaultdict(list)
    listed_st_thomas = set()
    issues = []
    islands = Counter()
    for day in schedule.get("days", []):
        if not isinstance(day, dict):
            continue
        for ship in day.get("ships", []):
            if not isinstance(ship, dict):
                continue
            label = f"{day.get('date')} {ship.get('name')}"
            island, conflict = location(ship)
            islands[island] += 1
            if island == "St. Thomas": listed_st_thomas.add(day.get("date"))
            if conflict: issues.append({"kind": "conflict", "call": label, "detail": "Source island/port fields disagree"})
            elif island == "Unknown": issues.append({"kind": "unknownIsland", "call": label})
            if ship.get("dock") in {None, "", "Unknown"}: issues.append({"kind": "unknownDock", "call": label})
            capacity = ship.get("passengers")
            if type(capacity) is not int or capacity <= 0: issues.append({"kind": "missingCapacity", "call": label})
            groups[(day.get("date"), key(ship.get("name")))].append(ship)
    for (day, name), ships in groups.items():
        if len(ships) > 1:
            variants = {(location(s)[0], key(s.get("rawDock") or s.get("dock")), s.get("arrival"), s.get("departure"), s.get("passengers")) for s in ships}
            issues.append({"kind": "conflict" if len(variants) > 1 else "duplicate", "call": f"{day} {name}"})
    now = datetime.combine(today, datetime.min.time(), timezone.utc)
    for field in ("lastUpdated", "sourcePublishedAt"):
        timestamp = parse_timestamp(schedule.get(field))
        if timestamp is None: issues.append({"kind": "unknownTimestamp", "field": field})
        elif timestamp > now + timedelta(days=1): issues.append({"kind": "futureTimestamp", "field": field})
        elif now - timestamp > timedelta(days=7): issues.append({"kind": "stale", "field": field})
    expected = [(today + timedelta(days=i)).isoformat() for i in range(horizon)]
    return {"listedDates": len(schedule.get("days", [])), "shipCalls": sum(islands.values()),
            "callsByIsland": dict(islands), "missingStThomasDates": [d for d in expected if d not in listed_st_thomas],
            "coverageMeaning": "Missing St. Thomas dates have unknown coverage, not zero ships.",
            "issues": issues, "authority": "Unverified unless independently reviewed against the original official source."}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--today", type=date.fromisoformat)
    parser.add_argument("--horizon", type=int, default=90)
    args = parser.parse_args()
    print(json.dumps(quality_report(json.loads(args.path.read_text()), args.today, args.horizon), indent=2))

if __name__ == "__main__": main()
