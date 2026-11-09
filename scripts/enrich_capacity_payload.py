"""Reapply reviewed capacity catalog without changing source freshness or schedule facts.
This is a local maintenance operation, not a source collection or publication command.
"""
import base64
import copy
import json
from datetime import datetime, timezone
from export_app_calendar import capacity_for_date


def enrich(calendar, catalog, now=None):
    now = now or datetime.now(timezone.utc)
    if calendar.get('schemaVersion') != 1 or catalog.get('schemaVersion') != 1:
        raise ValueError('Unsupported schema')
    records = {r['ship'].strip().casefold(): r for r in catalog['records']}
    if len(records) != len(catalog['records']):
        raise ValueError('Duplicate capacity identity')
    result = copy.deepcopy(calendar)
    payload = json.loads(base64.b64decode(result['payload'], validate=True))
    for day in payload['days']:
        for ship in day['ships']:
            record = records.get(ship['name'].strip().casefold())
            if record is None:
                continue
            # Do not apply newly researched present-day configuration to history.
            if record.get('effectiveFrom') and day['date'] < record['effectiveFrom']:
                continue
            value = capacity_for_date(record, day['date'], now)
            ship.update(passengers=value, guestCapacity=value,
                        capacityMeaning='doubleOccupancyCapacityNotAttendance')
            details = ship.setdefault('listingDetails', {})
            for field, key in [('capacityBasis','basis'), ('capacitySourceURL','sourceURL'),
                               ('capacityCheckedAt','checkedAt')]:
                details[field] = record[key] if value is not None else None
    result['payload'] = base64.b64encode(json.dumps(payload).encode()).decode()
    if len((json.dumps(result, indent=2)+'\n').encode()) > 3_000_000:
        raise ValueError('Enriched feed exceeds client transport limit')
    return result
