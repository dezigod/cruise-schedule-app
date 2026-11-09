import base64, copy, json, sys, unittest
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from enrich_capacity_payload import enrich

class CapacityEnrichmentTests(unittest.TestCase):
    def test_only_capacity_fields_change_and_history_is_preserved(self):
        root=Path(__file__).resolve().parents[1]
        old=json.loads((root/'official-cruise-calendar-v1.json').read_text())
        catalog=json.loads((root/'data/ship-capacities.json').read_text())
        # Use only additions so older catalog expiration cannot affect this regression.
        catalog['records']=[r for r in catalog['records'] if r.get('effectiveFrom')]
        payload=json.loads(base64.b64decode(old['payload']))
        names={r['ship'].casefold() for r in catalog['records']}
        for day in payload['days']:
            for ship in day['ships']:
                if ship['name'].casefold() in names:
                    ship['passengers']=None
                    ship['guestCapacity']=None
        old['payload']=base64.b64encode(json.dumps(payload).encode()).decode()
        new=enrich(old,catalog,datetime(2026,11,9,tzinfo=timezone.utc))
        a=copy.deepcopy(old); b=copy.deepcopy(new)
        pa=json.loads(base64.b64decode(a.pop('payload')))
        pb=json.loads(base64.b64decode(b.pop('payload')))
        self.assertEqual(a,b)
        changed=0
        for da,db in zip(pa['days'],pb['days']):
            if da['date']<'2026-11-08': self.assertEqual(da,db)
            for sa,sb in zip(da['ships'],db['ships']):
                changed += sa != sb
                for row in (sa,sb):
                    for key in ('passengers','guestCapacity','capacityMeaning'): row.pop(key,None)
                    for key in ('capacityBasis','capacitySourceURL','capacityCheckedAt'): row.get('listingDetails',{}).pop(key,None)
        self.assertGreater(changed,0)
        self.assertEqual(pa,pb)
