import base64, copy, json, sys, unittest
from pathlib import Path
from datetime import date, datetime, timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import test_official_collection as fixtures
from export_app_calendar import export_calendar
from official_parse import canonical_vessel

class AppCalendarTests(unittest.TestCase):
    def setUp(self):
        self.parser=fixtures.ParserTests(); self.parser.setUp(); self.addCleanup(self.parser.doCleanups)
        self.docs=self.parser.documents()
        self.now=datetime(2026,10,13,tzinfo=timezone.utc)
        self.catalog={'schemaVersion':1,'records':[{'ship':self.docs[0]['calls'][0]['canonicalVessel'],
            'value':1234,'basis':'doubleOccupancy','status':'sourced','sourceURL':'https://www.ncl.com/synthetic',
            'checkedAt':'2026-10-12T20:00:00Z'}]}
    def export(self): return export_calendar(self.docs,self.catalog,date(2026,10,12),date(2026,11,30),self.now)
    def rows(self,value): return json.loads(base64.b64decode(value['payload']))
    def test_preserves_source_documents_and_never_claims_complete_coverage(self):
        value=self.export(); self.assertEqual(len(value['documents']),3)
        self.assertFalse(self.rows(value)['coverage']['complete'])
        self.assertNotIn('reviewedAt',value)
        self.assertTrue(value['conflictDays'])
    def test_capacities_join_only_the_correct_identity_and_basis(self):
        self.assertTrue(any(c['passengers']==1234 for d in self.rows(self.export())['days'] for c in d['ships']))
        self.catalog['records'][0]['basis']='maximum'
        self.assertTrue(all(c['passengers'] is None for d in self.rows(self.export())['days'] for c in d['ships']))
    def test_conflicting_capacity_is_not_guessed(self):
        self.catalog['records'][0]['status']='conflicting'
        self.assertTrue(all(c['passengers'] is None for d in self.rows(self.export())['days'] for c in d['ships']))
    def test_invalid_or_future_source_is_rejected(self):
        self.docs[0]['checkedAt']='2027-01-01T00:00:00Z'
        with self.assertRaises(ValueError): self.export()
        self.docs[0]['checkedAt']='2026-10-12T20:00:00Z';self.docs[0]['sha256']='bad'
        with self.assertRaises(ValueError): self.export()
    def test_quarantined_rows_and_unknown_dates_cannot_disappear(self):
        self.docs[0]['calls'][0]['flags']=['weekday_date_mismatch']
        self.assertIn(self.docs[0]['calls'][0]['localDate'],self.export()['heldDays'])
        self.docs[0]['calls'][0]['localDate']=None
        with self.assertRaises(ValueError): self.export()
    def test_duplicate_capacity_identity_is_rejected(self):
        self.catalog['records'].append(copy.deepcopy(self.catalog['records'][0]))
        with self.assertRaises(ValueError): self.export()

    def test_exact_aliases_share_identity_but_similar_ship_names_do_not(self):
        self.assertEqual(canonical_vessel('NCL Getaway','wico')[0],canonical_vessel('Norwegian Getaway','vipa')[0])
        self.assertEqual(canonical_vessel('Queen Mary II','wico')[0],canonical_vessel('Queen Mary 2','vipa')[0])
        self.assertEqual(canonical_vessel('Sea Dream 2','wico')[0],canonical_vessel('SeaDream II','vipa')[0])
        self.assertNotEqual(canonical_vessel('SeaDream I','wico')[0],canonical_vessel('SeaDream II','vipa')[0])
