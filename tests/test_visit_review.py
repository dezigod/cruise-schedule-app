import base64
import copy
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from export_app_calendar import export_calendar
from official_parse import canonical_vessel, source_visit_status
from visit_review import review_visits
import test_official_collection as fixtures

class VisitReviewTests(unittest.TestCase):
    def setUp(self):
        helper=fixtures.ParserTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        self.doc=helper.parse('wico_fiscal')
        self.doc['calls']=[self.doc['calls'][0]]
        self.day=date.fromisoformat(self.doc['calls'][0]['localDate'])
        self.now=datetime(2026,10,13,tzinfo=timezone.utc)
        self.catalog={'schemaVersion':1,'records':[]}

    def export(self, docs): return export_calendar(docs,self.catalog,self.day,self.day,self.now)
    def rows(self, value): return [row for day in json.loads(base64.b64decode(value['payload']))['days'] for row in day['ships']]

    def test_exact_duplicates_merge_without_losing_either_reference(self):
        other=copy.deepcopy(self.doc); other['id']='second';other['url']='https://wico-ltd.com/second.pdf'
        value=self.export([self.doc,other])
        self.assertEqual(len(self.rows(value)),1)
        self.assertEqual(value['reviewSummary']['duplicateSourceRowsMerged'],1)
        self.assertEqual(len(value['visits'][0]['candidates']),2)
        self.assertEqual(self.rows(value)[0]['sourceDocumentIDs'],['second','wico_fiscal'])

    def test_real_port_berth_and_time_disagreements_are_not_resolved_by_majority(self):
        for field,replacement,reason in [('dock','CB','port_assignment_conflict'),('rawDock','AMNS','berth_assignment_conflict'),('departure','23:59','planned_time_conflict')]:
            with self.subTest(field=field):
                other=copy.deepcopy(self.doc);other['id']='disagree'
                other['calls'][0][field]=replacement
                if field=='rawDock':other['calls'][0]['dockCode']='AMNS'
                value=self.export([self.doc,copy.deepcopy(self.doc),other])
                self.assertIn(reason,value['visits'][0]['conflictReasons'])
                self.assertEqual(value['visits'][0]['visitStatus'],'conflicting')
                self.assertIn(self.day.isoformat(),value['conflictDays'])
                self.assertEqual(len(value['visits'][0]['candidates']),3)

    def test_source_cancelled_tentative_rescheduled_visits_are_preserved_but_not_active(self):
        for status in ['sourceCancelled','sourceTentative','sourceRescheduled']:
            with self.subTest(status=status):
                doc=copy.deepcopy(self.doc);doc['calls'][0]['visitStatus']=status
                value=self.export([doc])
                self.assertEqual(self.rows(value),[])
                self.assertEqual(value['visits'][0]['visitStatus'],status)
                self.assertIn(self.day.isoformat(),value['heldDays'])

    def test_disputed_cancellation_blocks_existing_client_forecast(self):
        other=copy.deepcopy(self.doc);other['calls'][0]['visitStatus']='sourceCancelled'
        value=self.export([self.doc,other])
        self.assertIn('visit_status_conflict',value['visits'][0]['conflictReasons'])
        self.assertEqual(self.rows(value)[0]['dockStatus'],'conflicting')
        self.assertIn(self.day.isoformat(),value['conflictDays'])

    def test_capacity_metadata_keeps_legacy_field_and_explicit_meaning(self):
        ship=self.doc['calls'][0]['canonicalVessel']
        self.catalog['records']=[{'ship':ship,'value':3260,'basis':'doubleOccupancy','status':'sourced','sourceURL':'https://www.celebritycruises.com/cruise-ships/example','checkedAt':'2026-10-12T20:00:00Z'}]
        row=self.rows(self.export([self.doc]))[0]
        self.assertEqual(row['passengers'],row['guestCapacity'])
        self.assertEqual(row['listingDetails']['capacityBasis'],'doubleOccupancy')
        self.assertEqual(row['capacityMeaning'],'doubleOccupancyCapacityNotAttendance')
        self.catalog['records'][0]['status']='conflicting'
        self.assertIsNone(self.rows(self.export([self.doc]))[0]['guestCapacity'])

    def test_status_suffix_does_not_create_a_different_ship(self):
        self.assertEqual(canonical_vessel('Example (CANCELLED)','wico')[0],canonical_vessel('Example','wico')[0])
        self.assertEqual(source_visit_status('Example CANCELLED'),'sourceCancelled')
        self.assertEqual(source_visit_status('Example TBA'),'sourceTentative')
        self.assertEqual(source_visit_status('Example RESCHEDULED'),'sourceRescheduled')

    def test_monthly_discrepancy_appears_on_affected_visit(self):
        self.doc['issues']=[{'kind':'monthly_total_mismatch','month':self.day.strftime('%Y-%m')}]
        value=self.export([self.doc])
        self.assertEqual(value['visits'][0]['visitStatus'],'needsReview')
        self.assertIn('monthly_total_mismatch',value['visits'][0]['reviewFlags'])

    def test_enrichment_cannot_reset_original_source_check_or_declare_complete_coverage(self):
        before=self.doc['checkedAt']
        value=self.export([self.doc])
        self.assertEqual(datetime.fromisoformat(value['documents'][0]['checkedAt'].replace('Z','+00:00')),datetime.fromisoformat(before))
        self.assertFalse(json.loads(base64.b64decode(value['payload']))['coverage']['complete'])

    def test_old_or_unsupported_capacities_are_not_added(self):
        ship=self.doc['calls'][0]['canonicalVessel']
        self.catalog['records']=[{'ship':ship,'value':3260,'basis':'doubleOccupancy','status':'sourced','sourceURL':'https://www.celebritycruises.com/cruise-ships/example','checkedAt':'2020-10-12T20:00:00Z'}]
        self.assertIsNone(self.rows(self.export([self.doc]))[0]['passengers'])
        self.catalog['records'][0]['checkedAt']='2026-10-12T20:00:00Z'
        self.catalog['records'][0]['sourceURL']='https://unreviewed.invalid/capacity'
        with self.assertRaises(ValueError):self.export([self.doc])

    def test_transport_cannot_outgrow_existing_iphone_limit(self):
        self.doc['calls'][0]['originalVesselName']='x'*3_000_000
        with self.assertRaisesRegex(ValueError,'transport limit'):self.export([self.doc])

    def test_ambiguous_or_negated_status_is_not_falsely_called_cancelled(self):
        self.assertEqual(source_visit_status('Example NOT CANCELLED'),'needsReview')
        self.assertEqual(source_visit_status('Example CANCELLED RESCHEDULED'),'needsReview')
