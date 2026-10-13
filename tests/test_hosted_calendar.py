import sys, unittest, copy
from pathlib import Path
from datetime import date
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from update_official_calendar import select_links, validate_document, scoped_vipa_links
from official_fetch import CollectionBlocked
from export_app_calendar import export_calendar
import test_official_collection as fixtures

class HostedCalendarTests(unittest.TestCase):
    def test_rollover_selects_current_fiscal_and_month(self):
        links=[{"url":"https://wico-ltd.com/FY-2027-SHIP-SCHEDULE-REV-4.pdf","label":""},
               {"url":"https://wico-ltd.com/FY-2028-SHIP-SCHEDULE-REV-1.pdf","label":""},
               {"url":"https://wico-ltd.com/October-2027-Ship-Schedule.pdf","label":""}]
        chosen=select_links("wico",links,date(2027,10,1))
        self.assertEqual(len(chosen),2)
        self.assertIn("2028",chosen[0][0]["url"])
    def test_highest_revision_and_ambiguity(self):
        a={"url":"https://wico-ltd.com/FY-2027-REV-1.pdf","label":""}
        b={"url":"https://wico-ltd.com/FY-2027-REV-2.pdf","label":""}
        self.assertEqual(select_links("wico",[a,b],date(2026,10,12))[0][0],b)
        with self.assertRaises(CollectionBlocked):
            select_links("wico",[b,dict(b,url=b["url"].replace(".pdf","-copy.pdf"))],date(2026,10,12))
    def test_missing_current_source_fails(self):
        with self.assertRaises(CollectionBlocked): select_links("wico",[],date(2026,10,12))
        with self.assertRaises(CollectionBlocked): select_links("vipa",[],date(2026,10,12))
    def test_layout_discrepancies_block_publication(self):
        doc={"calls":[{"localDate":"2026-10-12","island":"stThomas"}],"issues":[{"kind":"unmatched_possible_call"}]}
        with self.assertRaises(CollectionBlocked): validate_document(doc)
        doc["issues"]=[{"kind":"monthly_total_mismatch","month":"2026-10"}]
        validate_document(doc)
    def test_monthly_total_discrepancy_holds_whole_month(self):
        helper=fixtures.ParserTests();helper.setUp();self.addCleanup(helper.doCleanups)
        doc=helper.parse("wico_monthly")
        doc["issues"]=[{"kind":"monthly_total_mismatch","month":"2026-10"}]
        out=export_calendar([doc],{"schemaVersion":1,"records":[]},date(2026,10,1),date(2026,10,31))
        self.assertEqual(len(out["heldDays"]),31)

    def test_vipa_geography_comes_from_heading_not_ambiguous_link_label(self):
        html='<section><h2>Crown Bay, St. Thomas and St. John</h2><div><a href="/stt.pdf">Fiscal Year 2027</a></div></section><section><h2>St. Croix</h2><a href="/stx.pdf">Fiscal Year 2027</a></section>'
        self.assertEqual([x['url'] for x in scoped_vipa_links(html)],['https://www.viport.com/stt.pdf'])
        self.assertEqual(scoped_vipa_links('<a href="/unknown.pdf">Fiscal Year 2027</a>'),[])

    def test_saved_backoff_stops_before_any_network_and_keeps_previous_file(self):
        import tempfile,json
        from datetime import datetime,timedelta,timezone
        from unittest.mock import patch
        from update_official_calendar import collect
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);work=root/'work';work.mkdir()
            previous=work/'official-cruise-calendar-v1.json';previous.write_text('previous valid feed placeholder')
            state=root/'state.json';state.write_text(json.dumps({'providers':{'vipa':{'nextAllowedAt':(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat(),'lastHTTPStatus':429}}}))
            with patch('update_official_calendar.OfficialFetcher.fetch') as fetch:
                with self.assertRaises(CollectionBlocked):collect(work,state,date(2026,10,12))
                fetch.assert_not_called()
            self.assertEqual(previous.read_text(),'previous valid feed placeholder')
