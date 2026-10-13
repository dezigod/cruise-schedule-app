import copy
import hashlib
import io
import json
import sys
import tempfile
import types
import unittest
import urllib.error
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from official_fetch import CollectionBlocked, OfficialFetcher, OfficialRedirects, discover_pdfs, retry_deadline, validate_url
from official_parse import candidate_calendar, parse_pdf, reconcile
from review_official import candidate_import, main, source_diff, write_outputs

FIXTURES=json.loads((Path(__file__).parent/'fixtures/official-layouts.json').read_text())
NOW=datetime(2026,10,12,20,tzinfo=timezone.utc)
AUTH={'status':'owner_attested_not_provider_inspected','automaticCollection':True,
      'collectionEnabled':False,'cadenceAuthorized':False,'checkIntervalSeconds':None}

class SyntheticPage:
    def __init__(self,spec,rows=None):
        self.width=spec['width']; self.height=spec['height']; self.lines=[]
        self.text=spec['heading']+'\n'+'\n'.join(spec.get('footers',[])); self.words=[]
        contents=[(10,[(0,spec['heading'])])]
        for idx,row in enumerate(rows if rows is not None else spec['rows']):
            contents.append((50+idx*12,list(zip(spec['columns'],row))))
        for idx,footer in enumerate(spec.get('footers',[])):
            contents.append((500+idx*12,[(424,footer)]))
        for top,cols in contents:
            for x,text in cols:
                if text:self.words.append({'x0':x,'x1':x+max(len(text),1),'top':top,'bottom':top+7,'text':text})
    def extract_words(self):return self.words
    def extract_text(self):return self.text

class ParserTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
    def parse(self,profile,*,spec=None,pages=None):
        spec=copy.deepcopy(spec or FIXTURES[profile]); provider='vipa' if profile.startswith('vipa') else 'wico'
        path=Path(self.temp.name)/(profile+'.bin'); path.write_bytes(b'synthetic extraction stub, not a PDF')
        record={'id':profile,'provider':provider,'url':'https://www.viport.com/source.pdf' if provider=='vipa' else 'https://wico-ltd.com/source.pdf',
                'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'path':str(path),'obtainedAt':NOW.isoformat(),'checkedAt':NOW.isoformat()}
        pdf=Mock(); pdf.__enter__=Mock(return_value=pdf); pdf.__exit__=Mock(return_value=False)
        pdf.pages=pages or [SyntheticPage(spec)]; pdf.metadata={'CreationDate':'D:20261012000000'}
        with patch('pdfplumber.open',return_value=pdf):return parse_pdf(path,record,profile)
    def documents(self):return [self.parse(p) for p in ('wico_monthly','wico_fiscal','vipa_fiscal')]

    def test_actual_time_formats_and_private_row_provenance(self):
        doc=self.parse('wico_monthly'); call=doc['calls'][0]
        self.assertEqual((call['arrival'],call['departure']),('08:00','18:00'))
        self.assertEqual(call['provenance']['page'],1); self.assertIn('rawColumns',call['provenance']); self.assertFalse(doc['issues'])
    def test_spaced_other_island_dock_is_retained(self):
        c=self.parse('wico_monthly')['calls'][1]
        self.assertEqual((c['rawDock'],c['dockCode'],c['island']),('ST J','STJ','stJohn'))
    def test_merged_date_cells_continue_across_pages_with_reference(self):
        spec=copy.deepcopy(FIXTURES['wico_fiscal']); first=SyntheticPage(spec,rows=[spec['rows'][-1]])
        continuation=spec['rows'][-1].copy(); continuation[0]=''; continuation[1]=''; continuation[3]='Synthetic Continuation'
        doc=self.parse('wico_fiscal',pages=[first,SyntheticPage(spec,rows=[continuation])])
        self.assertEqual(doc['calls'][1]['localDate'],'2026-11-08'); self.assertEqual(doc['calls'][1]['provenance']['dateInheritedFrom']['page'],1)
    def test_unknown_dock_is_quarantined_without_inventing_island(self):
        spec=copy.deepcopy(FIXTURES['wico_monthly']); spec['rows'][0][1]='UNLISTED'
        doc=self.parse('wico_monthly',spec=spec); self.assertEqual(doc['calls'][0]['island'],'unknown')
        imported,quarantine=candidate_import([doc]); self.assertEqual(len(quarantine),1); self.assertEqual(len(imported['cruiseCalls']),3)
    def test_unknown_date_is_not_borrowed_from_prior_row(self):
        spec=copy.deepcopy(FIXTURES['wico_fiscal']); spec['rows'][1][0]='31-Feb-26'
        doc=self.parse('wico_fiscal',spec=spec); self.assertIsNone(doc['calls'][1]['localDate']); self.assertFalse(doc['structuralParsingComplete'])
    def test_weekday_errors_are_flagged(self):
        spec=copy.deepcopy(FIXTURES['wico_monthly']); spec['rows'][0][0]='MON 13'
        self.assertIn('weekday_date_mismatch',self.parse('wico_monthly',spec=spec)['calls'][0]['flags'])
    def test_unknown_time_is_retained_not_guessed(self):
        spec=copy.deepcopy(FIXTURES['vipa_fiscal']); spec['rows'][0][4]='6:00 AM'
        doc=self.parse('vipa_fiscal',spec=spec); self.assertIn('time_window_unresolved',doc['calls'][0]['flags'])
        self.assertEqual(candidate_import([doc])[1][0]['flags'],['time_window_unresolved'])
    def test_missing_capacity_stays_null(self):
        doc=self.parse('wico_fiscal'); self.assertTrue(all(c['guestCapacity'] is None for c in doc['calls']))
        self.assertTrue(all(c['guestCapacity'] is None for c in candidate_import([doc])[0]['cruiseCalls']))
    def test_pdf_and_download_times_never_become_publication(self):
        vipa=self.parse('vipa_fiscal'); self.assertIsNone(vipa['issueDate'])
        self.assertEqual(self.parse('wico_fiscal')['issueDate'],'2026-09-14')
    def test_day_only_issue_date_does_not_fabricate_midnight_publication(self):
        doc=self.parse('wico_fiscal'); imported,_=candidate_import([doc])
        self.assertEqual(doc['issueDate'],'2026-09-14'); self.assertEqual(doc['publicationTimePrecision'],'day')
        self.assertTrue(all(c['source']['issuedAt'] is None for c in imported['cruiseCalls']))
    def test_monthly_count_mismatch_blocks_parsing_completeness(self):
        spec=copy.deepcopy(FIXTURES['vipa_fiscal']); spec['footers'][0]='TOTAL PORT CALLS/SHIP DAYS FOR OCTOBER (03/02)'
        doc=self.parse('vipa_fiscal',spec=spec); self.assertIn('monthly_total_mismatch',[i['kind'] for i in doc['issues']]); self.assertFalse(doc['structuralParsingComplete'])
    def test_missing_months_are_reported_separately(self):
        doc=self.parse('vipa_fiscal'); self.assertEqual(len(doc['missingMonths']),10)
        rows,_=candidate_calendar([doc],date(2026,12,1),date(2026,12,1)); self.assertEqual(rows[0]['candidateKind'],'missing_coverage')
    def test_no_schedule_is_unknown_not_zero(self):
        rows,_=candidate_calendar([],date(2026,10,12),date(2026,10,12)); self.assertEqual(rows[0]['state'],'unknown'); self.assertIsNone(rows[0]['scheduledCallCount'])
    def test_october_12_is_only_candidate_empty_day(self):
        rows,_=candidate_calendar(self.documents(),date(2026,10,12),date(2026,10,12)); row=rows[0]
        self.assertEqual(row['candidateKind'],'empty_day_scope_review'); self.assertEqual(row['state'],'unknown'); self.assertIsNone(row['reviewedAt'])
        self.assertTrue(all(not p['completenessVerified'] for p in row['portCoverage'].values()))
    def test_october_13_preserves_calls(self):
        rows,_=candidate_calendar(self.documents(),date(2026,10,13),date(2026,10,13)); self.assertEqual(rows[0]['parsedUniqueStThomasCalls'],2)
    def test_october_15_and_19_other_island_does_not_prove_coverage(self):
        for day in (15,19):
            rows,_=candidate_calendar(self.documents(),date(2026,10,day),date(2026,10,day))
            self.assertIn('other_island_rows_do_not_establish_st_thomas_coverage',rows[0]['blockers']); self.assertIsNone(rows[0]['scheduledCallCount'])
    def test_adventure_conflict_preserves_both_docks(self):
        rows,res=candidate_calendar(self.documents(),date(2026,10,20),date(2026,10,20)); self.assertEqual(rows[0]['state'],'conflicting')
        call=next(r for r in res if r['canonicalVessel']=='adventure of the seas')
        self.assertEqual({c['rawDock'] for c in call['candidates']},{'WICO','AMNS'}); self.assertIn('berth_conflict',call['conflicts'])
    def test_duplicate_sources_do_not_double_count_calls(self):
        resolutions=reconcile(self.documents()); liner=next(r for r in resolutions if r['canonicalVessel']=='synthetic liner')
        self.assertEqual(liner['matchingDuplicateRows'],1); self.assertEqual(liner['sourceRowCount'],2)
    def test_cancellation_text_never_silently_removes_row(self):
        spec=copy.deepcopy(FIXTURES['wico_monthly']); spec['rows'][0][2]+=' CANCELLED'
        doc=self.parse('wico_monthly',spec=spec); self.assertEqual(len(doc['calls']),4); self.assertIn('explicit_flag_requires_review',doc['calls'][0]['flags'])
    def test_image_or_unrecognized_layout_is_flagged(self):
        spec=copy.deepcopy(FIXTURES['wico_monthly']); spec['width']=800
        self.assertFalse(self.parse('wico_monthly',spec=spec)['structuralParsingComplete'])
        page=SyntheticPage(FIXTURES['wico_monthly']); page.words=[]
        self.assertIn('image_or_empty_page',[i['kind'] for i in self.parse('wico_monthly',pages=[page])['issues']])
    def test_revision_diff_invalidates_review_and_reports_removal(self):
        doc=self.parse('wico_fiscal'); changed=copy.deepcopy(doc); changed['sha256']='new'; changed['calls'].pop()
        diff=source_diff([changed],[doc])[0]; self.assertTrue(diff['invalidatesPriorReview']); self.assertEqual(len(diff['removedOrChangedRows']),1)
    def test_candidate_import_reuses_ast_schema_with_no_approval(self):
        imported,_=candidate_import(self.documents()); call=imported['cruiseCalls'][0]
        self.assertEqual(imported['schemaVersion'],1); self.assertEqual(call['localDay'],'2026-10-13T04:00:00Z')
        self.assertEqual(call['source']['reviewState'],'candidate'); self.assertIsNone(call['source']['reviewerAlias']); self.assertEqual(call['source']['rightsScope'],'localReviewOnly')
    def test_private_outputs_never_claim_reviewed_zero(self):
        report=write_outputs(Path(self.temp.name)/'out',self.documents(),date(2026,10,12),date(2026,10,20))
        self.assertEqual(report['reviewedNoCallDays'],0); self.assertFalse(report['productionEnabled'])
        self.assertEqual(len((Path(self.temp.name)/'out/date-port-coverage.csv').read_text().splitlines()),28)
    def test_damaged_original_is_rejected(self):
        doc=self.parse('wico_fiscal'); doc['sha256']='bad'
        with self.assertRaises(ValueError):parse_pdf(Path(self.temp.name)/'wico_fiscal.bin',doc,'wico_fiscal')
    def test_offline_cli_makes_no_network_request(self):
        doc=self.parse('wico_fiscal'); root=Path(self.temp.name)
        doc['path']=str(root/'wico_fiscal.bin'); (root/'manifest.json').write_text(json.dumps([doc])); (root/'selection.json').write_text(json.dumps({'wico_fiscal':'wico_fiscal'}))
        with patch('review_official.parse_pdf',return_value=doc), patch('urllib.request.build_opener') as network, patch('sys.stdout',new=io.StringIO()):
            self.assertEqual(main(['--manifest',str(root/'manifest.json'),'--selection',str(root/'selection.json'),'--output',str(root/'out'),'--start','2026-10-12','--end','2026-10-20']),0)
        network.assert_not_called()
    def test_heading_with_column_fragments_cannot_become_arrival(self):
        page=SyntheticPage(FIXTURES['vipa_fiscal']); page.words.extend([{'x0':250,'x1':270,'top':25,'bottom':32,'text':'Ports'}, {'x0':332,'x1':350,'top':25,'bottom':32,'text':'2027'}, {'x0':496,'x1':520,'top':25,'bottom':32,'text':'Schedules'}])
        self.assertEqual(len(self.parse('vipa_fiscal',pages=[page])['calls']),3)
    def test_missing_anchorage_scope_keeps_future_date_unknown(self):
        docs=self.documents()
        for doc in docs:doc['portCodesSeen']=[]
        rows,_=candidate_calendar(docs,date(2026,11,12),date(2026,11,12))
        self.assertEqual(rows[0]['candidateKind'],'missing_coverage'); self.assertEqual(rows[0]['portCoverage']['stThomasAnchorages']['parsingStatus'],'missing')
    def test_other_island_only_unofficial_comparison_is_not_coverage(self):
        docs=self.documents()
        for doc in docs:doc['calls']=[c for c in doc['calls'] if c['localDate'] not in ('2026-10-15','2026-10-19')]
        feed={'days':[{'date':f'2026-10-{d}','ships':[{'name':'Synthetic Croix call','rawDock':'St. Croix','dock':'Unknown'}]} for d in (15,19)]}
        write_outputs(Path(self.temp.name)/'comparison',docs,date(2026,10,15),date(2026,10,19),comparison_feed=feed)
        rows=json.loads((Path(self.temp.name)/'comparison/candidate-calendar.json').read_text())
        for row in (rows[0],rows[-1]):
            self.assertEqual(row['candidateKind'],'other_island_only_scope_review'); self.assertFalse(row['unofficialComparison']['provesStThomasCoverage']); self.assertIsNone(row['scheduledCallCount'])

class FetchTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup); self.opener=Mock(); self.clock=NOW
        self.fetcher=OfficialFetcher(self.temp.name,AUTH,opener_factory=lambda _:self.opener,clock=lambda:self.clock)
        self.url='https://wico-ltd.com/source.pdf'
    def response(self,body=b'%PDF-synthetic',headers=None):
        response=Mock(status=200,url=self.url,headers=headers or {'Content-Type':'application/pdf','ETag':'version-one'})
        response.read.return_value=body; response.__enter__=Mock(return_value=response); response.__exit__=Mock(return_value=False)
        self.opener.open.return_value=response; return response
    def fetch(self,**kw):return self.fetcher.fetch('wico',self.url,'synthetic',one_shot=True,**kw)
    def test_recurring_collection_is_disabled_and_makes_no_request(self):
        with self.assertRaises(CollectionBlocked):self.fetcher.fetch('wico',self.url,'synthetic')
        self.opener.open.assert_not_called()
    def test_missing_attestation_blocks_one_shot(self):
        self.fetcher.authorization={}
        with self.assertRaises(CollectionBlocked):self.fetch()
        self.opener.open.assert_not_called()
    def test_unapproved_or_credential_url_is_blocked_before_request(self):
        for url in ('https://vinow.com/source.pdf','http://wico-ltd.com/source.pdf','https://secret@wico-ltd.com/source.pdf'):
            with self.assertRaises(CollectionBlocked):validate_url('wico',url)
        self.opener.open.assert_not_called()
    def test_redirect_to_unapproved_host_is_blocked(self):
        with self.assertRaises(CollectionBlocked):OfficialRedirects('wico').redirect_request(None,None,302,'',{},'https://unapproved.test/source.pdf')
    def test_discovery_selects_only_published_official_links(self):
        links=discover_pdfs('wico','<a href="/official.pdf">Current</a><a href="https://unapproved.test/other.pdf">Other</a>')
        self.assertEqual([l['url'] for l in links],['https://wico-ltd.com/official.pdf'])
    def test_pdf_and_cache_are_private_and_publication_unknown(self):
        self.response(); result=self.fetch()
        self.assertIsNone(result['sourceIssuedAt']); self.assertEqual(Path(result['path']).stat().st_mode&0o777,0o600)
        self.assertEqual(Path(self.temp.name).stat().st_mode&0o777,0o700)
    def test_conditional_get_keeps_original_obtainment(self):
        self.response(); first=self.fetch(); self.clock+=timedelta(hours=1)
        self.opener.open.side_effect=urllib.error.HTTPError(self.url,304,'not modified',{},None)
        second=self.fetch(); request=self.opener.open.call_args.args[0]
        self.assertEqual(request.get_header('If-none-match'),'version-one'); self.assertEqual(second['obtainedAt'],first['obtainedAt']); self.assertNotEqual(second['checkedAt'],first['checkedAt'])
    def test_304_with_corrupt_cache_is_rejected(self):
        self.response(); record=self.fetch(); Path(record['path']).write_bytes(b'corrupt')
        self.opener.open.side_effect=urllib.error.HTTPError(self.url,304,'not modified',{},None)
        with self.assertRaises(CollectionBlocked):self.fetch()
    def test_server_refusal_is_persistent_without_retry(self):
        self.opener.open.side_effect=urllib.error.HTTPError(self.url,403,'refused',{},None)
        with self.assertRaises(CollectionBlocked):self.fetch()
        with self.assertRaises(CollectionBlocked):self.fetch()
        self.opener.open.assert_called_once(); self.assertTrue(self.fetcher.cache['providers']['wico']['accessRefused'])
    def test_rate_limit_respects_server_delay(self):
        self.opener.open.side_effect=urllib.error.HTTPError(self.url,429,'limited',{'Retry-After':'86400'},None)
        with self.assertRaises(CollectionBlocked):self.fetch()
        self.assertEqual(datetime.fromisoformat(self.fetcher.cache['providers']['wico']['nextAllowedAt']),NOW+timedelta(days=1))
        with self.assertRaises(CollectionBlocked):self.fetch()
        self.opener.open.assert_called_once()
    def test_bad_format_does_not_replace_prior_original(self):
        self.response(); first=self.fetch(); self.response(body=b'not a PDF')
        with self.assertRaises(CollectionBlocked):self.fetch()
        self.assertEqual(self.fetcher.cache['documents'][self.url]['sha256'],first['sha256']); self.assertEqual(Path(first['path']).read_bytes(),b'%PDF-synthetic')
    def test_new_revision_keeps_old_original_and_requires_diff(self):
        self.response(); first=self.fetch(); self.response(body=b'%PDF-new-version'); second=self.fetch()
        self.assertEqual(second['supersedesSHA256'],first['sha256']); self.assertTrue(Path(first['path']).exists()); self.assertNotEqual(first['path'],second['path'])
    def test_access_challenge_stops_without_bypass(self):
        self.response(body=b'<html>cf-chl-challenge</html>',headers={'Content-Type':'text/html'})
        with self.assertRaises(CollectionBlocked):self.fetch(pdf=False)
        self.assertTrue(self.fetcher.cache['providers']['wico']['accessRefused'])
    def test_contact_form_captcha_library_is_not_an_access_refusal(self):
        self.response(body=b'<html><title>Published schedules</title><script src="captcha-contact-form.js"></script><a href="source.pdf">Schedule</a></html>',headers={'Content-Type':'text/html'})
        self.assertEqual(self.fetch(pdf=False)['status'],200); self.assertFalse(self.fetcher.cache['providers']['wico'].get('accessRefused',False))
    def test_server_http_date_backoff_is_not_shortened(self):
        self.assertEqual(retry_deadline('Tue, 13 Oct 2026 20:00:00 GMT',NOW,1),NOW+timedelta(days=1))
    def test_raw_outputs_cannot_be_inside_git_checkout(self):
        git=Path(self.temp.name)/'repo'; git.mkdir(); (git/'.git').mkdir()
        with self.assertRaises(CollectionBlocked):OfficialFetcher(git/'private',AUTH)
    def test_authorized_cadence_still_enforces_interval(self):
        self.fetcher.authorization={**AUTH,'collectionEnabled':True,'cadenceAuthorized':True,'checkIntervalSeconds':3600}
        self.response(); self.fetcher.fetch('wico',self.url,'synthetic')
        with self.assertRaises(CollectionBlocked):self.fetcher.fetch('wico',self.url,'synthetic')
        self.opener.open.assert_called_once()
    def test_intact_private_cache_seed_avoids_unconditional_fetch(self):
        self.response(); record=self.fetch(); other=OfficialFetcher(Path(self.temp.name)/'other',AUTH,opener_factory=lambda _:self.opener,clock=lambda:NOW)
        other.seed([record]); self.opener.open.side_effect=urllib.error.HTTPError(self.url,304,'not modified',{},None)
        result=other.fetch('wico',self.url,'synthetic',one_shot=True)
        self.assertEqual(result['sha256'],record['sha256']); self.assertEqual(result['obtainedAt'],record['obtainedAt'])

if __name__=='__main__':unittest.main()
