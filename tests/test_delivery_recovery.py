import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from scripts import check_feed_health as monitor
ALERTS = "https://www.viwapa.vi/news-information/advisory-details"


class CruiseIncidentRecoveryTests(unittest.TestCase):
    def test_unreadable_state_cannot_close_existing_source_failure(self):
        with patch.object(monitor, 'published_json', side_effect=[{}, OSError('temporary')]), \
                patch.object(monitor, 'problems', return_value=[]), \
                patch.object(monitor, 'source_times', return_value={}), \
                patch.object(monitor, 'alert', return_value='new_incident') as alert:
            self.assertEqual(monitor.run_check('cruise', 'example/test'), 1)
        self.assertEqual(alert.call_count, 1)
        self.assertEqual(alert.call_args.kwargs['title'], 'Cruise feed delivery unavailable')

    def test_read_recovery_does_not_announce_source_recovery(self):
        with patch.object(monitor, 'published_json', return_value={}), \
                patch.object(monitor, 'problems', return_value=['Source is stale']), \
                patch.object(monitor, 'source_times', return_value={}), \
                patch.object(monitor, 'cruise_state_problems', return_value=[]), \
                patch.object(monitor, 'alert', side_effect=['recovered', 'known_incident']) as alert:
            self.assertEqual(monitor.run_check('cruise', 'example/test'), 1)
        delivery, source = alert.call_args_list
        self.assertEqual(delivery.kwargs['title'], 'Cruise feed delivery unavailable')
        self.assertIn('does not close', delivery.kwargs['recovery_note'])
        self.assertEqual(source.kwargs['title'], monitor.TITLE)
        self.assertEqual(source.args[1], ['Source is stale'])

    def test_unchanged_transport_problem_is_still_unhealthy_without_repeat_notifications(self):
        output = io.StringIO()
        with patch.object(monitor, 'published_json', side_effect=OSError('temporary')), \
                patch.object(monitor, 'alert', return_value='known_incident'), redirect_stdout(output):
            self.assertEqual(monitor.run_check('cruise', 'example/test'), 0)
        self.assertIn('"healthy": false', output.getvalue())
        self.assertIn('"repeatNotificationSuppressed": true', output.getvalue())

class PublishedReadRecoveryTests(unittest.TestCase):
    def run_read(self, outcomes, limit=1000):
        opener = Mock()
        def respond(request, timeout):
            value = outcomes.pop(0)
            if isinstance(value, Exception):
                raise value
            return io.BytesIO(value)
        opener.open.side_effect = respond
        with patch.object(monitor.urllib.request, 'build_opener', return_value=opener), \
                patch.object(monitor.time, 'sleep') as sleep:
            result = monitor.published_json(monitor.CONFIG['outage'][0], limit)
        return result, opener, sleep

    def test_transient_failure_recovers_with_same_cache_bucket(self):
        result, opener, sleep = self.run_read([URLError('secret-token-do-not-log'), b'{', b'{"source":{}}'])
        self.assertEqual(result, {'source': {}})
        self.assertEqual(opener.open.call_count, 3)
        self.assertEqual(len({c.args[0].full_url for c in opener.open.call_args_list}), 1)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [1, 3])

    def test_retry_exhaustion_preserves_safe_diagnostics(self):
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(monitor.PublishedReadError) as caught:
            self.run_read([URLError('secret-token-do-not-log')] * 3)
        self.assertEqual(caught.exception.attempts, 3)
        self.assertNotIn('secret-token', output.getvalue())
        self.assertIn('network_or_incomplete_response', output.getvalue())

    def test_http_503_without_retry_after_can_recover(self):
        result, opener, _ = self.run_read([HTTPError('https://github.invalid', 503, 'temporary', {}, io.BytesIO()), b'{}'])
        self.assertEqual(result, {})
        self.assertEqual(opener.open.call_count, 2)

    def test_denial_rate_limit_and_retry_after_are_not_retried(self):
        for status, headers in [(401, {}), (403, {}), (429, {}), (503, {'Retry-After': '600'})]:
            with self.subTest(status=status), self.assertRaises(monitor.PublishedReadError) as caught:
                self.run_read([HTTPError('https://github.invalid', status, 'secret', headers, io.BytesIO())])
            self.assertEqual(caught.exception.attempts, 1)
            self.assertEqual(caught.exception.http_status, status)

    def test_redirect_size_and_wrong_envelope_are_not_retried(self):
        for value, limit, category in [(monitor.PublishedReadError('unexpected_redirect'), 1000, 'unexpected_redirect'),
                                       (b'xxx', 2, 'response_too_large'), (b'[]', 1000, 'invalid_json_envelope')]:
            with self.subTest(category=category), self.assertRaises(monitor.PublishedReadError) as caught:
                self.run_read([value], limit)
            self.assertEqual(caught.exception.category, category)

    def test_provider_urls_cannot_be_retried_by_delivery_helper(self):
        with patch.object(monitor.urllib.request, 'build_opener') as opener:
            with self.assertRaises(monitor.PublishedReadError):
                monitor.published_json(ALERTS, 1000)
        opener.assert_not_called()
