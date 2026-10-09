"""HTTP control validation with command dispatch stubbed; no radio actions."""
import importlib.util
import json
from email.message import Message
from io import BytesIO
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    'request_dashboard', Path(__file__).parents[1] / 'tools/zmq_web_dashboard.py')
dashboard = importlib.util.module_from_spec(spec)
with mock.patch.dict(sys.modules, {'zmq': types.ModuleType('zmq')}):
    spec.loader.exec_module(dashboard)

MAC = '12:34:56:78:9a:bc'
SENSOR = 'sensor-example'
UUID = '00002a00-0000-1000-8000-00805f9b34fb'
ROUTES = {
    '/api/watch': {'mac': MAC, 'watch': True},
    '/api/gatt/query': {'mac': MAC},
    '/api/gatt/write': {'mac': MAC, 'char_uuid': UUID, 'data': '01 02'},
    '/api/c2/set_gain': {'sensor_id': SENSOR, 'gain': 20},
    '/api/c2/set_squelch': {'sensor_id': SENSOR, 'threshold': -40},
    '/api/c2/restart': {'sensor_id': SENSOR, 'center_freq': 2440, 'channels': 40},
    '/api/c2/get_status': {'sensor_id': SENSOR},
    '/api/c2/spoof_adv': {'sensor_id': SENSOR, 'duration': 1, 'name': 'stub'},
    '/api/c2/l2cap_flood': {'sensor_id': SENSOR, 'mac': MAC, 'count': 1, 'hold_secs': 1, 'psm': 0},
}


class RequestTests(unittest.TestCase):
    def setUp(self):
        self.state = dashboard.DashboardState()
        self.state.c2_sensor_status[SENSOR] = {}
        self.state.device_sensor_rssi[MAC] = {SENSOR: {}}
        self.state.alert_mgr = types.SimpleNamespace(watch_set=set())
        self.state.send_c2_command = mock.Mock()
        self.patch = mock.patch.object(dashboard, 'state', self.state)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def request(self, route, body, headers=None, raw=None):
        handler = dashboard.DashboardHandler.__new__(dashboard.DashboardHandler)
        payload = json.dumps(body).encode() if raw is None else raw
        handler.path = route
        handler.headers = Message()
        combined = {'Content-Type': 'application/json', 'Content-Length': str(len(payload))}
        combined.update(headers or {})
        for key, value in combined.items():
            if value is not None:
                handler.headers[key] = value
        handler.rfile = BytesIO(payload)
        handler.send_error = mock.Mock()
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        handler.wfile = BytesIO()
        handler.do_POST()
        if handler.send_error.called:
            return handler.send_error.call_args.args[0]
        self.assertTrue(handler.send_response.called)
        status = handler.send_response.call_args.args[0]
        handler.send_header.assert_any_call("Content-Type", "application/json")
        response = json.loads(handler.wfile.getvalue())
        self.last_response = response
        if status == 400:
            self.assertIs(response["ok"], False)
            self.assertIsInstance(response["error"], str)
            self.assertTrue(response["error"])
        return status

    def test_every_route_accepts_object_with_json_charset(self):
        for route, body in ROUTES.items():
            with self.subTest(route=route):
                self.assertEqual(self.request(route, body, {'Content-Type': 'application/json; charset=utf-8'}), 200)
        self.assertEqual(self.state.send_c2_command.call_count, len(ROUTES) - 1)
        self.assertIn(MAC, self.state.alert_mgr.watch_set)

    def test_every_route_rejects_malformed_nonobject_or_incomplete_body(self):
        for route in ROUTES:
            for raw in (b'{', b'[]', b'null', b'4', b'"string"', b'', b'\xff', b'[' * 1200):
                with self.subTest(route=route, raw=raw[:20]):
                    self.assertEqual(self.request(route, None, raw=raw), 400)
            self.assertEqual(self.request(route, {}, {'Content-Length': 'bogus'}), 400)
            self.assertEqual(self.request(route, {}, {'Content-Length': '100'}), 400)
            self.assertEqual(self.request(route, {}, {'Content-Length': '65537'}), 413)
        self.state.send_c2_command.assert_not_called()
        self.assertEqual(self.state.alert_mgr.watch_set, set())

    def test_numeric_fields_reject_shapes_nonfinite_fractional_and_range(self):
        fields = [('/api/c2/set_gain', 'gain', -1, False),
                  ('/api/c2/set_gain', 'lna', 41, True),
                  ('/api/c2/set_gain', 'vga', 63, True),
                  ('/api/c2/set_squelch', 'threshold', -101, False),
                  ('/api/c2/restart', 'center_freq', 2600, True),
                  ('/api/c2/restart', 'channels', 97, True),
                  ('/api/c2/spoof_adv', 'duration', -1, True),
                  ('/api/c2/l2cap_flood', 'count', 2**32, True),
                  ('/api/c2/l2cap_flood', 'hold_secs', -1, True),
                  ('/api/c2/l2cap_flood', 'psm', 65536, True)]
        for route, field, outside, integer in fields:
            invalid = [None, [], {}, True, 'bad', float('nan'), float('inf'), outside]
            if integer:
                invalid.append(1.5)
            for value in invalid:
                with self.subTest(route=route, field=field, value=value):
                    self.assertEqual(self.request(route, {**ROUTES[route], field: value}), 400)
        self.state.send_c2_command.assert_not_called()

    def test_invalid_string_boolean_address_and_write_fields(self):
        cases = [('/api/watch', 'mac', []), ('/api/watch', 'watch', 'false'),
                 ('/api/gatt/query', 'mac', '<img>'),
                 ('/api/gatt/write', 'mac', []), ('/api/gatt/write', 'char_uuid', []),
                 ('/api/gatt/write', 'service_uuid', []), ('/api/gatt/write', 'data', []),
                 ('/api/gatt/write', 'data', 'zz'), ('/api/c2/get_status', 'sensor_id', []),
                 ('/api/c2/spoof_adv', 'name', []), ('/api/c2/spoof_adv', 'connectable', 'yes'),
                 ('/api/c2/spoof_adv', 'service_uuids', [1]),
                 ('/api/c2/l2cap_flood', 'mac', 1)]
        for route, field, value in cases:
            with self.subTest(route=route, field=field):
                self.assertEqual(self.request(route, {**ROUTES[route], field: value}), 400)
        self.state.send_c2_command.assert_not_called()

    def test_field_errors_identify_field_and_range_without_internal_details(self):
        cases = [('/api/c2/set_gain', 'gain', 200, 'gain out of range (0 to 100)'),
                 ('/api/c2/set_gain', 'lna', 1.5, 'lna must be an integer'),
                 ('/api/c2/set_squelch', 'threshold', 'inf', 'threshold out of range (-100 to -5)'),
                 ('/api/c2/restart', 'center_freq', None, 'center_freq must be a number'),
                 ('/api/c2/l2cap_flood', 'psm', 70000, 'psm out of range (0 to 65535)'),
                 ('/api/c2/spoof_adv', 'name', 5, 'name must be a string'),
                 ('/api/c2/spoof_adv', 'service_uuids', [1], 'service_uuids must be a list of strings'),
                 ('/api/watch', 'watch', 'false', 'watch must be a boolean')]
        for route, field, value, message in cases:
            with self.subTest(field=field):
                self.assertEqual(self.request(route, {**ROUTES[route], field: value}), 400)
                self.assertEqual(self.last_response['error'], message)
        self.state.send_c2_command.assert_not_called()
        for error in (TypeError('internal detail'), ValueError('internal detail')):
            with mock.patch.object(self.state, 'send_c2_command', side_effect=error):
                self.assertEqual(self.request('/api/c2/get_status', {'sensor_id': SENSOR}), 400)
                self.assertNotIn('internal detail', self.last_response['error'])

    def test_header_absent_lan_json_and_rewritten_proxy_host(self):
        self.assertEqual(self.request('/api/c2/get_status', ROUTES['/api/c2/get_status'],
            {'Host': '127.0.0.1:8099', 'Origin': 'https://dashboard.example'}), 200)
        self.state.send_c2_command.assert_called_once_with(SENSOR, 'get_status')

    def test_simple_cross_origin_posts_cannot_dispatch_without_fetch_metadata(self):
        for route, body in ROUTES.items():
            for content_type in (None, 'text/plain', 'application/x-www-form-urlencoded', 'multipart/form-data; boundary=x'):
                self.assertEqual(self.request(route, body, {'Content-Type': content_type,
                    'Origin': 'http://other.example'}), 415)
        self.state.send_c2_command.assert_not_called()
        self.assertEqual(self.state.alert_mgr.watch_set, set())

    def test_fetch_metadata_is_supplemental(self):
        for site in ('cross-site', 'same-site'):
            self.assertEqual(self.request('/api/c2/get_status', ROUTES['/api/c2/get_status'],
                {'Sec-Fetch-Site': site}), 403)
        self.state.send_c2_command.assert_not_called()
        for site in ('same-origin', 'none'):
            self.assertEqual(self.request('/api/c2/get_status', ROUTES['/api/c2/get_status'],
                {'Sec-Fetch-Site': site}), 200)


if __name__ == '__main__':
    unittest.main()
