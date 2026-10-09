"""Non-finite JSON rejection at collector boundaries; no radio or sockets."""
import copy
import importlib.util
import json
from email.message import Message
from io import BytesIO
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('json_intake_dashboard',
    Path(__file__).parents[1] / 'tools/zmq_web_dashboard.py')
dashboard = importlib.util.module_from_spec(spec)
with patch.dict(sys.modules, {'zmq': types.ModuleType('zmq')}):
    spec.loader.exec_module(dashboard)

MAC = '12:34:56:78:9a:bc'
SENSOR = 'sensor-example'
TOKENS = ('NaN', 'Infinity', '-Infinity', '1e400', '-1e400')


def payload(value, token):
    return json.dumps(value).replace('"NONFINITE"', token).encode()


class JsonIntakeTests(unittest.TestCase):
    def setUp(self):
        dashboard.state = dashboard.DashboardState()
        dashboard._process_smp_message.state = dashboard.state
        dashboard.running = True

    def test_manual_gatt_rejects_nonfinite_unknown_fields_at_every_depth(self):
        dashboard._process_scan_message([b'scan:', json.dumps({'mac': MAC}).encode()])
        good = {'mac': MAC, 'services': [{'uuid': 'service',
                'characteristics': [{'uuid': 'characteristic', 'flags': []}]}]}
        for token in TOKENS:
            for level in ('result', 'service', 'characteristic'):
                with self.subTest(token=token, level=level):
                    value = copy.deepcopy(good)
                    target = value if level == 'result' else value['services'][0]
                    if level == 'characteristic':
                        target = target['characteristics'][0]
                    target['future'] = {'values': ['NONFINITE']}
                    dashboard._process_gatt_message([b'gatt:', payload(value, token)])
                    self.assertNotIn('gatt', dashboard.state.devices[MAC])
        dashboard._process_gatt_message([b'gatt:', json.dumps(good).encode()])
        self.assertEqual(dashboard.state.devices[MAC]['gatt'], good)
        json.dumps(dashboard.state.get_devices(), allow_nan=False)

    def test_scan_and_smp_reject_unknown_nonfinite_before_valid_messages(self):
        for token in TOKENS:
            dashboard._process_scan_message([b'scan:', payload(
                {'mac': MAC, 'future': {'values': ['NONFINITE']}}, token)])
            dashboard._process_smp_message([b'smp:', payload(
                {'event': 'future_event', 'future': {'values': ['NONFINITE']}}, token)])
            self.assertFalse(dashboard.state.devices)
            self.assertFalse(getattr(dashboard.state, 'smp_events', []))
        dashboard._process_scan_message([b'scan:', json.dumps({'mac': MAC}).encode()])
        dashboard._process_smp_message([b'smp:', b'{"event":"future_event"}'])
        self.assertIn(MAC, dashboard.state.devices)
        self.assertEqual(len(dashboard.state.smp_events), 1)
        json.dumps(dashboard.state.get_devices(), allow_nan=False)

    def test_router_rejects_nonfinite_and_still_drains_outbox(self):
        class Again(Exception):
            pass
        class Socket:
            def __init__(self):
                bad = [[SENSOR.encode(), payload({'type': 'heartbeat',
                    'sensor_id': SENSOR, 'future': {'values': ['NONFINITE']}}, token)]
                    for token in TOKENS]
                self.messages = iter(bad + [[SENSOR.encode(), json.dumps({
                    'type': 'heartbeat', 'sensor_id': SENSOR, 'sdr': 'stub'}).encode()]])
                self.received = 0
                self.sent_after = []
            def bind(self, *args): pass
            def close(self): pass
            def recv_multipart(self, *args):
                if self.received <= len(TOKENS):
                    self_test.assertNotIn(SENSOR, dashboard.state.c2_sensor_status)
                try:
                    value = next(self.messages)
                    self.received += 1
                    return value
                except StopIteration:
                    dashboard.running = False
                    raise Again()
            def send_multipart(self, value): self.sent_after.append(self.received)
        self_test = self
        socket = Socket()
        class Context:
            def socket(self, *args): return socket
            def term(self): pass
        class Poller:
            def register(self, *args): pass
            def poll(self, *args): return [(socket, 1)]
        zmq = types.SimpleNamespace(Context=Context, Poller=Poller, ROUTER=1,
                                    POLLIN=1, NOBLOCK=1, Again=Again, ZMQError=RuntimeError)
        dashboard.state.send_c2_command(SENSOR, 'get_status')
        with patch.object(dashboard, 'zmq', zmq):
            dashboard.control_router_thread(5555, None)
        self.assertEqual(socket.sent_after, [1])
        self.assertEqual(dashboard.state.c2_sensor_status[SENSOR]['sdr'], 'stub')
        json.dumps(dashboard.state.get_sensor_nodes(), allow_nan=False)

    def test_http_rejects_nested_nonfinite_without_queueing_command(self):
        for token in TOKENS:
            raw = payload({'sensor_id': SENSOR, 'future': {'values': ['NONFINITE']}}, token)
            handler = dashboard.DashboardHandler.__new__(dashboard.DashboardHandler)
            handler.path = '/api/c2/get_status'
            handler.headers = Message()
            handler.headers['Content-Type'] = 'application/json'
            handler.headers['Content-Length'] = str(len(raw))
            handler.rfile = BytesIO(raw)
            handler.wfile = BytesIO()
            handler.send_error = Mock()
            handler.send_response = Mock()
            handler.send_header = Mock()
            handler.end_headers = Mock()
            handler.do_POST()
            response = handler.send_error if handler.send_error.called else handler.send_response
            self.assertEqual(response.call_args.args[0], 400)
            self.assertTrue(dashboard.state.c2_outbox.empty())


if __name__ == '__main__':
    unittest.main()
