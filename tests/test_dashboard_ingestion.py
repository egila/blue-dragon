#!/usr/bin/env python3
"""Receiver survival with mocked sockets; no live ZMQ or radio operations."""
import importlib.util
import json
from io import BytesIO
import time
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

try:
    import zmq
except ImportError:
    sys.modules['zmq'] = types.ModuleType('zmq')

spec = importlib.util.spec_from_file_location('ingestion_dashboard',
    Path(__file__).parents[1] / 'tools/zmq_web_dashboard.py')
dashboard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dashboard)
MAC = '12:34:56:78:9a:bc'
NESTED = b'[' * 64 + b'0' + b']' * 64
JSON_LOADS = json.loads


def depth_limited_loads(payload, **kwargs):
    # Simulate the decoder limit consistently across Python versions; the
    # real nesting depth that triggers RecursionError varies by interpreter.
    if payload == NESTED:
        raise RecursionError('decoder depth limit')
    return JSON_LOADS(payload, **kwargs)


def frames(topic, value):
    return [topic, b'sensor-example', json.dumps(value).encode()]


class FakeAgain(Exception):
    pass


class FakeSocket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.sent = []
        self.received = 0
        self.sent_after = []

    def setsockopt(self, *args):
        pass

    def bind(self, *args):
        pass

    def recv_multipart(self, *args):
        try:
            message = next(self.messages)
            self.received += 1
            return message
        except StopIteration:
            dashboard.running = False
            raise FakeAgain()

    def send_multipart(self, value):
        self.sent.append(value)
        self.sent_after.append(self.received)

    def close(self):
        pass


class FakeContext:
    def __init__(self, socket):
        self.sock = socket

    def socket(self, *args):
        return self.sock

    def term(self):
        pass


def fake_zmq(socket):
    class Poller:
        def register(self, *args):
            pass
        def poll(self, *args):
            return [(socket, 1)]
    return types.SimpleNamespace(Context=lambda: FakeContext(socket),
        SUB=1, ROUTER=2, SUBSCRIBE=3, RCVTIMEO=4, NOBLOCK=5, POLLIN=6,
        Again=FakeAgain, ZMQError=RuntimeError, Poller=Poller)


class IngestionTests(unittest.TestCase):
    def setUp(self):
        dashboard.state = dashboard.DashboardState()
        dashboard.running = True
        # Expose an upstream storage hook that production leaves unset.
        dashboard._process_smp_message.state = dashboard.state

    def test_sub_bad_frames_then_valid_scan_and_gatt(self):
        bad = [frames(topic, value) for topic in (b'gatt:', b'scan:', b'smp:')
               for value in (None, [], 4, 'bad', {'mac': []})]
        nested = NESTED
        bad += [[topic, b'sensor-example', nested] for topic in (b'gatt:', b'scan:', b'smp:')]
        bad += [[b'sensor-example', b'short packet']]
        bad += [frames(b'scan:', {'mac': MAC, 'manufacturer_data': {'1': [999]}}),
                frames(b'scan:', {'mac': MAC, 'appearance': []}),
                frames(b'scan:', {'mac': MAC, 'timestamp': float('nan')}),
                [b'smp:', b'\xff']]
        sock = FakeSocket(bad + [frames(b'scan:', {'mac': MAC, 'name': 'valid', 'rssi': -50}),
            frames(b'gatt:', {'mac': MAC, 'services': [], 'error': None})])
        with patch.object(dashboard, 'zmq', fake_zmq(sock)), \
                patch.object(dashboard.json, 'loads', side_effect=depth_limited_loads):
            dashboard.zmq_receiver(['stub'], None, BytesIO(), False)
        self.assertEqual(dashboard.state.devices[MAC]['name'], 'valid')
        self.assertEqual(dashboard.state.devices[MAC]['gatt']['services'], [])

    def test_router_drains_outbox_after_bad_input_then_accepts_heartbeat(self):
        sock = FakeSocket([[b'sensor-example', NESTED],
            [b'sensor-example', b'[]'],
            frames(b'sensor-example', {'type': 'heartbeat', 'sensor_id': [], 'gps': []}),
            frames(b'sensor-example', {'type': 'heartbeat', 'sensor_id': 'sensor-example',
                                      'sdr': 'stub', 'gps': [1.0, 2.0, 0.0]})])
        dashboard.state.send_c2_command('sensor-example', 'get_status')
        with patch.object(dashboard, 'zmq', fake_zmq(sock)), \
                patch.object(dashboard.json, 'loads', side_effect=depth_limited_loads):
            dashboard.control_router_thread(5555, None)
        self.assertEqual(len(sock.sent), 1)
        self.assertEqual(sock.sent_after, [1])
        self.assertEqual(json.loads(sock.sent[0][1])['cmd'], 'get_status')
        self.assertEqual(dashboard.state.get_sensor_nodes()[0]['sdr'], 'stub')

    def test_capture_io_failure_is_not_silently_discarded(self):
        sock = FakeSocket([[b'packet']])
        with patch.object(dashboard, 'zmq', fake_zmq(sock)), \
                patch.object(dashboard, '_process_zmq_message', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                dashboard.zmq_receiver(['stub'], None, BytesIO(), False)

    def test_scan_null_timestamp_and_extreme_numbers_leave_renderable_state(self):
        before = time.time()
        dashboard._process_scan_message(frames(b'scan:', {
            'mac': MAC, 'timestamp': None, 'rssi': -50, 'tx_power': 0}))
        device = dashboard.state.get_devices()[0]
        self.assertGreaterEqual(device['last'], before)
        self.assertLessEqual(device['last'], time.time())
        self.assertIsNotNone(device['est_dist'])
        for key, value in (('rssi', -1e300), ('tx_power', 1e300),
                           ('timestamp', 1e300), ('timestamp', -1)):
            dashboard._process_scan_message(frames(b'scan:', {
                'mac': MAC, 'name': 'rejected', key: value}))
        self.assertNotEqual(dashboard.state.get_devices()[0]['name'], 'rejected')
        dashboard.state.get_stats()
        dashboard.state.get_summary()

    def test_known_device_rejects_gatt_shapes_then_accepts_valid_result(self):
        dashboard._process_scan_message(frames(b'scan:', {'mac': MAC, 'rssi': -40}))
        bad_services = ({}, [None], [{'uuid': 7}],
            [{'uuid': 'service', 'characteristics': {}}],
            [{'uuid': 'service', 'characteristics': [None]}],
            [{'uuid': 'service', 'characteristics': [{'uuid': 7}]}],
            [{'uuid': 'service', 'characteristics': [
                {'uuid': 'characteristic', 'flags': 'write'}]}])
        for services in bad_services:
            with self.subTest(services=services):
                dashboard._process_gatt_message(frames(b'gatt:',
                    {'mac': MAC, 'services': services}))
                self.assertNotIn('gatt', dashboard.state.devices[MAC])
        good = {'mac': MAC, 'services': [{'uuid': 'service', 'characteristics': [
            {'uuid': 'characteristic', 'flags': ['read', 'write']}]}]}
        dashboard._process_gatt_message(frames(b'gatt:', good))
        self.assertEqual(dashboard.state.devices[MAC]['gatt'], good)

    def test_heartbeat_gps_ranges_reject_updates_then_accept_boundaries(self):
        for gps in ([90.1, 0], [-90.1, 0], [0, 180.1], [0, -180.1]):
            with self.subTest(gps=gps):
                dashboard._process_control_message(frames(b'sensor-example',
                    {'type': 'heartbeat', 'sensor_id': 'sensor-example', 'gps': gps}))
                self.assertNotIn('sensor-example', dashboard.state.c2_sensor_status)
        for gps in ([90, 180, 0], [-90, -180, 0]):
            dashboard._process_control_message(frames(b'sensor-example',
                {'type': 'heartbeat', 'sensor_id': 'sensor-example', 'gps': gps}))
            self.assertEqual(dashboard.state.c2_sensor_status['sensor-example']['gps'], gps)

    def test_smp_rejects_bad_addresses_then_accepts_valid_event(self):
        for address in ([], 1, None, 'invalid', '12:34:56:78:9a', '12:34:56:78:9a:zz'):
            with self.subTest(address=address):
                dashboard._process_smp_message(frames(b'smp:',
                    {'event': 'identity_address', 'addr': address}))
                self.assertFalse(getattr(dashboard.state, 'smp_events', []))
        event = {'event': 'identity_address', 'addr': MAC, 'addr_type': 'public'}
        dashboard._process_smp_message(frames(b'smp:', event))
        self.assertEqual(dashboard.state.smp_events, [event])


if __name__ == '__main__':
    unittest.main()
