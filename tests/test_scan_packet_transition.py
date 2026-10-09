"""Scan-created devices must accept later SDR packets without losing side effects."""
from contextlib import ExitStack
import importlib.util
import json
from pathlib import Path
import struct
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('scan_packet_dashboard',
    Path(__file__).parents[1] / 'tools/zmq_web_dashboard.py')
dashboard = importlib.util.module_from_spec(spec)
with patch.dict(sys.modules, {'zmq': types.ModuleType('zmq')}):
    spec.loader.exec_module(dashboard)

MAC = '12:34:56:78:9a:bc'
SENSOR = 'sensor-example'


def advertising_record(second, rssi):
    flags = dashboard.LE_DEWHITENED | dashboard.LE_SIGNAL_POWER_VALID
    flags |= dashboard.LE_CRC_CHECKED | dashboard.LE_CRC_VALID
    address = bytes.fromhex(MAC.replace(':', ''))[::-1]
    pdu = struct.pack('<I', dashboard.BLE_ADV_AA) + b'\x00\x06' + address
    payload = dashboard.BLE_RF_HDR.pack(0, rssi, -100, 0,
                                         dashboard.BLE_ADV_AA, flags) + pdu
    return b'\x00' + dashboard.PCAP_REC_HDR.pack(second, 0, len(payload), len(payload)) + payload


class ScanPacketTransitionTests(unittest.TestCase):
    def test_scan_then_sdr_packets_reach_rssi_database_and_alerts(self):
        dashboard.state = state = dashboard.DashboardState()
        # An actual temporary database catches failures before persistence;
        # the alert manager has no shell command or webhook configured.
        with ExitStack() as cleanup:
            tmp = cleanup.enter_context(tempfile.TemporaryDirectory())
            state.db = dashboard.DeviceDB(Path(tmp) / 'devices.db')
            cleanup.callback(state.db.close)
            state.alert_mgr = dashboard.AlertManager(cooldown=0)
            state.alert_mgr.watch_set.add(MAC)
            dashboard._process_scan_message([b'scan:', SENSOR.encode(), json.dumps({
                'mac': MAC, 'name': 'scan-first', 'rssi': -60,
                'timestamp': 1000.0}).encode()])
            for second, rssi in ((1001, -50), (1002, -40)):
                dashboard._process_zmq_message(
                    [SENSOR.encode(), advertising_record(second, rssi)], None, False)
            device = state.devices[MAC]
            self.assertEqual(device['name'], 'scan-first')
            self.assertEqual(device['pkts'], 2)
            self.assertEqual(device['chans'], {0: 2})
            self.assertEqual((device['hec_ok'], device['ac_clean'],
                              device['uap_verified']), (0, 0, False))
            self.assertEqual(state.device_sensor_rssi[MAC][SENSOR],
                {'rssi_sum': -90, 'rssi_cnt': 2, 'last': 1002.0})
            self.assertEqual(state.db.conn.execute(
                'SELECT dev_key, total_pkts, best_rssi, crc_ok FROM devices').fetchall(),
                [(MAC, 2, -40, 2)])
            self.assertEqual(state.db.conn.execute(
                'SELECT dev_key, channel, pkts FROM device_channels').fetchall(),
                [(MAC, 0, 2)])
            alerts = state.alert_mgr.get_pending_alerts()
            self.assertEqual([(a['mac'], a['reason']) for a in alerts],
                             [(MAC, 'watchlist'), (MAC, 'watchlist')])
            json.dumps(state.get_devices(), allow_nan=False)


if __name__ == '__main__':
    unittest.main()
