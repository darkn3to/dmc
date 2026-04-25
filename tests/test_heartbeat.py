import json
import os
import tempfile
import unittest
from unittest.mock import patch

from dmc_fault.heartbeat import MasterNode, WorkerNode


class BreakLoop(Exception):
    pass


class FakeMasterSocket:
    def __init__(self, packets):
        self._packets = list(packets)
        self.bound = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def bind(self, addr):
        self.bound = addr

    def recvfrom(self, _):
        if not self._packets:
            raise BreakLoop()
        packet = self._packets.pop(0)
        if isinstance(packet, Exception):
            raise packet
        return packet


class FakeWorkerSocket:
    def __init__(self):
        self.sent = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def sendto(self, data, addr):
        self.sent.append((data, addr))


class HeartbeatTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.json_path = f"{self.tmp.name}/placement_map.json"
        self.log_path = f"{self.tmp.name}/heartbeats.log"
        self.current_master = None
        with open(self.json_path, "w") as f:
            json.dump({}, f)

    def tearDown(self):
        print(f"\n[STATE] {self.id()}")
        if self.current_master is not None:
            try:
                print(f"known_workers: {list(self.current_master.known_workers)}")
                print(f"worker_status: {dict(self.current_master.worker_status)}")
                print(f"heartbeats: {dict(self.current_master.heartbeats)}")
            except Exception as e:
                print(f"master_state_error: {e}")

        try:
            with open(self.json_path, "r") as f:
                print(f"placement_map_json: {json.load(f)}")
        except Exception as e:
            print(f"placement_map_json_error: {e}")

        if os.path.exists(self.log_path):
            try:
                with open(self.log_path, "r") as f:
                    print(f"heartbeat_log: {f.read().strip()}")
            except Exception as e:
                print(f"heartbeat_log_error: {e}")

        self.tmp.cleanup()

    def _new_master(self, known_workers=None, raw=None):
        if raw is not None:
            with open(self.json_path, "w") as f:
                json.dump(raw, f)
        master = MasterNode(
            "127.0.0.1",
            known_workers=known_workers or [],
            heartbeat_interval=1,
            min_heartbeat_threshold=2,
            json_file=self.json_path,
            log_file=self.log_path,
        )
        initial = master._load_initial_heartbeats()
        master.heartbeats.clear()
        master.heartbeats.update(initial)
        self.current_master = master
        self.addCleanup(master.manager.shutdown)
        return master

    def test_format_timestamp_returns_string(self):
        master = self._new_master()
        formatted = master._format_timestamp(1700000000)
        self.assertIsInstance(formatted, str)
        self.assertEqual(len(formatted), 19)

    def test_format_timestamp_round_trip(self):
        master = self._new_master()
        ts = 1700000000
        formatted = master._format_timestamp(ts)
        parsed = master._parse_timestamp(formatted)
        self.assertEqual(parsed, ts)

    def test_parse_timestamp_with_int(self):
        master = self._new_master()
        self.assertEqual(master._parse_timestamp(123456), 123456)

    def test_parse_timestamp_invalid_returns_none(self):
        master = self._new_master()
        self.assertIsNone(master._parse_timestamp("invalid-date"))

    def test_load_initial_heartbeats_adds_last_seen_and_default_replica(self):
        raw = {
            "10.0.0.1": {"primary": [1]},
            "10.0.0.2": {"primary": [3], "replica": [4]},
        }
        master = self._new_master(raw=raw)
        parsed = dict(master.heartbeats)
        self.assertIn("last_seen", parsed["10.0.0.1"])
        self.assertEqual(parsed["10.0.0.1"]["replica"], [])
        self.assertEqual(parsed["10.0.0.2"]["replica"], [4])

    def test_load_initial_heartbeats_invalid_json_returns_empty(self):
        with open(self.json_path, "w") as f:
            f.write("{bad-json")
        master = self._new_master()
        self.assertEqual(dict(master.heartbeats), {})

    def test_save_heartbeats_writes_timestamp_string(self):
        master = self._new_master()
        master.heartbeats["10.0.0.1"] = {
            "primary": [1],
            "replica": [2],
            "active_replicas": [],
            "last_seen": 1700000000,
        }
        master._save_heartbeats()
        with open(self.json_path, "r") as f:
            data = json.load(f)
        self.assertIsInstance(data["10.0.0.1"]["last_seen"], str)

    def test_save_heartbeats_preserves_primary_and_replica(self):
        master = self._new_master()
        master.heartbeats["10.0.0.1"] = {
            "primary": [1, 2],
            "replica": [3],
            "active_replicas": [],
            "last_seen": 1700000000,
        }
        master._save_heartbeats()
        with open(self.json_path, "r") as f:
            data = json.load(f)
        self.assertEqual(data["10.0.0.1"]["primary"], [1, 2])
        self.assertEqual(data["10.0.0.1"]["replica"], [3])

    def test_add_worker_creates_new_worker_with_primary_and_replica(self):
        master = self._new_master()
        master.add_worker("10.0.0.10", primary=[1, 2], replica=[3, 4])
        entry = dict(master.heartbeats)["10.0.0.10"]
        self.assertEqual(entry["primary"], [1, 2])
        self.assertEqual(entry["replica"], [3, 4])
        self.assertIn("10.0.0.10", list(master.known_workers))

    def test_add_worker_updates_existing_worker_shards(self):
        master = self._new_master(known_workers=["10.0.0.10"])
        master.add_worker("10.0.0.10", primary=[8], replica=[9])
        entry = dict(master.heartbeats)["10.0.0.10"]
        self.assertEqual(entry["primary"], [8])
        self.assertEqual(entry["replica"], [9])

    def test_remove_worker_promotes_matching_replica(self):
        raw = {
            "10.0.0.1": {"primary": [1, 2], "replica": [3]},
            "10.0.0.2": {"primary": [4], "replica": [1, 2, 5]},
        }
        master = self._new_master(known_workers=["10.0.0.1", "10.0.0.2"], raw=raw)
        master.remove_worker("10.0.0.1")
        remaining = dict(master.heartbeats)["10.0.0.2"]
        self.assertEqual(sorted(remaining["active_replicas"]), [1, 2])

    def test_remove_worker_unknown_worker_keeps_state(self):
        raw = {"10.0.0.2": {"primary": [4], "replica": [1]}}
        master = self._new_master(known_workers=["10.0.0.2"], raw=raw)
        before = dict(master.heartbeats)
        master.remove_worker("10.0.0.1")
        after = dict(master.heartbeats)
        self.assertEqual(before, after)

    def test_check_worker_status_marks_online(self):
        raw = {"10.0.0.2": {"primary": [4], "replica": [1], "last_seen": 100}}
        master = self._new_master(known_workers=["10.0.0.2"], raw=raw)
        with patch("dmc_fault.heartbeat.time.time", return_value=101), patch(
            "dmc_fault.heartbeat.time.sleep", side_effect=[None, BreakLoop()]
        ):
            with self.assertRaises(BreakLoop):
                master.check_worker_status()
        self.assertEqual(master.worker_status["10.0.0.2"], "online")

    def test_check_worker_status_removes_long_offline_worker(self):
        raw = {"10.0.0.2": {"primary": [4], "replica": [1], "last_seen": 1}}
        master = self._new_master(known_workers=["10.0.0.2"], raw=raw)
        with patch("dmc_fault.heartbeat.time.time", return_value=100), patch(
            "dmc_fault.heartbeat.time.sleep", side_effect=[None, BreakLoop()]
        ):
            with self.assertRaises(BreakLoop):
                master.check_worker_status()
        self.assertNotIn("10.0.0.2", list(master.known_workers))

    def test_receive_heartbeats_updates_known_worker_last_seen(self):
        master = self._new_master(known_workers=["10.0.0.9"], raw={"10.0.0.9": {"primary": [1], "replica": [2]}})
        fake_socket = FakeMasterSocket([
            (b"heartbeat", ("10.0.0.9", 3456)),
            BreakLoop(),
        ])
        with patch("dmc_fault.heartbeat.socket.socket", return_value=fake_socket), patch(
            "dmc_fault.heartbeat.time.time", return_value=500
        ):
            with self.assertRaises(BreakLoop):
                master.receive_heartbeats()
        self.assertEqual(dict(master.heartbeats)["10.0.0.9"]["last_seen"], 500)

    def test_receive_heartbeats_logs_unknown_worker(self):
        master = self._new_master(known_workers=[])
        fake_socket = FakeMasterSocket([
            (b"heartbeat", ("10.0.0.99", 3456)),
            BreakLoop(),
        ])
        with patch("dmc_fault.heartbeat.socket.socket", return_value=fake_socket):
            with self.assertRaises(BreakLoop):
                master.receive_heartbeats()
        with open(self.log_path, "r") as f:
            logs = f.read()
        self.assertIn("UNKNOWN worker 10.0.0.99", logs)

    def test_worker_send_heartbeats_sends_packet_to_master(self):
        worker = WorkerNode("10.0.0.1", heartbeat_interval=1)
        fake_socket = FakeWorkerSocket()
        with patch("dmc_fault.heartbeat.socket.socket", return_value=fake_socket), patch(
            "dmc_fault.heartbeat.time.sleep", side_effect=BreakLoop()
        ):
            with self.assertRaises(BreakLoop):
                worker.send_heartbeats()
        self.assertEqual(fake_socket.sent[0], (b"heartbeat", ("10.0.0.1", 9999)))

    def test_worker_send_heartbeats_uses_configured_interval(self):
        worker = WorkerNode("10.0.0.1", heartbeat_interval=7)
        fake_socket = FakeWorkerSocket()
        with patch("dmc_fault.heartbeat.socket.socket", return_value=fake_socket), patch(
            "dmc_fault.heartbeat.time.sleep", side_effect=BreakLoop()
        ) as mocked_sleep:
            with self.assertRaises(BreakLoop):
                worker.send_heartbeats()
        mocked_sleep.assert_called_with(7)


if __name__ == "__main__":
    unittest.main()
