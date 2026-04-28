import socket
import time
import multiprocessing
import json
import os
from datetime import datetime

class MasterNode:
    def __init__(
        self,
        master_ip: str,
        known_workers: list = [],
        heartbeat_interval: int = 5,
        min_heartbeat_threshold: int = 10,
        json_file: str = "broadcast/placement_map.json",
        log_file: str = "logs/heartbeats.log",
    ):
        self.master_ip = master_ip
        self.manager = multiprocessing.Manager()
        self.known_workers = self.manager.list(known_workers)
        self.heartbeat_interval = heartbeat_interval
        self.min_heartbeat_threshold = min_heartbeat_threshold
        self.json_file = json_file
        self.log_file = log_file

        self.heartbeats = self.manager.dict()
        self.lock = self.manager.Lock()
        self.worker_status = self.manager.dict()

        initial_heartbeats = self._load_initial_heartbeats()
        self.heartbeats.update(initial_heartbeats)

        for worker_ip in self.known_workers:
            self.worker_status[worker_ip] = "unknown"

    def _format_timestamp(self, ts: int) -> str:
        return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S")

    def _parse_timestamp(self, value):
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(datetime.strptime(value, "%d/%m/%Y %H:%M:%S").timestamp())
            except ValueError:
                return None
        return None

    def _save_heartbeats(self):
        serializable = {}
        for worker_ip, value in dict(self.heartbeats).items():
            value["last_seen"] = self._format_timestamp(value["last_seen"])
            serializable[worker_ip] = value
        with open(self.json_file + ".tmp", "w") as f:
            json.dump(serializable, f, indent=4)
        os.replace(self.json_file + ".tmp", self.json_file)

    def _load_initial_heartbeats(self):
        if os.path.exists(self.json_file):
            try:
                with open(self.json_file, "r") as f:
                    raw = json.load(f)
                if not isinstance(raw, dict):
                    return {}

                parsed = {}
                current_time = int(time.time())
                for worker_ip, value in raw.items():
                    if "replica" not in value:
                        value["replica"] = []
                    if "active_replicas" not in value:
                        value["active_replicas"] = []

                    if "last_seen" not in value:
                        value["last_seen"] = current_time
                    else:
                        value["last_seen"] = self._parse_timestamp(value["last_seen"])
                    
                    if value["last_seen"] is not None:
                        parsed[worker_ip] = value
                return parsed
            except (json.JSONDecodeError, ValueError):
                return {}
        return {}

    def add_worker(self, worker_ip: str, primary=None, replica=None):
        with self.lock:
            is_new_worker = worker_ip not in self.known_workers
            if is_new_worker:
                self.known_workers.append(worker_ip)
            self.worker_status[worker_ip] = "unknown"

            current_time = int(time.time())
            worker_entry = dict(self.heartbeats.get(worker_ip, {}))
            worker_entry["last_seen"] = worker_entry.get("last_seen", current_time)
            worker_entry["primary"] = list(primary) if primary is not None else []
            worker_entry["replica"] = list(replica) if replica is not None else []
            worker_entry["active_replicas"] = list(worker_entry.get("active_replicas", []))
            self.heartbeats[worker_ip] = worker_entry
            self._save_heartbeats()

            if is_new_worker:
                print(f"++ New worker added: {worker_ip}")
            else:
                print(f"++ Worker updated: {worker_ip}")

    def remove_worker(self, worker_ip: str):
        with self.lock:
            if worker_ip in self.known_workers:
                self.known_workers.remove(worker_ip)
                self.worker_status.pop(worker_ip, None)
                removed_entry = self.heartbeats.pop(worker_ip, None)
                
                removed_primary = removed_entry.get("primary", []) if removed_entry else []
                
                for remaining_worker_ip in list(self.known_workers):
                    remaining_entry = dict(self.heartbeats.get(remaining_worker_ip, {}))
                    if remaining_entry:
                        if "active_replicas" not in remaining_entry:
                            remaining_entry["active_replicas"] = []
                        
                        replica_list = remaining_entry.get("replica", [])
                        for shard in replica_list:
                            if shard in removed_primary and shard not in remaining_entry["active_replicas"]:
                                remaining_entry["active_replicas"].append(shard)
                        
                        self.heartbeats[remaining_worker_ip] = remaining_entry
                
                self._save_heartbeats()
                print(f"-- Worker removed: {worker_ip}. Primary shards {removed_primary} promoted to active replicas for remaining workers")
            else:
                print(f"Worker {worker_ip} not in known_workers")

    def check_worker_status(self):
        remove_after = int(self.min_heartbeat_threshold * 1.5)
        while True:
            time.sleep(self.min_heartbeat_threshold)
            current_time = int(time.time())

            with self.lock:
                for worker_ip in list(self.known_workers):
                    if worker_ip == self.master_ip:
                        continue
                    value = self.heartbeats.get(worker_ip)
                    last_ts = value["last_seen"] if value else None
                    current_status = self.worker_status.get(worker_ip, "unknown")

                    if last_ts is not None:
                        time_since = current_time - last_ts
                        if time_since > self.min_heartbeat_threshold:
                            if current_status != "offline":
                                print(
                                    f"!! Worker [{worker_ip}] OFFLINE ({time_since}s ago)"
                                )
                                self.worker_status[worker_ip] = "offline"

                            if time_since > remove_after:
                                self.known_workers.remove(worker_ip)
                                self.worker_status.pop(worker_ip, None)
                                self.heartbeats.pop(worker_ip, None)
                                self._save_heartbeats()
                                print(
                                    f"-- Worker [{worker_ip}] REMOVED after {time_since}s offline"
                                )
                        else:
                            if current_status != "online":
                                print(f"++ Worker [{worker_ip}] is ONLINE")
                                self.worker_status[worker_ip] = "online"
                    else:
                        if current_status != "never_seen":
                            print(f"?? Worker [{worker_ip}] NEVER SEEN")
                            self.worker_status[worker_ip] = "never_seen"

    def receive_heartbeats(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("", 9999))
            print("Master is listening on port 9999...")

            while True:
                data, addr = s.recvfrom(1024)
                worker_ip = addr[0]
                timestamp = int(time.time())

                if worker_ip in self.known_workers:
                    msg = f"Heartbeat: {worker_ip} at {timestamp}\n"

                    with self.lock:
                        worker_entry = dict(self.heartbeats.get(worker_ip, {}))
                        worker_entry["last_seen"] = timestamp
                        self.heartbeats[worker_ip] = worker_entry
                        self._save_heartbeats()
                else:
                    msg = f"UNKNOWN worker {worker_ip}\n"

                with open(self.log_file, "a") as f:
                    f.write(msg)

    def start_host(self):
        t1 = multiprocessing.Process(target=self.receive_heartbeats, daemon=True)
        t2 = multiprocessing.Process(target=self.check_worker_status, daemon=True)
        t1.start()
        t2.start()


class WorkerNode:
    def __init__(self, master_ip: str, heartbeat_interval: int = 5):
        self.master_ip = master_ip
        self.heartbeat_interval = heartbeat_interval

    def send_heartbeats(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                s.sendto(b"heartbeat", (self.master_ip, 9999))
                time.sleep(self.heartbeat_interval)
