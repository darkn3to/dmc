import socket
import time
import multiprocessing
import json
import os

log_file = "heartbeats.log"
json_file = "heartbeats.json"


class MasterNode:
    def __init__(
        self,
        known_workers: list = [],
        heartbeat_interval: int = 5,
        min_heartbeat_threshold: int = 15,
    ):
        self.manager = multiprocessing.Manager()
        self.known_workers = self.manager.list(known_workers)
        self.heartbeat_interval = heartbeat_interval
        self.min_heartbeat_threshold = min_heartbeat_threshold

        self.heartbeats = self.manager.dict()
        self.lock = self.manager.Lock()
        self.worker_status = self.manager.dict()
        
        initial_heartbeats = self._load_initial_heartbeats()
        self.heartbeats.update(initial_heartbeats)
        
        for worker_id in self.known_workers:
            self.worker_status[worker_id] = "unknown"

    def _load_initial_heartbeats(self):
        if os.path.exists(json_file):
            try:
                with open(json_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, ValueError):
                return {}
        return {}
    
    def add_worker(self, worker_id: str):
        with self.lock:
            if worker_id not in self.known_workers:
                self.known_workers.append(worker_id)
                self.worker_status[worker_id] = "unknown"
                print(f"++ New worker added: {worker_id}")
    
    def remove_worker(self, worker_id: str):
        with self.lock:
            if worker_id in self.known_workers:
                self.known_workers.remove(worker_id)
                self.worker_status.pop(worker_id, None)
                self.heartbeats.pop(worker_id, None)
                print(f"-- Worker removed: {worker_id}")
    

    def check_worker_status(self):
        remove_after = int(self.min_heartbeat_threshold * 1.5)
        while True:
            time.sleep(self.min_heartbeat_threshold)
            current_time = int(time.time())

            with self.lock:
                for worker_id in list(self.known_workers):
                    last_ts = self.heartbeats.get(worker_id)
                    current_status = self.worker_status.get(worker_id, "unknown")
                    
                    if last_ts is not None:
                        time_since = current_time - last_ts
                        if time_since > self.min_heartbeat_threshold:
                            if current_status != "offline":
                                print(
                                    f"!! Worker [{worker_id}] OFFLINE ({time_since}s ago)"
                                )
                                self.worker_status[worker_id] = "offline"

                            if time_since > remove_after:
                                self.known_workers.remove(worker_id)
                                self.worker_status.pop(worker_id, None)
                                self.heartbeats.pop(worker_id, None)
                                with open(json_file + ".tmp", "w") as f:
                                    json.dump(dict(self.heartbeats), f, indent=4)
                                os.replace(json_file + ".tmp", json_file)
                                print(
                                    f"-- Worker [{worker_id}] REMOVED after {time_since}s offline"
                                )
                        else:
                            if current_status != "online":
                                print(f"++ Worker [{worker_id}] is ONLINE")
                                self.worker_status[worker_id] = "online"
                    else:
                        if current_status != "never_seen":
                            print(f"?? Worker [{worker_id}] NEVER SEEN")
                            self.worker_status[worker_id] = "never_seen"

    def receive_heartbeats(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("", 9999))
            print("Master is listening on port 9999...")

            while True:
                data, addr = s.recvfrom(1024)
                worker_id = data.decode("utf-8")
                timestamp = int(time.time())

                if worker_id in self.known_workers:
                    msg = f"Heartbeat: {worker_id} from {addr[0]} at {timestamp}\n"

                    with self.lock:
                        self.heartbeats[worker_id] = timestamp
                        with open(json_file + ".tmp", "w") as f:
                            json.dump(dict(self.heartbeats), f, indent=4)
                        os.replace(json_file + ".tmp", json_file)
                else:
                    msg = f"UNKNOWN worker {worker_id} at {addr[0]}\n"

                print(msg.strip())
                with open(log_file, "a") as f:
                    f.write(msg)
    
    def start_host(self):
        t1 = multiprocessing.Process(target=self.receive_heartbeats, daemon=True)
        t2 = multiprocessing.Process(target=self.check_worker_status, daemon=True)
        t1.start()
        t2.start()


class WorkerNode:
    def __init__(self, master_ip: str, worker_id: str, heartbeat_interval: int = 5):
        self.master_ip = master_ip
        self.worker_id = worker_id
        self.heartbeat_interval = heartbeat_interval

    def send_heartbeats(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                s.sendto(self.worker_id.encode("utf-8"), (self.master_ip, 9999))
                time.sleep(self.heartbeat_interval)
