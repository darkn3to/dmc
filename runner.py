import subprocess
import socket
import json
import threading
import os
import signal
import time
from dmc_fault.heartbeat import *

CONFIG_PORT = 5005
BUFFER_SIZE = 4096

current_proc = None
current_config = None


def start_training(config):
    global current_proc

    time.sleep(2)  # ensure all nodes got config

    cmd = [
        "python3", "-u", "-m", "torch.distributed.run",
        "--nnodes", str(config["nnodes"]),
        "--nproc_per_node", "1",
        "--node_rank", str(config["rank"]),
        "--master_addr", config["master_addr"],
        "--master_port", "29500",
        config["script"]
    ]

    print(f"[RUNNER] Starting training: {cmd}")

    current_proc = subprocess.Popen(cmd, preexec_fn=os.setsid)


def stop_training():
    global current_proc

    if current_proc and current_proc.poll() is None:
        print("[RUNNER] Stopping current training...")
        os.killpg(os.getpgid(current_proc.pid), signal.SIGTERM)
        current_proc.wait()
        print("[RUNNER] Training stopped.")

    current_proc = None


def config_listener():
    global current_config

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", CONFIG_PORT))

    print(f"[RUNNER] Listening for config on UDP {CONFIG_PORT}...")

    while True:
        data, _ = sock.recvfrom(BUFFER_SIZE)
        config = json.loads(data.decode())

        print(f"[RUNNER] Received config: {config}")

        if config != current_config:
            stop_training()
            current_config = config
            start_training(config)


def monitor_process():
    global current_proc

    while True:
        if current_proc:
            ret = current_proc.poll()
            if ret is not None:
                print(f"[RUNNER] Training exited with code {ret}")
                current_proc = None
        time.sleep(2)


if __name__ == "__main__":
    threading.Thread(target=config_listener, daemon=True).start()
    threading.Thread(target=monitor_process, daemon=True).start()
    if current_config:
        if str(current_config["rank"])=="0":
            heartbeat_receiver= MasterNode(master_ip=current_config["master_addr"], heartbeat_interval=5, min_heartbeat_threshold=10,json_file="broadcast/placement_map.json")
            threading.Thread(target=heartbeat_receiver.start_host, daemon=True).start()
        else:
            heartbeat_sender=WorkerNode(master_ip=current_config["master_addr"], heartbeat_interval=5)
            threading.Thread(target=heartbeat_sender.send_heartbeats, daemon=True).start()
    while True:
        time.sleep(10)