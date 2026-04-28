import subprocess
import socket
import json
import threading
import os
import signal
import time
import shlex
import utils

CONFIG_PORT = 5005
BUFFER_SIZE = 4096
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

current_proc = None
current_config = None


def infer_socket_ifname(master_addr):
    try:
        cmd = f"ip route get {shlex.quote(master_addr)}"
        output = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return None

    tokens = output.strip().split()
    if "dev" in tokens:
        idx = tokens.index("dev")
        if idx + 1 < len(tokens):
            return tokens[idx + 1]
    return None


def start_training(config):
    global current_proc

    time.sleep(2)  # ensure all nodes got config

    script_path = config["script"]
    if not os.path.isabs(script_path):
        script_path = os.path.join(BASE_DIR, script_path)
    script_path = os.path.abspath(script_path)

    if not os.path.exists(script_path):
        print(f"[RUNNER] Script not found: {script_path}")
        return

    cmd = [
        "python3", "-u", "-m", "torch.distributed.run",
        "--nnodes", str(config["nnodes"]),
        "--nproc_per_node", "1",
        "--node_rank", str(config["rank"]),
        "--master_addr", config["master_addr"],
        "--master_port", "29500",
        script_path
    ]

    print(f"[RUNNER] Starting training: {cmd}")

    env = os.environ.copy()
    env["MASTER_ADDR"] = config["master_addr"]
    env["MASTER_PORT"] = "29500"
    env["NCCL_IB_DISABLE"] = "1"
    env["NCCL_DEBUG"] = "WARN"
    env["NCCL_DEBUG_SUBSYS"] = "ALL"
    env["NCCL_P2P_DISABLE"] = "1"

    if utils.find_own_ip() == config["master_addr"]:
        ifname = config.get("master_ifname")
    else:    
        ifname = infer_socket_ifname(config["master_addr"]) 
    if ifname:
        env.setdefault("NCCL_SOCKET_IFNAME", ifname)     
        env.setdefault("GLOO_SOCKET_IFNAME", ifname)
        print(f"[RUNNER] Using interface {ifname} ...")
    else:
        print("[RUNNER] Could not infer interface; using default interface selection")

    current_proc = subprocess.Popen(cmd, preexec_fn=os.setsid, env=env, cwd=BASE_DIR)


def stop_training():
    global current_proc

    if current_proc and current_proc.poll() is None:
        print("[RUNNER] Stopping current training...")
        os.killpg(os.getpgid(current_proc.pid), signal.SIGTERM)
        current_proc.wait()
        print("[RUNNER] Training stopped.")

    os.system("pkill -f torch.distributed.run || true")
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

    while True:
        time.sleep(10)