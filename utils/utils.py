import json
import os
import psutil
import socket
from math import ceil

def sock_init(IP, PORT, MODE) -> socket.socket: 
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if MODE == 'm':
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    else:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind((IP, PORT))
    return sock

#find IP address of the current machine
def find_own_ip() -> int:
    s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        #a dummy connection to get Machine's local IP; it determines the 
        # "FROM IP address" in a connection request
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

def collect_resources(IP) -> dict:
    print("Collecting System Resources...")
    storage = int(input("Enter storage amount in GBs you would like to allocate for data storage: "))
    spec = {
        "worker_IP": IP,
        "cpus": psutil.cpu_count(logical=True),
        "ram": ceil(psutil.virtual_memory().total / (1073741824)),
        "storage": storage
    }
    return spec

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def estimate_file_size(path: str) -> int:
    return os.path.getsize(path)

'''
def generate_mpirun_command(nodes, script_path="broadcast.py") -> str:
    base = "/usr/bin/mpirun"
    parts = []

    for i, (user, ip) in enumerate(nodes):
        host = "localhost" if i == 0 else ip
        part = f"-np 1 --host {host} python3 /home/{user}/dmc/{script_path}"
        parts.append(part)

    joined = " : \\\n  ".join(parts)
    cmd = f"{base} \\\n  {joined}"
    return cmd
'''

if __name__ == "__main__":
    resources = collect_resources()
    with open("resources.json", "w") as json_file:  
        json.dump(resources, json_file, indent=4) 
    print("Resources saved to resources.json")
