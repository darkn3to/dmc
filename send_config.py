import socket
import json
import sys
import time

ip = sys.argv[1]
config_path = sys.argv[2]

with open(config_path, "r") as f:
    config = json.load(f)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# 🔥 send multiple times to avoid packet loss
for _ in range(5):
    sock.sendto(json.dumps(config).encode(), (ip, 5005))
    time.sleep(0.5)

print(f"Sent config to {ip}")