import socket
import json
import sys

ip = sys.argv[1]
config = json.loads(sys.argv[2])

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(json.dumps(config).encode(), (ip, 5005))

print(f"Sent config to {ip}")