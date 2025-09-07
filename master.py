import socket
import threading
import time

PORT = 50000
BUFFER_SIZE = 1024

#setup a UDP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#listens to requests on all interfaces (wired OR wireless) on PORT 50000
sock.bind(("0.0.0.0", PORT))

print("[Master] Listening for workers...")

#set for storing discovered workers
workers = set()

#discover workers by listening for their broadcast messages.
def discover_workers_daemon():
    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        message = data.decode().strip()
        if addr[0] not in workers:
            workers.add(addr[0])
            print(f"[Master] New worker {addr[0]} says: {message}")
            sock.sendto(b"ACK", addr)

threading.Thread(target=discover_workers_daemon, daemon=True).start()

#find master's IP.
def master_ip():
    s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        #a dummy connection to get Master's local IP; it determines the 
        # "FROM IP address" in a connection request
        s.connect(("8.8.8.8", 80))
        ip=s.getsockname()[0]
        print(f"[Master] IP Address: {ip}")
    finally:
        s.close()

master_ip()

while True:
    time.sleep(1)
            