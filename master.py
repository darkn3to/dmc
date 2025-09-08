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
stop_flag = False

#discover workers by listening for their broadcast messages.
def discover_workers_daemon():
    while not stop_flag:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
        except OSError:
            break  # socket closed
        message = data.decode().strip()
        if addr[0] not in workers:
            workers.add(addr[0])
            print(f"[Master] New worker {addr[0]} says: {message}")
            #print(f"[Master] Port: {addr[1]}")
        sock.sendto(b"ACK", addr)

t=threading.Thread(target=discover_workers_daemon, daemon=True)
t.start()
    
#find master's IP.
def master_ip() -> int:
    s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        #a dummy connection to get Master's local IP; it determines the 
        # "FROM IP address" in a connection request
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
        #print(f"[Master] IP Address: {ip}")
    finally:
        s.close()

# wait 15 seconds for workers to announce themselves
print("[Master] Waiting 15 seconds for workers to join...")
time.sleep(15)

stop_flag = True

# after 15s, write discovered nodes into file
with open("nodes.txt", "w") as f:
    f.write(f"[Master]: {master_ip()}\n")
    for w in workers:
        f.write(f"[Worker]: {w}\n")


with open("nodes.txt", "rb") as f:
    for w in workers:
        print(f"[Master] Sending nodes.txt to {w}")
        sock.sendto(b"FILE_START", (w, PORT))
        f.seek(0)
        while chunk := f.read(1024):
            sock.sendto(chunk, (w, PORT))
        sock.sendto(b"EOF", (w, PORT))
sock.close()

print("[Master] Sent nodes.txt to all workers.")
