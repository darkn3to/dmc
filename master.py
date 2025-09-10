import threading
import time
import socket
import os
import utils
import json

# --- Constants ---
PORT = 50000
BUFFER_SIZE = 1024
DISCOVERY_SECONDS = 15
NODES_FILENAME = "nodes.txt"
RESOURCES_DIR = "resources"

# --- Protocol Messages ---
ACK_MSG = b"ACK"
FILE_START_MSG = b"FILE_START"
EOF_MSG = b"EOF"
RESOURCE_FILE_ACK_MSG = b"RESOURCE_FILE_ACK"

def discover_workers_daemon(sock: socket.socket, workers: set, stop_event: threading.Event):
    """Listens for worker broadcasts and adds them to the workers set until stop_event is set."""
    print("[Master] Worker discovery daemon started.")
    sock.settimeout(1.0)
    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            worker_ip = addr[0]
            if worker_ip not in workers:
                workers.add(worker_ip)
                print(f"[Master] New worker {worker_ip} says: {data.decode().strip()}")
            sock.sendto(ACK_MSG, addr)
        except socket.timeout:
            continue # Allows the loop to check the stop_event
        except (OSError, ConnectionResetError):
            print("[Master] Socket error in discovery daemon.")
            break
    sock.settimeout(None)
    print("[Master] Worker discovery daemon has stopped.")

def send_file_to_workers(sock: socket.socket, filename: str, workers: set):
    """Sends a single file to every worker in the set."""
    print(f"[Master] Sending '{filename}' to all workers.")
    with open(filename, "rb") as f:
        for worker_ip in workers:
            print(f"    -> Sending to {worker_ip}")
            f.seek(0)
            sock.sendto(FILE_START_MSG, (worker_ip, PORT))
            time.sleep(0.01)
            while chunk := f.read(BUFFER_SIZE - 50):
                sock.sendto(chunk, (worker_ip, PORT))
            time.sleep(0.01)
            sock.sendto(EOF_MSG, (worker_ip, PORT))
    print(f"[Master] Finished sending '{filename}'.")

def receive_resource_files(sock: socket.socket, workers: set):
    """Waits for, receives, and confirms resource files from workers."""
    print("[Master] Waiting to receive resource files from workers...")
    sock.settimeout(10.0)
    
    for worker_ip in workers.copy():
        print(f"    -> Listening for file from {worker_ip}...")
        filename = f"{RESOURCES_DIR}/{worker_ip}_resources.json"
        worker_full_addr = None
        file_saved_successfully = False

        try:
            while True:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                if addr[0] == worker_ip and data == FILE_START_MSG:
                    worker_full_addr = addr
                    print(f"[Master] Receiving '{filename}' from {addr}...")
                    break
            
            with open(filename, "wb") as f:
                while True:
                    data, addr = sock.recvfrom(BUFFER_SIZE)
                    if addr[0] == worker_ip:
                        if data == EOF_MSG:
                            file_saved_successfully = True
                            break
                        f.write(data)
            
            if file_saved_successfully and worker_full_addr:
                print(f"File saved. Sending confirmation to {worker_full_addr}")
                sock.sendto(RESOURCE_FILE_ACK_MSG, worker_full_addr)

        except socket.timeout:
            print(f"Timed out waiting for file from {worker_ip}.")
            continue

    print("[Master] Finished processing all workers.")

def main():
    """Main execution function for the master node."""
    sock = utils.sock_init("0.0.0.0", PORT, 'm')
    try:
        workers = set()
        stop_discovery = threading.Event()
        discovery_thread = threading.Thread(
            target=discover_workers_daemon,
            args=(sock, workers, stop_discovery),
            daemon=True
        )
        discovery_thread.start()

        print(f"[Master] Waiting {DISCOVERY_SECONDS} seconds for workers to join...")
        time.sleep(DISCOVERY_SECONDS)
        
        stop_discovery.set()
        discovery_thread.join()

        ip = utils.find_own_ip()
        os.makedirs(RESOURCES_DIR, exist_ok=True)
        with open(NODES_FILENAME, "w") as f:
            f.write(f"[Master]: {ip}\n")
            for w in workers:
                f.write(f"[Worker]: {w}\n")
        
        if workers:
            send_file_to_workers(sock, NODES_FILENAME, workers)
        
        resources = utils.collect_resources(ip)
        with open(f"{RESOURCES_DIR}/{ip}_resources.json", "w") as f:
            json.dump(resources, f, indent=4)
        print("[Master] Own resources saved.")

        if workers:
            receive_resource_files(sock, workers)
        else:
            print("[Master] No workers found, skipping resource file reception.")
    finally:
        print("[Master] Shutting down.")
        sock.close()

if __name__ == "__main__":
    main()