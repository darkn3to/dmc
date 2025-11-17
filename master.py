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
BROADCAST_COMPLETE_MSG = b"BROADCAST_COMPLETE"

def discover_workers_daemon(sock: socket.socket, workers: dict, stop_event: threading.Event) -> None:
    """Listens for worker broadcasts and adds them to the workers dict {ip: {"username": username, "python_path": None}}."""
    print("[Master] Worker discovery daemon started.")
    sock.settimeout(1.0)

    workers[utils.find_own_ip()] = {"username": os.getlogin(), "python_path": utils.get_python_path()}
    
    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            worker_ip = addr[0]
            
            # --- UPDATED ---
            # Check if the IP (key) is not already in the dictionary
            if worker_ip not in workers:
                username = data.decode().strip()
                print(f"[Master] New worker '{username}' at {worker_ip} found.")
                # Add to dict: {ip: {"username": username, "python_path": None}}
                workers[worker_ip] = {"username": username, "python_path": None}
                sock.sendto(ACK_MSG, addr)
                
        except socket.timeout:
            continue 
        except (OSError, ConnectionResetError):
            print("[Master] Socket error in discovery daemon.")
            break
    sock.settimeout(None)
    print("[Master] Worker discovery daemon has stopped.")

def send_file_to_workers(sock: socket.socket, filename: str, workers: dict) -> None:
    """Sends a single file to every worker in the dict."""
    print(f"[Master] Sending '{filename}' to all workers.")
    if not os.path.exists(filename):
        print(f"[Master] File not found: {filename}")
        return
        
    with open(filename, "rb") as f:
        # --- UPDATED ---
        # Iterating a dict gives its keys (the IPs)
        for worker_ip in workers:
            print(f"    -> Sending to {worker_ip}")
            f.seek(0)
            sock.sendto(FILE_START_MSG, (worker_ip, PORT))
            time.sleep(0.01) 
            
            while chunk := f.read(BUFFER_SIZE - 50):
                sock.sendto(chunk, (worker_ip, PORT))
                time.sleep(0.001)
                
            time.sleep(0.01)
            sock.sendto(EOF_MSG, (worker_ip, PORT))
            time.sleep(0.05)
            
    print(f"[Master] Finished sending '{filename}'.")

def send_broadcast_files_to_workers(sock: socket.socket, directory: str, workers: dict) -> None:
    """Sends all files in the broadcast directory to every worker."""
    print(f"[Master] Sending all files in '{directory}' to workers...")
    if not os.path.exists(directory):
        print(f"[Master] Broadcast directory '{directory}' does not exist.")
        return

    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if not files:
        print(f"[Master] No files to broadcast in '{directory}'.")
        return

    for file_name in files:
        print(f"[Master] Broadcasting file '{file_name}'...")
        file_path = os.path.join(directory, file_name)
        
        with open(file_path, "rb") as f:
            # --- UPDATED ---
            # Iterating a dict gives its keys (the IPs)
            for worker_ip in workers:
                print(f"    -> Sending to {worker_ip}")
                f.seek(0) 
                
                sock.sendto(file_name.encode(), (worker_ip, PORT))
                time.sleep(0.01)
                
                sock.sendto(FILE_START_MSG, (worker_ip, PORT))
                time.sleep(0.01)
                
                while chunk := f.read(BUFFER_SIZE - 50):
                    sock.sendto(chunk, (worker_ip, PORT))
                    time.sleep(0.001)
                    
                time.sleep(0.01)
                sock.sendto(EOF_MSG, (worker_ip, PORT))
                time.sleep(0.05)
                
        print(f"[Master] Finished broadcasting '{file_name}'.")

def write_nodes_file(workers: dict) -> None:
    """Writes the nodes.txt file with IP, username, and Python path for each node."""
    print("[Master] Writing nodes.txt with Python paths...")
    with open(NODES_FILENAME, "w") as f:
        for worker_ip, worker_info in workers.items():
            username = worker_info["username"]
            python_path = worker_info.get("python_path", "unknown")
            f.write(f"[{username}]: {worker_ip}, {python_path}\n")
    print(f"[Master] nodes.txt written with {len(workers)} entries.")

def receive_resource_files(sock: socket.socket, workers: dict) -> None:
    """
    Waits for, receives, and confirms resource files from any expected worker.
    Extracts Python path from resource files and updates workers dict.
    """
    print("[Master] Waiting to receive resource files from workers...")
    os.makedirs(RESOURCES_DIR, exist_ok=True) 
    
    # --- UPDATED ---
    # workers_pending is now a dictionary {ip: {"username": username, "python_path": None}}
    workers_pending = workers.copy()
    
    total_timeout_end = time.time() + 30.0 + (len(workers_pending) * 15.0) 
    
    while workers_pending and time.time() < total_timeout_end:
        print(f"[Master] Waiting for files from: {[w['username'] for w in workers_pending.values()]}")
        sock.settimeout(5.0) 
        
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            worker_ip = addr[0]

            # --- UPDATED ---
            # Check if the IP (key) is in the pending dict
            if worker_ip not in workers_pending or data != FILE_START_MSG:
                continue 

            print(f"[Master] Receiving file from {workers_pending[worker_ip]['username']} ({worker_ip})...")
            filename = f"{RESOURCES_DIR}/{worker_ip}_resources.json"
            file_data = bytearray()
            file_saved_successfully = False

            sock.settimeout(10.0) 
            while True:
                data, file_addr = sock.recvfrom(BUFFER_SIZE)
                if file_addr == addr: 
                    if data == EOF_MSG:
                        file_saved_successfully = True
                        break
                    file_data.extend(data)
            
            if file_saved_successfully:
                with open(filename, "wb") as f:
                    f.write(file_data)
                print(f"File '{filename}' saved. Sending confirmation to {addr}")
                sock.sendto(RESOURCE_FILE_ACK_MSG, addr)
                
                # Extract Python path from resource file
                try:
                    with open(filename, "r") as f:
                        resource_data = json.load(f)
                        python_path = resource_data.get("python_path", "unknown")
                        if worker_ip in workers:
                            workers[worker_ip]["python_path"] = python_path
                            print(f"[Master] Updated Python path for {worker_ip}: {python_path}")
                except Exception as e:
                    print(f"[Master] Error parsing resource file: {e}")
                
                # --- UPDATED as requested ---
                # Remove the worker from the pending dict using its IP (the key)
                removed_worker = workers_pending.pop(worker_ip, None)
                if removed_worker:
                    print(f"[Master] Confirmed file from {removed_worker['username']} ({worker_ip}).")
                # --- END UPDATE ---
                    
            else:
                print(f"File transfer from {worker_ip} failed (no EOF received).")

        except socket.timeout:
            print("[Master] Socket timed out waiting for next file...")
            continue 
        except Exception as e:
            print(f"[Master] Error in receive_resource_files: {e}")
    
    sock.settimeout(None) 
    if workers_pending:
        print(f"[Master] Finished waiting. Did NOT receive files from: {[(ip, w['username']) for ip, w in workers_pending.items()]}")
    print("[Master] Finished receiving resource files.")


def main():
    """Main execution function for the master node."""
    sock = utils.sock_init("0.0.0.0", PORT, 'm')
    try:
        # --- UPDATED ---
        # workers is now a dictionary {ip: {"username": username, "python_path": None}}
        workers = {}
        
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
        
        print(f"[Master] Discovery complete. Found {len(workers)} workers: {workers}")

        ip = utils.find_own_ip()
        os.makedirs(RESOURCES_DIR, exist_ok=True)
        
        # Write initial nodes.txt without Python paths for workers (only master's path is known)
        with open(NODES_FILENAME, "w") as f:
            for worker_ip, worker_info in workers.items():
                username = worker_info["username"]
                python_path = worker_info.get("python_path", "unknown")
                f.write(f"[{username}]: {worker_ip}, {python_path}\n")
        
        if workers:
            send_file_to_workers(sock, NODES_FILENAME, workers)
            #send_broadcast_files_to_workers(sock, "broadcast", workers)
            
            '''print("[Master] Sending broadcast complete signal to all workers.")
            for worker_ip in workers: # Iterating dict gives keys (IPs)
                sock.sendto(BROADCAST_COMPLETE_MSG, (worker_ip, PORT))
                time.sleep(0.01)'''
            
        else:
            print("[Master] No workers found. Skipping file sends.")
        
        resources = utils.collect_resources(ip)
        with open(f"{RESOURCES_DIR}/{ip}_resources.json", "w") as f:
            json.dump(resources, f, indent=4)
        print("[Master] Own resources saved.")

        if workers:
            receive_resource_files(sock, workers)
            # Rewrite nodes.txt with Python paths after receiving resource files
            write_nodes_file(workers)
        else:
            print("[Master] No workers found, skipping resource file reception.")
    
    except Exception as e:
        print(f"An error occurred in main: {e}")
    finally:
        print("[Master] Shutting down.")
        sock.close()

if __name__ == "__main__":
    main()