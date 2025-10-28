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
RESOURCES_DIR = "resources" # Changed from "resources" to match worker's dir? User had "resources" here and "broadcast" in worker. I'll keep "resources" as it's for resource files.

# --- Protocol Messages ---
ACK_MSG = b"ACK"
FILE_START_MSG = b"FILE_START"
EOF_MSG = b"EOF"
RESOURCE_FILE_ACK_MSG = b"RESOURCE_FILE_ACK"
# ADDED: New message to signal the end of broadcasts
BROADCAST_COMPLETE_MSG = b"BROADCAST_COMPLETE"

def discover_workers_daemon(sock: socket.socket, workers: set, stop_event: threading.Event):
    """Listens for worker broadcasts and adds them to the workers set until stop_event is set."""
    print("[Master] Worker discovery daemon started.")
    sock.settimeout(1.0)
    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            worker_ip = addr[0]
            if data == b"HELLO" and worker_ip not in workers: # Check for HELLO msg
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
    if not os.path.exists(filename):
        print(f"[Master] File not found: {filename}")
        return
        
    with open(filename, "rb") as f:
        for worker_ip in workers:
            print(f"    -> Sending to {worker_ip}")
            f.seek(0)
            sock.sendto(FILE_START_MSG, (worker_ip, PORT))
            time.sleep(0.01) # Give worker time to see START
            
            while chunk := f.read(BUFFER_SIZE - 50):
                sock.sendto(chunk, (worker_ip, PORT))
                # --- TIMING FIX ---
                # Pace the packets to prevent buffer overflow/loss
                time.sleep(0.001)
                
            time.sleep(0.01)
            sock.sendto(EOF_MSG, (worker_ip, PORT))
            
            # --- TIMING FIX ---
            # Give the worker time to receive EOF, save file, and
            # loop back to its listening state before we send more.
            time.sleep(0.05)
            
    print(f"[Master] Finished sending '{filename}'.")

def send_broadcast_files_to_workers(sock: socket.socket, directory: str, workers: set):
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
            for worker_ip in workers:
                print(f"    -> Sending to {worker_ip}")
                f.seek(0) # Rewind file for each worker
                
                # Send the file name first
                sock.sendto(file_name.encode(), (worker_ip, PORT))
                time.sleep(0.01)
                
                # Send the file content
                sock.sendto(FILE_START_MSG, (worker_ip, PORT))
                time.sleep(0.01)
                
                while chunk := f.read(BUFFER_SIZE - 50):
                    sock.sendto(chunk, (worker_ip, PORT))
                    # --- TIMING FIX ---
                    # Pace the packets
                    time.sleep(0.001)
                    
                time.sleep(0.01)
                sock.sendto(EOF_MSG, (worker_ip, PORT))
                
                # --- TIMING FIX ---
                # Give worker time to process this file before we send the next
                time.sleep(0.05)
                
        print(f"[Master] Finished broadcasting '{file_name}'.")

def receive_resource_files(sock: socket.socket, workers: set):
    """
    Waits for, receives, and confirms resource files from any expected worker.
    This is non-sequential and much more robust.
    """
    print("[Master] Waiting to receive resource files from workers...")
    os.makedirs(RESOURCES_DIR, exist_ok=True) # Ensure dir exists
    
    workers_pending = workers.copy()
    # Set a generous overall timeout for receiving all files
    total_timeout_end = time.time() + 30.0 + (len(workers_pending) * 15.0) 
    
    while workers_pending and time.time() < total_timeout_end:
        print(f"[Master] Waiting for files from: {workers_pending}")
        # Set a short timeout to check loop condition
        sock.settimeout(5.0) 
        
        try:
            # 1. Wait for a FILE_START from *any* valid worker
            data, addr = sock.recvfrom(BUFFER_SIZE)
            worker_ip = addr[0]

            # Ignore if it's not a worker we're waiting for or not a START msg
            if worker_ip not in workers_pending or data != FILE_START_MSG:
                continue 

            print(f"[Master] Receiving file from {worker_ip}...")
            filename = f"{RESOURCES_DIR}/{worker_ip}_resources.json"
            file_data = bytearray()
            file_saved_successfully = False

            # 2. Now, lock onto this address and get the file
            # Set a specific timeout for this single file transfer
            sock.settimeout(10.0) 
            while True:
                data, file_addr = sock.recvfrom(BUFFER_SIZE)
                if file_addr == addr: # Ensure packets are from the same worker
                    if data == EOF_MSG:
                        file_saved_successfully = True
                        break
                    file_data.extend(data)
            
            # 3. If successful, save and ACK
            if file_saved_successfully:
                with open(filename, "wb") as f:
                    f.write(file_data)
                print(f"File '{filename}' saved. Sending confirmation to {addr}")
                sock.sendto(RESOURCE_FILE_ACK_MSG, addr)
                workers_pending.remove(worker_ip) # Mark worker as complete
            else:
                print(f"File transfer from {worker_ip} failed (no EOF received).")

        except socket.timeout:
            print("[Master] Socket timed out waiting for next file...")
            continue # Loop again to check total_timeout
        except Exception as e:
            print(f"[Master] Error in receive_resource_files: {e}")
    
    sock.settimeout(None) # Clear timeout
    if workers_pending:
        print(f"[Master] Finished waiting. Did NOT receive files from: {workers_pending}")
    print("[Master] Finished receiving resource files.")


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
        
        print(f"[Master] Discovery complete. Found {len(workers)} workers: {workers}")

        ip = utils.find_own_ip()
        os.makedirs(RESOURCES_DIR, exist_ok=True)
        with open(NODES_FILENAME, "w") as f:
            f.write(f"[Master]: {ip}\n")
            for w in workers:
                f.write(f"[Worker]: {w}\n")
        
        if workers:
            # Send nodes.txt to all workers
            send_file_to_workers(sock, NODES_FILENAME, workers)
            # Send all files in 'broadcast' dir to all workers
            #send_broadcast_files_to_workers(sock, "broadcast", workers)
            
            # --- CRITICAL FIX ---
            # Tell all workers that the broadcast is finished
            print("[Master] Sending broadcast complete signal to all workers.")
            for worker_ip in workers:
                sock.sendto(BROADCAST_COMPLETE_MSG, (worker_ip, PORT))
                time.sleep(0.01)
            # --- END FIX ---
            
        else:
            print("[Master] No workers found. Skipping file sends.")
        
        # Save master's own resources
        resources = utils.collect_resources(ip)
        with open(f"{RESOURCES_DIR}/{ip}_resources.json", "w") as f:
            json.dump(resources, f, indent=4)
        print("[Master] Own resources saved.")

        # Now, wait for all workers to send their resource files
        if workers:
            receive_resource_files(sock, workers)
        else:
            print("[Master] No workers found, skipping resource file reception.")
    
    except Exception as e:
        print(f"An error occurred in main: {e}")
    finally:
        print("[Master] Shutting down.")
        sock.close()

if __name__ == "__main__":
    main()