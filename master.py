import threading
import time, socket, os
import utils, json

PORT = 50000
BUFFER_SIZE = 1024

# Setup a UDP socket
sock = utils.sock_init("0.0.0.0", PORT, 'm')

print("[Master] Listening for workers...")

# Set to keep track of discovered workers
workers = set()
stop_flag = False

# Function to discover workers
def discover_workers_daemon() -> None:
    sock.settimeout(1.0)
    while not stop_flag:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            message = data.decode().strip()
            if addr[0] not in workers:
                workers.add(addr[0])
                print(f"[Master] New worker {addr[0]} says: {message}")
            sock.sendto(b"ACK", addr)
        except socket.timeout:
            continue
        except OSError:
            break
    sock.settimeout(None)
    print("[Master] Worker discovery daemon has stopped.")

# Function to send a file to workers
def send_file_to_workers(file_path, workers) -> None:
    with open(file_path, "rb") as f:
        for w in workers:
            print(f"[Master] Sending {file_path} to {w}")
            sock.sendto(b"FILE_START", (w, PORT))
            f.seek(0)
            while chunk := f.read(1024):
                sock.sendto(chunk, (w, PORT))
            sock.sendto(b"EOF", (w, PORT))
    print(f"[Master] Sent {file_path} to all workers.")

# Function to receive resource files from workers
def receive_resource_files(workers) -> None:
    print("[Master] Waiting to receive resource files from workers...")
    while workers:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            if addr[0] in workers:
                filename = f"resources/{addr[0]}_resources.json"
                with open(filename, "wb") as f:
                    print(f"[Master] Receiving resource file from {addr[0]}...")
                    while True:
                        if data == b"EOF":
                            print(f"[Master] Finished receiving file from {addr[0]}")
                            workers.remove(addr[0])
                            break
                        f.write(data)
                        data, addr = sock.recvfrom(BUFFER_SIZE)
        except socket.timeout:
            print("[Master] Waiting for workers to send resource files...")
    print("[Master] All resource files received.")

# Main execution
if __name__ == "__main__":
    t = threading.Thread(target=discover_workers_daemon, daemon=True)
    t.start()

    print("[Master] Waiting 15 seconds for workers to join...")
    time.sleep(15)
    stop_flag = True
    # Ensure that discovery daemon has stopped 
    # to prevent packet theft in receving resources.json
    t.join()  

    IP = utils.find_own_ip()
    os.makedirs("resources", exist_ok=True)

    # Write discovered nodes to file
    nodes_file = "nodes.txt"
    with open(nodes_file, "w") as f:
        f.write(f"[Master]: {IP}\n")
        for w in workers:
            f.write(f"[Worker]: {w}\n")

    # Send nodes.txt to workers
    send_file_to_workers(nodes_file, workers)

    # Save master resources
    resources = utils.collect_resources(IP)
    with open(f"resources/{IP}_resources.json", "w") as json_file:
        json.dump(resources, json_file, indent=4)
    print("[Master] Resources saved to resources.json")

    # Receive resource files from workers
    receive_resource_files(workers)

    sock.close()