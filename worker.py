import socket
import time
import json
import utils
import os
import subprocess

# --- Constants ---
PORT = 50001
BROADCAST_ADDR = "255.255.255.255"
NODES_FILENAME = "nodes1.txt"
MAX_RETRIES = 15
ACK_TIMEOUT = 3.0 # seconds
RESOURCES_DIR = "broadcast"

# --- Protocol Messages ---
HELLO_MSG = b"HELLO"
ACK_MSG = b"ACK"
FILE_START_MSG = b"FILE_START"
EOF_MSG = b"EOF"
RESOURCE_FILE_ACK_MSG = b"RESOURCE_FILE_ACK"
# ADDED: New message to signal the end of broadcasts
BROADCAST_COMPLETE_MSG = b"BROADCAST_COMPLETE" 

def discover_master(sock: socket.socket) -> tuple | None:
    """Broadcasts to find the master and returns its address if found."""
    print("Searching for the master...")
    username = subprocess.run(["whoami"], capture_output=True, text=True).stdout.strip().encode()
    while True:
        sock.sendto(username, (BROADCAST_ADDR, PORT))
        print("Sent HELLO, waiting for ACK...")
        try:
            data, addr = sock.recvfrom(1024)
            if data == ACK_MSG:
                print(f"Got ACK from master at {addr}")
                return addr
        except socket.timeout:
            print("No ACK from master, retrying...")
        time.sleep(2)

def receive_file(sock: socket.socket, expected_addr: tuple, filename: str) -> None:
    """Receives a file from a specific address."""
    print(f"Waiting for file '{filename}' from master...")
    with open(filename, "wb") as f:
        while True:
            data, addr = sock.recvfrom(1024)
            if addr == expected_addr and data == FILE_START_MSG:
                print(f"Receiving '{filename}'...")
                break
        while True:
            data, addr = sock.recvfrom(1024)
            if addr == expected_addr:
                if data == EOF_MSG:
                    print(f"'{filename}' transfer complete.")
                    break
                f.write(data)

def receive_broadcast_files(sock: socket.socket, master_addr: tuple) -> None:
    """Receives broadcasted files from the master."""
    os.makedirs(RESOURCES_DIR, exist_ok=True)
    print(f"[Worker] Listening for broadcasted files from master {master_addr[0]}...")

    while True:
        try:
            # Wait for the file name OR the complete signal
            file_name_data, addr = sock.recvfrom(1024)
            if addr != master_addr:
                continue

            # --- THIS IS THE CRITICAL FIX ---
            # Check if the master signaled that broadcasting is done
            if file_name_data == BROADCAST_COMPLETE_MSG:
                print("[Worker] Received broadcast complete signal. Moving on.")
                break # Exit the while True loop
            # --- END FIX ---

            # If not complete, assume it's a file name
            file_name = file_name_data.decode()
            print(f"[Worker] Receiving file '{file_name}' from {addr[0]}...")

            # Wait for FILE_START message
            data, addr = sock.recvfrom(1024)
            if addr != master_addr or data != FILE_START_MSG:
                print("[Worker] Expected FILE_START, got something else. Skipping.")
                continue

            # Receive file chunks
            file_data = bytearray()
            while True:
                data, addr = sock.recvfrom(1024)
                if addr == master_addr:
                    if data == EOF_MSG:
                        break
                    file_data.extend(data)
            
            # Save the received file with the original file name
            file_path = os.path.join(RESOURCES_DIR, file_name)
            with open(file_path, "wb") as f:
                f.write(file_data)
            print(f"[Worker] Saved file as '{file_path}'")

        except KeyboardInterrupt:
            print("[Worker] Exiting...")
            break
        except Exception as e:
            print(f"[Worker] Error: {e}")

def send_file_with_retransmission(sock: socket.socket, recipient_addr: tuple, filename: str) -> None:
    """Sends a file and waits for a confirmation ACK, with retransmissions."""
    print(f"Sending file '{filename}' to master with confirmation...")

    # Ensure buffer size is reasonable, e.g., 1024
    buffer_size = 1024 - 50 # Give some headroom

    for i in range(MAX_RETRIES):
        print(f"Attempt {i+1}/{MAX_RETRIES}: Sending file '{filename}'...")
        sock.sendto(FILE_START_MSG, recipient_addr)
        time.sleep(0.01) # Small delay
        
        with open(filename, "rb") as f:
            while chunk := f.read(buffer_size):
                sock.sendto(chunk, recipient_addr)
                # --- TIMING FIX ---
                # Add a tiny sleep to pace packets and not flood the receiver
                time.sleep(0.001) 
                
        time.sleep(0.01) # Give receiver a moment before EOF
        sock.sendto(EOF_MSG, recipient_addr)
        
        sock.settimeout(ACK_TIMEOUT)
        try:
            data, addr = sock.recvfrom(1024)
            if addr == recipient_addr and data == RESOURCE_FILE_ACK_MSG:
                print("Master confirmed receipt. Transfer successful.")
                return
        except socket.timeout:
            print(f"No confirmation from master. Retrying...")
    
    print(f"Transfer failed after {MAX_RETRIES} attempts.")

def main():
    """Main execution function for the worker node."""
    sock = utils.sock_init("", PORT, 'w')
    try:
        sock.settimeout(2.0)
        master_addr = discover_master(sock)
        if not master_addr:
            print("Could not find master. Exiting.")
            return

        sock.settimeout(None) # IMPORTANT: Clear timeout for blocking receives

        # Receive the nodes.txt file from the master
        receive_file(sock, master_addr, NODES_FILENAME)

        # Receive broadcasted files from the master
        # This function will now correctly exit when done
        #receive_broadcast_files(sock, master_addr)

        # --- THIS CODE IS NOW REACHABLE ---
        print("[Worker] Collecting local resources...")
        
        # Collect and send resource information to the master
        ip = utils.find_own_ip()
        resources = utils.collect_resources(ip)
        resource_file = f"{ip}_resources.json"
        with open(resource_file, "w") as f:
            json.dump(resources, f, indent=4)
        print(f"Local resources saved to '{resource_file}'")

        send_file_with_retransmission(sock, master_addr, resource_file)
    
    except Exception as e:
        print(f"An error occurred in main: {e}")
    finally:
        print("Worker shutting down.")
        sock.close()

if __name__ == "__main__":
    main()