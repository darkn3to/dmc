import socket
import time
import json
import utils

# --- Constants ---
PORT = 50000
BROADCAST_ADDR = "255.255.255.255"
NODES_FILENAME = "nodes.txt"
MAX_RETRIES = 15
ACK_TIMEOUT = 3.0 # seconds

# --- Protocol Messages ---
HELLO_MSG = b"HELLO"
ACK_MSG = b"ACK"
FILE_START_MSG = b"FILE_START"
EOF_MSG = b"EOF"
RESOURCE_FILE_ACK_MSG = b"RESOURCE_FILE_ACK"

def discover_master(sock: socket.socket) -> tuple | None:
    """Broadcasts to find the master and returns its address if found."""
    print("Searching for the master...")
    while True:
        sock.sendto(HELLO_MSG, (BROADCAST_ADDR, PORT))
        print("Sent HELLO, waiting for ACK...")
        try:
            data, addr = sock.recvfrom(1024)
            if data == ACK_MSG:
                print(f"Got ACK from master at {addr}")
                return addr
        except socket.timeout:
            print("No ACK from master, retrying...")
        time.sleep(2)

def receive_file(sock: socket.socket, expected_addr: tuple, filename: str):
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

def send_file_with_retransmission(sock: socket.socket, recipient_addr: tuple, filename: str):
    """Sends a file and waits for a confirmation ACK, with retransmissions."""
    print(f"Sending file '{filename}' to master with confirmation...")

    for i in range(MAX_RETRIES):
        sock.sendto(FILE_START_MSG, recipient_addr)
        time.sleep(0.01)
        with open(filename, "rb") as f:
            while chunk := f.read(1024):
                sock.sendto(chunk, recipient_addr)
        time.sleep(0.01)
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

        sock.settimeout(None)
        receive_file(sock, master_addr, NODES_FILENAME)

        ip = utils.find_own_ip()
        resources = utils.collect_resources(ip)
        resource_file = f"{ip}_resources.json"
        with open(resource_file, "w") as f:
            json.dump(resources, f, indent=4)
        print(f"Local resources saved to '{resource_file}'")

        send_file_with_retransmission(sock, master_addr, resource_file)
    finally:
        print("Worker shutting down.")
        sock.close()

if __name__ == "__main__":
    main()