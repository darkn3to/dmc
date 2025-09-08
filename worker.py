import socket, time

PORT = 50000
MESSAGE = b"HELLO"
ACK = b"ACK"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
sock.settimeout(2)
sock.bind(("", PORT))

master_found = False
master_addr = None  # <--- FIX 1: Initialize the variable here.

while not master_found:
    # send broadcast hello
    sock.sendto(MESSAGE, ("255.255.255.255", PORT))
    print("Sent HELLO, waiting for ACK...")

    try:
        data, addr = sock.recvfrom(1024)
        if data == ACK:
            print(f"Got ACK from master at {addr}")
            master_addr = addr  # <--- FIX 2: Save the master's address.
            master_found = True
    except socket.timeout:
        print("No ACK, retrying...")
        time.sleep(1)
    if not master_found:
        time.sleep(1)

sock.settimeout(None)
with open("nodes.txt", "wb") as f:
    print("Waiting for file from master...")
    while True:
        data, addr = sock.recvfrom(1024)
        # Now this comparison works correctly
        if addr != master_addr:
            continue
        if data == b"FILE_START":
            print("FILE_START received. Receiving file...")
            break

    # receive the actual file
    while True:
        data, addr = sock.recvfrom(1024)
        # And this comparison also works correctly
        if addr != master_addr:
            continue
        if data == b"EOF":
            print("File transfer complete.")
            break
        f.write(data)

sock.close()