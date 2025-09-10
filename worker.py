import time, socket
import utils
import json

PORT = 50000
MESSAGE = b"HELLO"
ACK = b"ACK"

sock = utils.sock_init("", PORT, 'w')
sock.settimeout(2)

master_found = False
master_addr = None  

while not master_found:
    # send broadcast hello
    sock.sendto(MESSAGE, ("255.255.255.255", PORT))
    print("Sent HELLO, waiting for ACK...")

    try:
        data, addr = sock.recvfrom(1024)
        if data == ACK:
            print(f"Got ACK from master at {addr}")
            master_addr = addr 
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
            print("FILE_START received. Receiving file nodes.txt...")
            break

    # receive the actual file
    while True:
        data, addr = sock.recvfrom(1024)
        # And this comparison also works correctly
        if addr != master_addr:
            continue
        if data == b"EOF":
            print("nodes.txt transfer complete.")
            break
        f.write(data)

IP = utils.find_own_ip()
resources = utils.collect_resources(IP)
resource_file = f"{IP}_resources.json"

# Save resources to a JSON file
with open(resource_file, "w") as json_file:  
    json.dump(resources, json_file, indent=4) 
print("Resources saved to resources.json")

# Send the resource file to the master
print(f"Sending resource file {resource_file} to master...")
with open(resource_file, "rb") as f:
    while chunk := f.read(1024):  # Read the file in chunks
        sock.sendto(chunk, master_addr)  # Send each chunk to the master
    sock.sendto(b"EOF", master_addr)  # Send EOF to indicate the end of the file

print("Resource file sent to master.")
sock.close()