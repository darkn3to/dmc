import socket, time

MASTER_PORT = 50000
MESSAGE = b"HELLO"
ACK = b"ACK"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
sock.settimeout(2) 

master_found = False
while not master_found:
    # send broadcast hello
    sock.sendto(MESSAGE, ("255.255.255.255", MASTER_PORT))
    print("Sent HELLO, waiting for ACK...")

    try:
        data = sock.recv(1024)
        if data == ACK:
            print("Got ACK from master.")
            master_found = True
    except socket.timeout:
        print("No ACK, retrying...")
        time.sleep(1)  
