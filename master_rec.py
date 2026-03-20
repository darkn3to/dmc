import socket

class Receiver:
    def __init__(self, allowed_sender, port):
        self.host = allowed_sender
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
    
    def start(self):
        print(f"Receiver listening on {self.host}:{self.port}...")
        while True:
            data, addr = self.sock.recvfrom(4096)
            print(f"Received from {addr}: {data.decode(errors='ignore')}")