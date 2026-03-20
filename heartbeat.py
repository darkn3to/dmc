import socket
import time

class HeartbeatSender:
    def __init__(self, target_ip, port):
        self.target_ip = target_ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send_heartbeat(self):
        message = f"heartbeat {time.time()}"
        self.sock.sendto(message.encode(), (self.target_ip, self.port))
        print("Sent:", message)

    def start(self, interval=1):
        while True:
            self.send_heartbeat()
            time.sleep(interval)