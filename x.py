#!/usr/bin/env python3
"""
Diagnostic script to test UDP broadcast discovery between master and worker.
Run on each machine separately to isolate the problem.
"""

import socket
import subprocess
import sys
import os
import time
import ipaddress

PORT = 50000
BROADCAST_ADDR = "255.255.255.255"

def test_outbound(targets):
    """Test if we can send UDP packets."""
    print("\n=== OUTBOUND TEST ===")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    
    message = b"TEST"
    for target in targets:
        try:
            sock.sendto(message, (target, PORT))
            print(f"✓ Successfully sent to {target}:{PORT}")
        except Exception as e:
            print(f"✗ Failed to send to {target}:{PORT}: {e}")
    sock.close()

def test_inbound():
    """Test if we can listen for UDP packets."""
    print("\n=== INBOUND TEST ===")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        sock.bind(("0.0.0.0", PORT))
        print(f"✓ Successfully bound to 0.0.0.0:{PORT}")
        print(f"Listening for 5 seconds... (send from another machine: python3 test_discovery.py <your_ip>)")
        
        sock.settimeout(5.0)
        try:
            while True:
                data, addr = sock.recvfrom(1024)
                print(f"✓ Received '{data.decode(errors='ignore')}' from {addr[0]}:{addr[1]}")
        except socket.timeout:
            print("(No packets received - this is expected if no one is sending)")
    except OSError as e:
        print(f"✗ Failed to bind to port {PORT}: {e}")
        print(f"   Check if another process is already using port {PORT}")
        print(f"   Try: lsof -i :{PORT}")
    finally:
        sock.close()

def test_local_ip():
    """Show detected local IP."""
    print("\n=== LOCAL CONFIGURATION ===")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        print(f"✓ Local IP detected: {local_ip}")
        
        parts = local_ip.split(".")
        if len(parts) == 4:
            directed_bcast = f"{parts[0]}.{parts[1]}.{parts[2]}.255"
            print(f"✓ Network directed broadcast: {directed_bcast}")
        
        return local_ip
    except Exception as e:
        print(f"✗ Could not detect local IP: {e}")
        return None

def main():
    print("UDP Discovery Diagnostic Tool")
    print("="*50)
    
    local_ip = test_local_ip()
    
    # Determine targets
    targets = []
    
    # If argument provided, that's the master IP to test
    if len(sys.argv) > 1:
        master_ip = sys.argv[1]
        try:
            ipaddress.ip_address(master_ip)
            targets.append(master_ip)
            print(f"Using explicit master IP: {master_ip}")
        except ValueError:
            print(f"Invalid IP address: {master_ip}")
            return
    
    # Add directed broadcast for local network
    if local_ip:
        parts = local_ip.split(".")
        if len(parts) == 4:
            targets.append(f"{parts[0]}.{parts[1]}.{parts[2]}.255")
    
    # Add global broadcast
    targets.append(BROADCAST_ADDR)
    
    print("\n=== TEST PLAN ===")
    print("1. Test outbound (can we send UDP?)")
    print("2. Test inbound (can we listen on port 50000?)")
    print("3. Results will help isolate firewall vs code issues")
    
    test_outbound(targets)
    test_inbound()
    
    print("\n=== TROUBLESHOOTING TIPS ===")
    print("If outbound FAILS:")
    print("  → Check firewall allows UDP 50000 outbound")
    print("  → Check hosts can ping each other")
    print()
    print("If inbound FAILS:")
    print("  → Port 50000 already in use (check: lsof -i :50000)")
    print("  → Permission issue (try with sudo)")
    print()
    print("If inbound works but master/worker don't connect:")
    print("  → Run with explicit MASTER_IP=<ip> python3 worker.py")
    print("  → Common: broadcast doesn't cross VLANs/subnets")

if __name__ == "__main__":
    main()
