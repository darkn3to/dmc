from dmc_fault.heartbeat import MasterNode, WorkerNode
import time
import multiprocessing
import random 

def worker_wrapper(master_ip, worker_id, interval, reliability):
    """
    Simulates a worker that sends heartbeats with crash/recovery.
    """
    print(f"--- [START] {worker_id} is now online ---")
    
    # Create worker node and start sending heartbeats
    worker = WorkerNode(master_ip, interval)
    
    try:
        while True:
            # Simulate a crash
            if random.random() > reliability:
                print(f"--- [CRASH] {worker_id} has stopped! ---")
                return
            
            # Send heartbeat
            worker.send_heartbeats()  # This will block, so we break if crash happens
    except:
        pass

def worker_process(master_ip, worker_id, interval, reliability, min_uptime):
    """
    Worker process that sends heartbeats until it crashes.
    It will stay alive for at least min_uptime seconds before crash checks begin.
    """
    print(f"--- [START] {worker_id} is now online ---")
    worker = WorkerNode(master_ip, interval)
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        # Allow crash checks only after minimum guaranteed runtime.
        if elapsed >= min_uptime and random.random() > reliability:
            print(f"--- [CRASH] {worker_id} has stopped! ---")
            return
        
        # Send one heartbeat
        try:
            import socket
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.sendto(b"heartbeat", (master_ip, 9999))
        except Exception:
            pass
        
        time.sleep(interval)

if __name__ == "__main__":
    # CONFIGURATION
    known_workers = ["127.0.0.1"]
    # Master waits 15 seconds before declaring offline
    threshold = 10 
    min_runtime_after_restart = threshold + 5
    
    # Master owns its own Manager for shared memory.
    master = MasterNode(known_workers=known_workers, min_heartbeat_threshold=threshold)

    # Start Master background tasks with multiprocessing
    master.start_host()

    # Worker Personalities: (Reliability, Heartbeat Interval)
    # Node-B is set to 0.6 reliability to crash often.
        # On localhost simulation all workers share 127.0.0.1, so identity is IP-based.
    worker_specs = {
            "Node-A": (0.99, 5),      
            "Node-B": (0.60, 5),      
            "Rogue-Node": (0.95, 5)
        }

    active_jobs = {}

    print(f"Simulation started. Threshold: {threshold}s. Press Ctrl+C to quit.\n")

    try:
        while True:
            for name, (rel, interval) in worker_specs.items():
                # If worker process is not running, handle the restart
                if name not in active_jobs or not active_jobs[name].is_alive():
                    
                    # If it was already running and died, it 'crashed'
                    if name in active_jobs:
                        # Keep worker down long enough to guarantee removal.
                        # Master checks every `threshold` seconds and removes after
                        # `int(threshold * 1.5)` seconds offline.
                        # Adding another full check interval guarantees the remove check runs.
                        min_reboot = int(threshold * 1.5) + threshold + 1
                        max_reboot = min_reboot + threshold
                        reboot_time = random.randint(min_reboot, max_reboot)
                        print(f"--- [RECOVERY] {name} will be offline for {reboot_time}s... ---")
                        time.sleep(reboot_time)

                    p = multiprocessing.Process(
                        target=worker_process, 
                        args=("127.0.0.1", name, interval, rel, min_runtime_after_restart),
                        daemon=True
                    )
                    p.start()
                    active_jobs[name] = p
            
            time.sleep(1) 
    except KeyboardInterrupt:
        print("\nSimulation ended.")