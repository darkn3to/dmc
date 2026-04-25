import json
import random

from dmc_fault.heartbeat import MasterNode


def print_state(label, master):
    print(f"\n--- {label} ---")
    print("Known workers:", list(master.known_workers))
    print("Heartbeats:", dict(master.heartbeats))


if __name__ == "__main__":
    with open("broadcast/placement_map.json", "r") as f:
        placement_map = json.load(f)

    master = MasterNode("127.0.0.1")
    print("MasterNode created with default values")
    print("heartbeat_interval:", master.heartbeat_interval)
    print("min_heartbeat_threshold:", master.min_heartbeat_threshold)
    print("json_file:", master.json_file)

    for worker_ip, shards in placement_map.items():
        master.add_worker(
            worker_ip,
            primary=shards.get("primary", []),
            replica=shards.get("replica", []),
        )

    print_state("After loading workers from placement_map.json", master)

    test1 = set(placement_map.keys()).issubset(set(master.heartbeats.keys()))
    print("TEST 1 (placement_map workers loaded):", "PASS" if test1 else "FAIL")

    random_worker_ip = f"10.99.{random.randint(1, 254)}.{random.randint(1, 254)}"
    random_primary = random.sample(range(20, 80), 2)
    random_replica = random.sample(range(20, 80), 2)

    master.add_worker(random_worker_ip, primary=random_primary, replica=random_replica)
    added = dict(master.heartbeats).get(random_worker_ip, {})
    test2 = (
        random_worker_ip in list(master.known_workers)
        and added.get("primary") == random_primary
        and added.get("replica") == random_replica
    )
    print_state("After adding random worker", master)
    print("TEST 2 (add_worker with random values):", "PASS" if test2 else "FAIL")

    master.remove_worker(random_worker_ip)
    test3 = random_worker_ip not in dict(master.heartbeats) and random_worker_ip not in list(
        master.known_workers
    )
    print_state("After removing random worker", master)
    print("TEST 3 (remove_worker on random worker):", "PASS" if test3 else "FAIL")

    removable_ip = None
    expected_promotions = {}
    for candidate_ip, candidate_shards in placement_map.items():
        candidate_primary = set(candidate_shards.get("primary", []))
        overlaps = {}
        for other_ip, other_shards in placement_map.items():
            if other_ip == candidate_ip:
                continue
            overlap = sorted(candidate_primary.intersection(set(other_shards.get("replica", []))))
            if overlap:
                overlaps[other_ip] = overlap
        if overlaps:
            removable_ip = candidate_ip
            expected_promotions = overlaps
            break

    if removable_ip is not None and removable_ip in list(master.known_workers):
        master.remove_worker(removable_ip)
        state = dict(master.heartbeats)
        promoted_ok = True
        for ip, expected in expected_promotions.items():
            active = state.get(ip, {}).get("active_replicas", [])
            for shard in expected:
                if shard not in active:
                    promoted_ok = False
                    break
            if not promoted_ok:
                break
        print_state(f"After removing {removable_ip}", master)
        print(
            "TEST 4 (active_replicas promoted from removed primary):",
            "PASS" if promoted_ok else "FAIL",
        )
    else:
        print("TEST 4 skipped: no overlapping replica/primary pairs found in placement_map.json")
