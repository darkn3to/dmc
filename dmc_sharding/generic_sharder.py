import os
import tarfile
from collections import defaultdict
from typing import List, Dict
import pandas as pd
from .compressor import get_compressor
from .utils import ensure_dir
from .metadata import write_metadata
import re
import hashlib

def hrw_score(shard_id, node_id) -> int:
    key = f"{shard_id}-{node_id}"
    h = hashlib.sha256(key.encode()).hexdigest()
    return int(h, 16)
    
def hrw_assign(shard_ids, nodes, replication_factor=2) -> dict[str, dict[str, list]]:
    """Assign shards to nodes using Highest Random Weight (HRW) hashing for balanced distribution and replication."""
    placement = {}
    placement_map = {node: {"primary": [], "replica": []} for node in nodes}

    for shard in shard_ids:
        scores = []

        for node in nodes:
            score = hrw_score(shard, node)
            scores.append((score, node))

        # maintian top-k nodes based on score using O(n) approach where sorting impact is minimal due to small replication_factor.
        selected_nodes = [(float('-inf'), None)] * replication_factor
        for score, node in scores:
            min_index = 0
            for i in range(1, replication_factor):
                if selected_nodes[i][0] < selected_nodes[min_index][0]:
                    min_index = i

            if score > selected_nodes[min_index][0]:
                selected_nodes[min_index] = (score, node)

        selected_nodes.sort(reverse=True)

        #placement[shard] = [node for _, node in selected_nodes]

        primary_node = selected_nodes[0][1]
        placement_map[primary_node]["primary"].append(shard)

        for _, replica_node in selected_nodes[1:]:
            placement_map[replica_node]["replica"].append(shard)

    return placement_map

def greedy_bin_pack(groups: List[Dict], num_shards: int) -> Dict[int, List[Dict]]:
    """
    Distribute logical groups into shards using greedy bin packing
    to balance shard sizes.
    """
    shard_loads = [0] * num_shards
    shard_groups = defaultdict(list)

    groups_sorted = sorted(groups, key=lambda g: g["size"], reverse=True)

    for group in groups_sorted:
        idx = shard_loads.index(min(shard_loads))
        shard_groups[idx].append(group)
        shard_loads[idx] += group["size"]

    return shard_groups


def shard_groups_to_archives(
    groups: List[Dict],
    output_dir: str,
    num_shards: int,
    compression: str = "zstd"
):
    """
    Create compressed shard archives from logical groups.
    Works for ANY data type: CSV, images, videos, mixed folders.
    """
    ensure_dir(output_dir)

    shard_map = greedy_bin_pack(groups, num_shards)
    compressor = get_compressor(compression)

    metadata_records = []

    all_paths = []
    for g in groups:
        all_paths.extend(g["items"])

    dataset_root = os.path.commonpath(all_paths)

    for shard_id, shard_groups in shard_map.items():

        tar_path = os.path.join(output_dir, f"shard_{shard_id}.tar")

        with tarfile.open(tar_path, "w") as tar:

            for group in shard_groups:
                group_id = group["group_id"]

                for path in group["items"]:

                    # Preserve folder structure relative to dataset root
                    arcname = os.path.relpath(path, dataset_root)

                    tar.add(path, arcname=arcname)

                    metadata_records.append({
                        "shard_id": shard_id,
                        "group_id": group_id,
                        "path": path
                    })

        # Compress archive
        compressed_path = tar_path + f".{compression}"
        compressor.compress(tar_path, compressed_path)
        os.remove(tar_path)

    write_metadata(
        output_dir=output_dir,
        compression=compression,
        shard_map=shard_map
    )

    # Get the path to the parent directory
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Construct the path to nodes.txt
    nodes_file_path = os.path.join(parent_dir, 'nodes.txt')

    # Function to parse IPs from nodes.txt
    def parse_ips(file_path):
        ip_list = []
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    match = re.match(r'\[(.*?)\]:\s*(\d+\.\d+\.\d+\.\d+)', line)
                    if match:
                        key, ip = match.groups()
                        ip_list.append(ip)
        except FileNotFoundError:
            print(f"nodes.txt not found at {file_path}")
        return ip_list


    '''-----------------------------------------------------------------------------------------------------'''
    # Parse the IPs
    parsed_ips = parse_ips(nodes_file_path)
    #print("Parsed IPs:", parsed_ips)
    df = pd.read_csv(os.path.join(output_dir, "metadata.csv"))
    unique_shards = df['shard_id'].unique().tolist()
    replication_factor = min(2, len(parsed_ips)) # Set replication factor to 2 or the number of nodes, whichever is smaller
    # compute original and replica shard placement
    placement_map = hrw_assign(unique_shards, parsed_ips, replication_factor)
    print(placement_map)
    '''-----------------------------------------------------------------------------------------------------'''

    '''
    from collections import defaultdict

    primary_count = defaultdict(int)
    replica_count = defaultdict(int)

    for shard, nodes in placement_map.items():

        if len(nodes) > 0:
            primary_count[nodes[0]] += 1

        for replica in nodes[1:]:
            replica_count[replica] += 1


    print("\nPrimary shard distribution:")
    for node in parsed_ips:
        print(f"{node}: {primary_count[node]}")

    print("\nReplica shard distribution:")
    for node in parsed_ips:
        print(f"{node}: {replica_count[node]}")
    '''

    print("[DMC-Sharding] Generic sharding complete.")