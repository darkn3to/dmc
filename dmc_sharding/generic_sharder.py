import os
import tarfile
from collections import defaultdict
from typing import List, Dict

from .compressor import get_compressor
from .utils import ensure_dir
from .metadata import write_metadata


def greedy_bin_pack(groups: List[Dict], num_shards: int) -> Dict[int, List[Dict]]:
    """
    Distribute logical groups into shards using greedy bin packing
    to balance shard sizes.
    """
    shard_loads = [0] * num_shards
    shard_groups = defaultdict(list)

    # Sort groups by size (largest first)
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

    for shard_id, shard_groups in shard_map.items():
        tar_path = os.path.join(output_dir, f"shard_{shard_id}.tar")

        with tarfile.open(tar_path, "w") as tar:
            for group in shard_groups:
                group_id = group["group_id"]

                for path in group["items"]:
                    # Preserve relative structure inside archive
                    arcname = os.path.join(group_id, os.path.relpath(path))
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

    # Write metadata for recovery / inspection
    write_metadata(
    output_dir=output_dir,
    compression=compression,
    shard_map=shard_map
)


    print("[DMC-Sharding] Generic sharding complete.")
