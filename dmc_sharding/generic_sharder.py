import os
import tarfile
from typing import List, Dict

from .compressor import get_compressor
from .utils import ensure_dir
from .metadata import write_metadata


def pack_samples_by_size(groups, max_shard_size):
    """
    Greedy size-based packing with shuffle + edge case handling
    """

    import random

    # Shuffle (randomize real/fake distribution)
    random.shuffle(groups)


    shards = []
    current_shard = []
    current_size = 0

    for group in groups:
        size = group["size"]

        # Case 1: Oversized sample
        if size > max_shard_size:
            shards.append([group])
            continue

        # Case 2: Start new shard if limit exceeded
        if current_size + size > max_shard_size:
            if current_shard:  # avoid empty shard
                shards.append(current_shard)
            current_shard = []
            current_size = 0

        current_shard.append(group)
        current_size += size

    if current_shard:
        shards.append(current_shard)

    return shards

def shard_groups_to_archives(
    groups: List[Dict],
    output_dir: str,
    max_shard_size: int,
    compression: str = "zstd"
):
    """
    Final production sharding:
    - Sample-level packing
    - Size-based shards
    - Preserves folder structure
    - No duplication
    """

    ensure_dir(output_dir)

    shards = pack_samples_by_size(groups, max_shard_size)
    compressor = get_compressor(compression)

    # Find dataset root safely
    all_paths = []
    for g in groups:
        all_paths.extend(g["items"])

    dataset_root = os.path.commonpath(all_paths)

    metadata_records = []

    for shard_id, shard in enumerate(shards):

        tar_path = os.path.join(output_dir, f"shard_{shard_id}.tar")

        with tarfile.open(tar_path, "w") as tar:

            shard_size = 0

            for sample in shard:
                sample_id = sample["group_id"]

                for path in sample["items"]:

                    # Preserve full structure
                    arcname = os.path.relpath(path, dataset_root)
                    arcname = arcname.replace("\\", "/")

                    tar.add(path, arcname=arcname)

                    size = os.path.getsize(path)
                    shard_size += size

                    metadata_records.append({
                        "shard_id": shard_id,
                        "sample_id": sample_id,
                        "path": path,
                        "size": size,
                        "arcname": arcname
                    })

        # Compress
        compressed_path = tar_path + f".{compression}"
        compressor.compress(tar_path, compressed_path)
        os.remove(tar_path)

        print(f"[Shard {shard_id}] Created (~{shard_size} bytes)")

    # Write metadata
    write_metadata(output_dir, metadata_records)

    print("[DMC-Sharding] Completed successfully.")