import os
from collections import defaultdict
from typing import Dict, List
from .compressor import get_compressor
from .metadata import write_metadata
from .utils import ensure_dir


def shard_dataset(
    dataset_path: str,
    output_dir: str,
    group_key: str,
    num_shards: int,
    compression: str = "zstd"
):
    import pandas as pd 
    """
    Splits a centralized dataset into balanced shards while keeping related
    data (based on group_key) intact. Outputs compressed shard files.
    """

    ensure_dir(output_dir)

    print("[DMC-Sharding] Loading dataset...")
    df = pd.read_csv(dataset_path)

    print("[DMC-Sharding] Grouping dataset...")
    grouped = df.groupby(group_key)

    # --- Step 1: Estimate group sizes ---
    group_sizes = {}
    for key, group in grouped:
        group_sizes[key] = group.memory_usage(deep=True).sum()

    # --- Step 2: Greedy bin-packing ---
    shard_loads = [0] * num_shards
    shard_groups: Dict[int, List[str]] = defaultdict(list)

    sorted_groups = sorted(group_sizes.items(), key=lambda x: x[1], reverse=True)

    for group_name, size in sorted_groups:
        idx = shard_loads.index(min(shard_loads))
        shard_groups[idx].append(group_name)
        shard_loads[idx] += size

    # --- Step 3: Write shards ---
    shard_paths = []
    for shard_id, groups in shard_groups.items():
        shard_file = f"{output_dir}/shard_{shard_id}.csv"
        shard_paths.append(shard_file)

        shard_df = df[df[group_key].isin(groups)]
        shard_df.to_csv(shard_file, index=False)

    # --- Step 4: Compress shards ---
    compressor = get_compressor(compression)

    for shard_file in shard_paths:
        compressed_path = shard_file + f".{compression}"
        print(f"[DMC-Sharding] Compressing {shard_file} → {compressed_path}")
        compressor.compress(shard_file, compressed_path)
        os.remove(shard_file)

    # --- Step 5: Write metadata ---
    write_metadata(output_dir, group_key, compression, shard_groups)

    print("[DMC-Sharding] Sharding complete.")
