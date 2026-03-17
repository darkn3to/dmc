import os
import csv
from typing import Dict, List


def write_metadata(
    output_dir: str,
    compression: str,
    shard_map: Dict[int, List[dict]]
):
    """
    Writes metadata for generic sharding.
    Records which group & files went into which shard.
    """

    meta_path = os.path.join(output_dir, "metadata.csv")

    with open(meta_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["shard_id", "group_id", "path", "compression"])

        for shard_id, groups in shard_map.items():
            for group in groups:
                for path in group["items"]:
                    writer.writerow([
                        shard_id,
                        group["group_id"],
                        path,
                        compression
                    ])
