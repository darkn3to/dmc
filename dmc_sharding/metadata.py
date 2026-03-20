import os
import csv


def write_metadata(output_dir: str, records: list):
    """
    Write metadata for shards.
    """

    path = os.path.join(output_dir, "metadata.csv")

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["shard_id", "sample_id", "path", "size", "arcname"]
        )
        writer.writeheader()
        writer.writerows(records)

    print(f"[Metadata] Written to {path}")