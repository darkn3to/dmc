import os
from dmc_sharding import (
    load_dataset,
    shard_groups_to_archives
)


def main():
    root_path = "dataset3/test"

    print("\n[TEST] Checking dataset path...")
    print("Current working directory:", os.getcwd())
    print("Dataset exists?", os.path.exists(root_path))

    if not os.path.exists(root_path):
        print("ERROR: Dataset not found.")
        return

    print("\n[TEST] Loading dataset...")

    groups = load_dataset(
    root_dir=root_path,
    grouping="file"
    
)

    print(f"\n[TEST] Found {len(groups)} logical groups")

    total_size = 0

    for group in groups:
        print(
            f"  - Group '{group['group_id']}' "
            f"contains {len(group['items'])} files, "
            f"size={group['size']} bytes"
        )
        total_size += group["size"]

    print(f"\n[TEST] Total dataset size: {total_size} bytes")

    if len(groups) == 0:
        print("No groups created. Sharding stopped.")
        return

    print("\n[TEST] Starting sharding process...")

    print("Total samples:", len(groups))
    

    shard_groups_to_archives(
    groups=groups,
    output_dir="dataset3_output",
    max_shard_size=256 * 1024 * 1024,  # 256 MB
    compression="zstd"
)
    print("\n[TEST] Sharding completed successfully!")


if __name__ == "__main__":
    main()