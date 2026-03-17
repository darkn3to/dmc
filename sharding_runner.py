import os
from dmc_sharding import (
    load_folder_dataset,
    shard_groups_to_archives
)


def main():
    root_path = "data/cifar-10-batches-py"

    print("\n[TEST] Checking dataset path...")
    print("Current working directory:", os.getcwd())
    print("Dataset exists?", os.path.exists(root_path))

    if not os.path.exists(root_path):
        print("ERROR: CIFAR dataset not found.")
        return

    print("\n[TEST] Loading CIFAR folder dataset...")

    groups = load_folder_dataset(
        root_dir=root_path,
        allowed_extensions=None
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

    print("\n[TEST] Starting sharding process...")

    shard_groups_to_archives(
        groups=groups,
        output_dir="cifar_shards_output",
        num_shards=4,          # You can change this
        compression="zstd"
    )

    print("\n[TEST] CIFAR sharding completed successfully!")


if __name__ == "__main__":
    main()