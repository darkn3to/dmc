# Runs at the master node to load the dataset, create logical groups, and shard them into archives.
import os
import argparse
from dmc_sharding import (
    load_dataset,
    shard_groups_to_archives
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load dataset, create logical groups, and shard into archives."
    )
    parser.add_argument(
        "src",
        help="Root dataset path (replaces root_path)."
    )
    parser.add_argument(
        "--max-shard-size",
        type=int,
        default=4 * 1024 * 1024,
        help="Maximum shard size in bytes (default: 4194304)."
    )
    parser.add_argument(
        "--mode",
        choices=["file", "folder"],
        default="file",
        help='Sharding mode: "file" (file mode) or "folder" (default: file).'
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=1,
        help='Folder depth (only used when --mode is "folder").'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    root_path = args.src  # src replaces previous hardcoded root_path
    output_dir = "shards_output"  # dest stays hardcoded as requested

    # Map CLI mode -> load_dataset grouping value
    grouping = "file" if args.mode == "file" else "folder"
    depth = args.depth if args.mode == "folder" else 1

    print("\n[TEST] Checking dataset path...")
    print("Current working directory:", os.getcwd())
    print("Dataset exists?", os.path.exists(root_path))

    if not os.path.exists(root_path):
        print("ERROR: Dataset not found.")
        return

    if args.mode != "folder" and args.depth != 1:
        print('[TEST] Note: --depth is ignored unless --mode is "folder".')

    print("\n[TEST] Loading dataset...")

    groups = load_dataset(
        root_dir=root_path,
        grouping=grouping,
        depth=depth
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
        output_dir=output_dir,
        max_shard_size=args.max_shard_size,
        compression="zstd"
    )

    print("\n[TEST] Sharding completed successfully!")


if __name__ == "__main__":
    main()