"""
DMC Sharding Library

Supports:
- Row-level CSV sharding (group-key based)
- Folder / multi-file / media sharding
- Compression + metadata
- Future integration with DMC master/worker
"""

# -------------------------------
# CSV / DataFrame-based sharding
# -------------------------------
from .sharder import shard_dataset


# --------------------------------
# Generic file / folder sharding
# --------------------------------
from .generic_sharder import shard_groups_to_archives


# -------------------------------
# Dataset loaders
# -------------------------------
from .loader import (
    load_single_csv,
    load_folder_dataset,
    load_from_metadata
)


__all__ = [
    # CSV sharding
    "shard_dataset",

    # Generic sharding
    "shard_groups_to_archives",

    # Loaders
    "load_single_csv",
    "load_folder_dataset",
    "load_from_metadata",
]
