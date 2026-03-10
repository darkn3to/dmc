import os
import pandas as pd
from typing import Set, List, Dict


def load_single_csv(csv_path: str, group_key: str) -> List[Dict]:
    df = pd.read_csv(csv_path)
    groups = []

    for key, group in df.groupby(group_key):
        groups.append({
            "group_id": str(key),
            "items": [csv_path],
            "size": group.memory_usage(deep=True).sum()
        })

    return groups


def load_folder_dataset(
    root_dir: str,
    allowed_extensions: Set[str] = None
) -> List[Dict]:
    """
    root_dir/
      users/
      orders/
      transactions/

    Each immediate subfolder = one logical group
    """

    if allowed_extensions:
        allowed_extensions = {ext.lower() for ext in allowed_extensions}

    groups = []

    for group_name in os.listdir(root_dir):
        group_dir = os.path.join(root_dir, group_name)

        if not os.path.isdir(group_dir):
            continue

        files = []
        total_size = 0

        for root, _, filenames in os.walk(group_dir):
            for fname in filenames:
                ext = os.path.splitext(fname)[1].lower()

                if allowed_extensions and ext not in allowed_extensions:
                    continue

                path = os.path.join(root, fname)
                files.append(path)
                total_size += os.path.getsize(path)

        if files:
            groups.append({
                "group_id": group_name,
                "items": files,
                "size": total_size
            })

    return groups


def load_from_metadata(metadata_csv: str) -> List[Dict]:
    """
    Reload groups from metadata CSV (used in recovery / resume)
    """
    df = pd.read_csv(metadata_csv)
    grouped = {}

    for _, row in df.iterrows():
        gid = row["group_id"]
        path = row["path"]

        grouped.setdefault(gid, {"items": [], "size": 0})
        grouped[gid]["items"].append(path)
        grouped[gid]["size"] += os.path.getsize(path)

    return [
        {"group_id": gid, **data}
        for gid, data in grouped.items()
    ]
