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
    Generic dataset loader.

    Works for:
    - folders
    - files
    - mixed datasets

    Each file or folder becomes a logical group.
    """

    if allowed_extensions:
        allowed_extensions = {ext.lower() for ext in allowed_extensions}

    groups = []

    for entry in os.listdir(root_dir):

        entry_path = os.path.join(root_dir, entry)

        files = []
        total_size = 0

        # -------------------------
        # CASE 1: entry is a FILE
        # -------------------------
        if os.path.isfile(entry_path):

            ext = os.path.splitext(entry)[1].lower()

            if allowed_extensions and ext not in allowed_extensions:
                continue

            size = os.path.getsize(entry_path)

            groups.append({
                "group_id": entry,
                "items": [entry_path],
                "size": size
            })

            continue

        # -------------------------
        # CASE 2: entry is a FOLDER
        # -------------------------
        if os.path.isdir(entry_path):

            for root, _, filenames in os.walk(entry_path):

                for fname in filenames:

                    ext = os.path.splitext(fname)[1].lower()

                    if allowed_extensions and ext not in allowed_extensions:
                        continue

                    path = os.path.join(root, fname)

                    files.append(path)
                    total_size += os.path.getsize(path)

        if files:
            groups.append({
                "group_id": entry,
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
