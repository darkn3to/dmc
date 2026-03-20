import os
from typing import List, Dict


def load_dataset(root_dir: str, grouping: str = "file") -> List[Dict]:
    """
    grouping:
        - "file"   → each file is a sample
        - "folder" → each folder is a group
    """

    groups = []

    if grouping == "file":
        # 🔥 File-level grouping (current behavior)
        for root, _, files in os.walk(root_dir):
            for file in files:
                path = os.path.join(root, file)

                groups.append({
                    "group_id": path,
                    "items": [path],
                    "size": os.path.getsize(path)
                })

    elif grouping == "folder":
        # 🔥 Folder-level grouping
        for root, _, files in os.walk(root_dir):

            if not files:
                continue

            items = []
            total_size = 0

            for file in files:
                path = os.path.join(root, file)
                items.append(path)
                total_size += os.path.getsize(path)

            groups.append({
                "group_id": root,   # folder becomes group
                "items": items,
                "size": total_size
            })

    else:
        raise ValueError("grouping must be 'file' or 'folder'")

    return groups