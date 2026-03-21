import os
from typing import List, Dict


def load_dataset(root_dir: str, grouping: str = "file", depth: int = 1) -> List[Dict]:
    groups = []

    if grouping == "file":
        # File-level grouping
        for root, _, files in os.walk(root_dir):
            for file in files:
                path = os.path.join(root, file)

                groups.append({
                    "group_id": path,
                    "items": [path],
                    "size": os.path.getsize(path)
                })

    elif grouping == "folder":
        group_map = {}
        max_available_depth = 0

        # First pass → find max depth
        for root, _, files in os.walk(root_dir):
            for file in files:
                path = os.path.join(root, file)
                rel_path = os.path.relpath(path, root_dir)
                parts = rel_path.split(os.sep)

                folder_depth = len(parts) - 1
                if folder_depth > max_available_depth:
                    max_available_depth = folder_depth

        # Depth check
        if depth > max_available_depth:
            print("Cannot do folder grouping: no more folders available.")
            print(f"Maximum folder depth in dataset is {max_available_depth}")
            return []

        # Second pass → grouping
        for root, _, files in os.walk(root_dir):
            for file in files:
                path = os.path.join(root, file)

                rel_path = os.path.relpath(path, root_dir)
                parts = rel_path.split(os.sep)

                if len(parts) > depth:
                    group_id = os.path.join(*parts[:depth])
                else:
                    group_id = parts[0]

                if group_id not in group_map:
                    group_map[group_id] = {
                        "group_id": group_id,
                        "items": [],
                        "size": 0
                    }

                group_map[group_id]["items"].append(path)
                group_map[group_id]["size"] += os.path.getsize(path)

        groups = list(group_map.values())

    else:
        raise ValueError("grouping must be 'file' or 'folder'")

    return groups