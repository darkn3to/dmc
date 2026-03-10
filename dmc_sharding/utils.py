import os

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def estimate_file_size(path: str) -> int:
    return os.path.getsize(path)
