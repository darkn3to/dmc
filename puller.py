import os
import tarfile
from dmc_sharding.compressor import get_compressor

SHARD_SOURCE_DIR = "shards_output"
SHARD_EXTRACT_DIR = "local_shards"

def safe_extract(tar, path):
    for member in tar.getmembers():
        member_path = os.path.join(path, member.name)
        if not os.path.abspath(member_path).startswith(os.path.abspath(path)):
            raise Exception("Unsafe tar extraction detected")
    tar.extractall(path)

def decompress_and_extract(shard_archive_path, extract_base_dir, compressor):
    filename = os.path.basename(shard_archive_path)
    shard_id = filename.split("_")[1].split(".")[0]

    shard_extract_path = os.path.join(extract_base_dir, f"shard_{shard_id}")

    # Skip if already extracted
    if os.path.exists(shard_extract_path) and os.listdir(shard_extract_path):
        print(f"[Worker] Shard {shard_id} already extracted. Skipping.")
        return

    os.makedirs(shard_extract_path, exist_ok=True)

    print(f"[Worker] Extracting shard {shard_id}...")

    tar_path = shard_archive_path.replace(".zstd", "")

    # Decompress zstd → tar
    compressor.decompress(shard_archive_path, tar_path)
    with tarfile.open(tar_path, "r:*") as tar:
        safe_extract(tar, shard_extract_path)

    if os.path.exists(tar_path):
        os.remove(tar_path)

    print(f"[Worker] Shard {shard_id} extracted.")

def pull_shards(source_dir, extract_dir):
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"Shard source directory not found: {source_dir}")

    os.makedirs(extract_dir, exist_ok=True)

    compressor = get_compressor("zstd")

    for file_name in sorted(os.listdir(source_dir)):
        if not file_name.endswith(".zstd"):
            continue

        shard_path = os.path.join(source_dir, file_name)
        print(f"[Worker] Processing local shard archive: {shard_path}")
        decompress_and_extract(shard_path, extract_dir, compressor)

if __name__ == "__main__":
    pull_shards(
        source_dir=SHARD_SOURCE_DIR,
        extract_dir=SHARD_EXTRACT_DIR,
    )