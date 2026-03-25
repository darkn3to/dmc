import json
import os
import requests
import utils
import tarfile
from dmc_sharding.compressor import get_compressor

SHARD_ARCHIVE_DIR = "data/shards_archives"
SHARD_EXTRACT_DIR = "data/shards"


def load_placement_map(path="broadcast/placement_map.json"):
    with open(path, "r") as f:
        return json.load(f)


def get_my_shards(placement_map, my_ip):
    if my_ip not in placement_map:
        raise ValueError(f"{my_ip} not found in placement map")

    primary = placement_map[my_ip]["primary"]
    replica = placement_map[my_ip]["replica"]

    return primary + replica


def download_shard(master_ip, shard_id, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    url = f"http://{master_ip}:8000/shards_output/shard_{shard_id}.tar.zstd"
    local_path = os.path.join(output_dir, f"shard_{shard_id}.tar.zstd")
    temp_path = local_path + ".tmp"

    if os.path.exists(local_path):
        print(f"[Worker] Shard {shard_id} already downloaded. Skipping.")
        return local_path

    print(f"[Worker] Downloading shard {shard_id}...")

    for attempt in range(3):
        try:
            response = requests.get(url, stream=True, timeout=10)

            if response.status_code != 200:
                raise Exception("Bad response")
            
            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            os.rename(temp_path, local_path)
            print(f"[Worker] Shard {shard_id} downloaded.")
            return local_path

        except Exception as e:
            print(f"[Retry {attempt+1}] Failed: {e}")

    print(f"[Error] Failed to download shard {shard_id}")
    return None


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

def pull_shards(master_ip, placement_file, archive_dir, extract_dir):
    os.makedirs(archive_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    placement_map = load_placement_map(placement_file)
    my_ip = utils.find_own_ip()

    print(f"[Worker] My IP: {my_ip}")

    shards = get_my_shards(placement_map, my_ip)
    print(f"[Worker] Need shards: {shards}")

    compressor = get_compressor("zstd")

    for shard_id in shards:
        archive_path = download_shard(master_ip, shard_id, archive_dir)

        if archive_path:
            decompress_and_extract(archive_path, extract_dir, compressor)

if __name__ == "__main__":
    ips = utils.parse_ips("nodes.txt")
    if not ips:
        raise ValueError("No IPs found in nodes.txt")

    pull_shards(
        master_ip=ips[0],
        placement_file="placement_map.json",
        archive_dir="./local_archives",
        extract_dir="./local_shards"
    )