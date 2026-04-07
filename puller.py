import argparse
import os
import requests
import utils
import tarfile
import subprocess
from dmc_sharding.compressor import get_compressor

SHARD_ARCHIVE_DIR = "data/shards_archives"

def download_shard(master_ip, shard_id, output_dir, dmc_folder_path):
    os.makedirs(output_dir, exist_ok=True)
    url = f"http://{master_ip}:8000/{dmc_folder_path}/shards_output/shard_{shard_id}.tar.zstd"
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

def pull_shards(master_ip, archive_dir, extract_dir, dmc_folder_path):
    os.makedirs(archive_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    placement_map = utils.load_placement_map()
    my_ip = utils.find_own_ip()

    print(f"[Worker] My IP: {my_ip}")

    shards = utils.get_my_shards(placement_map, my_ip, preprocess=True)
    print(f"[Worker] Need shards: {shards}")

    compressor = get_compressor("zstd")

    for shard_id in shards:
            if my_ip != master_ip:
                archive_path = download_shard(master_ip, shard_id, archive_dir, dmc_folder_path)

                if archive_path:
                    decompress_and_extract(archive_path, extract_dir, compressor)
            else:
                shards_output_dir = os.path.join(dmc_folder_path, "shards_output")
                shard_file = os.path.join(shards_output_dir, f"shard_{shard_id}.tar.zstd")
                
                if os.path.exists(shard_file):
                    print(f"[Master] Processing local shard {shard_id} from {shard_file}")
                    decompress_and_extract(shard_file, extract_dir, compressor)
                else:
                    print(f"[Master] Shard {shard_id} not found in {shards_output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dmc_folder_path", help="Path to the DMC folder")
    args = parser.parse_args()

    ips = utils.parse_ips("nodes.txt")
    if not ips:
        raise ValueError("No IPs found in nodes.txt")

    if ips[0] == utils.find_own_ip():
        server=subprocess.Popen(["python3", "-m", "http.server", "8000"])

    pull_shards(
        master_ip=ips[0],
        archive_dir="./local_archives",
        extract_dir="./local_shards",
        dmc_folder_path=args.dmc_folder_path
    )

    if ips[0] == utils.find_own_ip():
        server.terminate()
