import os
import tarfile
from dmc_sharding.compressor import get_compressor

SHARD_DIR = "dataset3_output"
OUTPUT_DIR = "worker_test3_output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Loading compressor...")
compressor = get_compressor("zstd")

for file in os.listdir(SHARD_DIR):

    if file.endswith(".zstd"):

        shard_path = os.path.join(SHARD_DIR, file)

        print("Processing:", shard_path)

        tar_path = shard_path.replace(".zstd", "")

        # Decompress
        compressor.decompress(shard_path, tar_path)

        # Extract TAR
        with tarfile.open(tar_path, "r") as tar:
            tar.extractall(OUTPUT_DIR)

        print("Finished:", file)

print("All shards decompressed and extracted.")