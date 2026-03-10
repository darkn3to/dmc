import tarfile
from compressor import get_compressor
import os

compressed_shard = "cifar_shards_output/shard_0.tar.zstd"
decompressed_tar = "shard_0.tar"
output_folder = "worker_dataset"

compressor = get_compressor("zstd")

print("Step 1: Decompressing shard...")
compressor.decompress(compressed_shard, decompressed_tar)

print("Step 2: Extracting dataset...")

os.makedirs(output_folder, exist_ok=True)

with tarfile.open(decompressed_tar) as tar:
    tar.extractall(output_folder)

print("Step 3: Checking extracted files...")

total_files = 0

for root, dirs, files in os.walk(output_folder):
    total_files += len(files)

print("Total files extracted:", total_files)

if total_files > 0:
    print("SUCCESS: Worker can read dataset")
else:
    print("ERROR: Dataset extraction failed")