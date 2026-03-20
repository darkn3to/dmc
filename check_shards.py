import os
import tarfile
from collections import Counter
from dmc_sharding.compressor import get_compressor

SHARD_DIR = "dataset3_folder_output"
TEMP_TAR = "temp.tar"


def inspect_shard(shard_path):
    print(f"\n🔍 Inspecting: {shard_path}")

    compressor = get_compressor("zstd")

    # Step 1: decompress
    compressor.decompress(shard_path, TEMP_TAR)

    # Step 2: analyze structure
    structure = set()
    counter = Counter()

    with tarfile.open(TEMP_TAR, "r") as tar:
        for member in tar.getmembers():

            parts = member.name.split("/")

            # Top-level folders
            if len(parts) >= 2:
                structure.add(parts[0])

                # Count files per class (real/fake)
                if not member.isdir():
                    counter[parts[0]] += 1

    print("Top-level folders:", structure)
    print("Class distribution:", counter)

    os.remove(TEMP_TAR)


def main():
    for file in os.listdir(SHARD_DIR):
        if file.endswith(".zstd"):
            inspect_shard(os.path.join(SHARD_DIR, file))


if __name__ == "__main__":
    main()