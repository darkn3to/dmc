from dmc_sharding.compressor import get_compressor
import os

# files
original_file = "test.txt"
compressed_file = "test.txt.zstd"
decompressed_file = "test_out.txt"

# create a small test file
with open(original_file, "w") as f:
    f.write("This is a compression test for DMC sharding project.")

compressor = get_compressor("zstd")

# compress
compressor.compress(original_file, compressed_file)
print("Compression done")

# decompress
compressor.decompress(compressed_file, decompressed_file)
print("Decompression done")

# verify
with open(original_file) as f1, open(decompressed_file) as f2:
    if f1.read() == f2.read():
        print("SUCCESS: Files match")
    else:
        print("ERROR: Files do not match")