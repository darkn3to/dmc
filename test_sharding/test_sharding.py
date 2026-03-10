from compressor import get_compressor

original_file = "test.txt"
compressed_file = "test.txt.zstd"
decompressed_file = "test_out.txt"

# create test file
with open(original_file, "w") as f:
    f.write("DMC sharding compression test")

compressor = get_compressor("zstd")

compressor.compress(original_file, compressed_file)
print("Compression done")

compressor.decompress(compressed_file, decompressed_file)
print("Decompression done")

with open(original_file) as f1, open(decompressed_file) as f2:
    if f1.read() == f2.read():
        print("SUCCESS: Files match")
    else:
        print("ERROR: Files do not match")