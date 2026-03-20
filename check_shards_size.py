import os

output_folder = "dataset3_folder_output"   # change if your folder name is different

print("Checking shard sizes...\n")

for f in os.listdir(output_folder):
    path = os.path.join(output_folder, f)
    size = os.path.getsize(path)
    print(f, round(size / (1024*1024), 2), "MB")
