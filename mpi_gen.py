import os

def parse_nodes(file_path="nodes.txt"):
    nodes = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Expected format: [username]: ipaddress
            try:
                username = line.split("]:")[0].strip("[]")
                ip = line.split("]:")[1].strip()
                nodes.append((username, ip))
            except Exception as e:
                print(f"Skipping malformed line: {line} ({e})")
    return nodes

def generate_mpirun_command(nodes, script_path="broadcast.py"):
    base = "/usr/bin/mpirun"
    parts = []

    for i, (user, ip) in enumerate(nodes):
        host = "localhost" if os.getlogin() == user else ip
        part = f"-np 1 --host {host} python3 /home/{user}/dmc/{script_path}"
        parts.append(part)

    joined = " : \\\n  ".join(parts)
    cmd = f"{base} \\\n  {joined}"
    return cmd

if __name__ == "__main__":
    nodes = parse_nodes()
    cmd = generate_mpirun_command(nodes)
    print("\nGenerated MPI Run Command:\n")
    print(cmd)
