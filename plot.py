import pandas as pd
import matplotlib.pyplot as plt

files = {
    "1node": "metrics_1node.csv",
    "2node_ddp": "metrics_multinode_nonTau.csv",
    "2node_dmc": "metrics_multinode_tau.csv"
}

data = {}

# Load all files
for name, file in files.items():
    df = pd.read_csv(file)
    df["setup"] = name
    df["epoch"] = pd.to_numeric(df["epoch"], errors="coerce")
    df["loss"] = pd.to_numeric(df["loss"], errors="coerce")
    df["epoch_time"] = pd.to_numeric(df["epoch_time"], errors="coerce")
    df["forward_time"] = pd.to_numeric(df["forward_time"], errors="coerce")
    df["backward_time"] = pd.to_numeric(df["backward_time"], errors="coerce")
    df = df.dropna(subset=["epoch", "loss", "epoch_time", "forward_time", "backward_time"])
    df["comm_time"] = df["backward_time"] - df["forward_time"]
    data[name] = df

# -------------------------
# SPEEDUP
# -------------------------
final_times = {}

for name, df in data.items():
    final_times[name] = df["epoch_time"].mean()

T1 = final_times["1node"]

for name in final_times:
    print(f"{name} speedup: {T1 / final_times[name]:.2f}x")

# -------------------------
# COMM vs COMPUTE
# -------------------------
for name, df in data.items():
    avg_forward = df["forward_time"].mean()
    avg_comm = df["comm_time"].mean()

    print(f"{name}: Compute={avg_forward:.2f}, Comm={avg_comm:.2f}")

# -------------------------
# LOSS CURVE
# -------------------------
for name, df in data.items():
    plt.plot(df["epoch"], df["loss"], label=name)

plt.legend()
plt.title("Loss vs Epoch")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.show()

plt.bar(final_times.keys(), final_times.values())
plt.title("Training Time Comparison")
plt.ylabel("Time (s)")
plt.show()

labels = []
compute_vals = []
comm_vals = []

for name, df in data.items():
    labels.append(name)
    compute_vals.append(df["forward_time"].mean())
    comm_vals.append(df["comm_time"].mean())

plt.bar(labels, compute_vals, label="Compute")
plt.bar(labels, comm_vals, bottom=compute_vals, label="Comm")

plt.legend()
plt.title("Compute vs Communication")
plt.show()