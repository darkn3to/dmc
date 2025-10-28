import os
import numpy as np
from mpi4py import MPI

communicator = MPI.COMM_WORLD
rank = communicator.Get_rank()

FILES = None

def broadcast_files(filename: str) -> None:
    """Broadcasts a single file from the master node (rank 0) to all worker nodes."""
    buffer = None
    filesize = None

    if rank == 0:
        # Only the master node reads the file and prepares the buffer
        print(f"[Master] Broadcasting file: {filename}")
        with open(filename, "rb") as f:
            buffer = f.read()
            filesize = len(buffer)
        print(f"[Master] File size: {filesize} bytes")

    # Broadcast the file size to all nodes
    # blocking in nature. Master broadcasts, workers listen.
    filesize = communicator.bcast(filesize, root=0)

    # Worker nodes allocate buffer space
    if rank != 0:
        buffer = bytearray(filesize)

    buf_array = np.frombuffer(buffer, dtype=np.byte)

    # Broadcast the file content
    communicator.Bcast(buf_array, root=0)

    if rank != 0:
        # Worker nodes save the received file
        buffer = buf_array.tobytes()
        os.makedirs("broadcast_received", exist_ok=True)
        worker_filename = os.path.join("broadcast_received", os.path.basename(filename))
        with open(worker_filename, "wb") as f:
            f.write(buffer)
        print(f"[Worker {rank}] Saved file as '{worker_filename}'")


def broadcast_directory(directory: str) -> None:
    """Broadcasts all files in the specified directory."""
    FILES = None

    if rank == 0:
        # Master node lists all files in the directory
        FILES = [os.path.join(directory, file) for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
        print(f"[Master] Files to broadcast: {FILES}")

    # Broadcast the list of files to all nodes
    FILES = communicator.bcast(FILES, root=0)

    # Broadcast each file
    for file in FILES:
        broadcast_files(file)


if __name__ == "__main__":
    # Directory to broadcast
    broadcast_dir = "broadcast"

    if rank == 0:
        print(f"[Master] Broadcasting all files in directory: {broadcast_dir}")

    broadcast_directory(broadcast_dir)