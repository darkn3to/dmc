import sys
import torch
from math import ceil
import mpi4py.MPI as MPI

#RANDOM_SEED = 42

class RingCommunicator:
    """"We would be flattening the list of parameters into one big buffer.
    Then we would be slicing this buffer into chunks and assign
    them to different devices based on the size in bytes (yay, load imbalance handled) while also ensuring that the edge gradients (the ones near the end of the chunk) don't get cut midway. This is more efficient than straight away assigning parameters to devices in a contiguous manner because communicating N such parameters would require multiple small messages thus contributing to a lot of overhead. """
    def __init__(self):
        # These were commented out, assuming a single-node setup for now
        # If distributed training is intended, MPI needs to be set up.
        self.rank = 0  # Assuming rank 0 for a single node
        self.ring_size = 4 # Assuming ring size 1 for a single node (no actual ring communication)

        #self.comm = MPI.COMM_WORLD
        #self.rank = self.comm.Get_rank()
        #self.ring_size = self.comm.Get_size()
        #self.leftNeighbor = (self.rank - 1 + self.ring_size) % self.ring_size
        #self.rightNeighbor = (self.rank + 1) % self.ring_size

    def set_metadata(self):
        dummy = 0

    def comms_neighbor(self):
        dummy = 0

    def assign_parameters(self, parameters, param_info, total_bytes) -> None:
        """Greedily assign chunks of parameters to all devices."""
        chunk_ranges = {0: [0, 0]}
        rank = 0
        current_load = 0
        target = ceil(total_bytes / self.ring_size)

        index, nbytes = 0, 0
        param_info_len = len(param_info)
        param_to_rank = {i: [] for i in range(param_info_len)}  

        while index < param_info_len:
            if nbytes == 0:
                nbytes = param_info[index]["nbytes"]

            load_can_be_added = min(nbytes, target - current_load)
            current_load += load_can_be_added
            nbytes -= load_can_be_added
            chunk_ranges[rank][1] += load_can_be_added

            # 🔹 Record that 'index'th parameter bytes went to this rank
            param_to_rank[index].append((rank, load_can_be_added))

            # move to next parameter if fully consumed
            if nbytes == 0:
                index += 1

            # when target full, move to next rank
            if current_load >= target:
                rank += 1
                if rank >= self.ring_size:
                    break
                current_load = 0
                chunk_ranges[rank] = [chunk_ranges[rank - 1][1], chunk_ranges[rank - 1][1]]
        
        '''
        print(f"\n=== {len(parameters)} Parameter → Rank Assignment ===")
        for i, assigns in param_to_rank.items():
            if not assigns:
                continue
            pretty = " | ".join([f"rank {r} ({b} bytes)" for r, b in assigns])
            print(f"Param {i+1} ({param_info[i]['nbytes']} bytes originally): {pretty}")

        print(self.ring_size, rank, chunk_ranges)
        for j in chunk_ranges:
            print(f"Rank {j} assigned bytes from {chunk_ranges[j][0]} to {chunk_ranges[j][1]}")
        '''

    def ring_all_reduce(self, parameters, param_info, total_bytes) -> int:
        self.comms_neighbor()