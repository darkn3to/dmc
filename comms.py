import sys
import torch
import random
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
        self.ring_size = 5 # Assuming ring size 1 for a single node (no actual ring communication)

        #self.comm = MPI.COMM_WORLD
        #self.rank = self.comm.Get_rank()
        #self.ring_size = self.comm.Get_size()

        '''
        # Pre-shuffle parameters to avoid imbalance due to
        # clustered parameters (parameters of similar sizes together)
        rng = random.Random(RANDOM_SEED)
        #torch.manual_seed(RANDOM_SEED)
        self.parameters = parameters.copy()
        rng.shuffle(self.parameters)

        self.avg_size = [p.numel() for p in self.parameters]
        self.avg_size = sum(self.avg_size) // self.ring_size
        '''

        #self.leftNeighbor = (self.rank - 1 + self.ring_size) % self.ring_size
        #self.rightNeighbor = (self.rank + 1) % self.ring_size

    def set_metadata(self):
        dummy = 0

    def comms_neighbor(self):
        dummy = 0

    def assign_parameters(self, parameters, param_info, total_bytes) -> None:
        chunk_ranges = dict()
        current_load = 0
        chunk_start = 0
        rank = 0
        target = total_bytes / self.ring_size
        for j in param_info:
            if current_load + j['nbytes'] > target and rank < self.ring_size - 1:
                chunk_ranges[rank] = (chunk_start, j['start'])
                chunk_start = j['start']
                rank += 1
                current_load = 0
            current_load += j['nbytes']
        chunk_ranges[rank] = (chunk_start, total_bytes)

        for j in chunk_ranges:
            print(f"Rank {j} assigned bytes from {chunk_ranges[j][0]} to {chunk_ranges[j][1]}")

    def ring_all_reduce(self, parameters, param_info, total_bytes) -> int:
        self.comms_neighbor()