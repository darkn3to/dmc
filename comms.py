import sys
import torch
import numpy as np
from math import ceil
import mpi4py.MPI as MPI

class RingCommunicator:
    """"We would be flattening the list of parameters into one big buffer.
    Then we would be slicing this buffer into chunks and assign
    them to different devices based on the size in bytes (yay, load imbalance handled) while also ensuring that the edge gradients (the ones near the end of the chunk) don't get cut midway. This is more efficient than straight away assigning parameters to devices in a contiguous manner because communicating N such parameters would require multiple small messages thus contributing to a lot of overhead. """
    def __init__(self):
        '''
        self.rank = 0 
        self.ring_size = 4 
        '''
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.ring_size = self.comm.Get_size()
        self.leftNeighbor = (self.rank - 1 + self.ring_size) % self.ring_size
        self.rightNeighbor = (self.rank + 1) % self.ring_size
        self.chunk_ranges = {0: [0, 0]}
        self.grad_buffer, self.send_buffer, self.recv_buffer = None, None, None

    def populate_grad_buffer(self, parameters) -> None:
        """Flatten all gradients into a single buffer."""
        tensors = torch.cat([p.grad.reshape(-1) for p in parameters])
        self.grad_buffer = tensors.detach().cpu().numpy()
        #print(self.grad_buffer)

    def comms_neighbor(self) -> None:
        """Send and receive gradients to/from neighbors."""
        # populate the send buffer
        self.send_buffer = self.grad_buffer[self.chunk_ranges[self.rank][0]:self.chunk_ranges[self.rank][1]]
        # populate the recv buffer
        self.recv_buffer = np.empty(self.chunk_ranges[self.leftNeighbor][1] - self.chunk_ranges[self.leftNeighbor][0])

        self.comm.Sendrecv(sendbuf=self.send_buffer, dest=self.rightNeighbor, recvbuf=self.recv_buffer, source=self.leftNeighbor)
    
    def assign_parameters(self, parameters, param_info, total_bytes) -> None:
        """Greedily assign chunks of parameters to all devices."""
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
            self.chunk_ranges[rank][1] += load_can_be_added

            param_to_rank[index].append((rank, load_can_be_added))

            if nbytes == 0:
                index += 1

            if current_load >= target:
                rank += 1
                if rank >= self.ring_size:
                    break
                current_load = 0
                self.chunk_ranges[rank] = [self.chunk_ranges[rank - 1][1], self.chunk_ranges[rank - 1][1]]
        
        '''
        print(f"\n=== {len(parameters)} Parameter → Rank Assignment ===")
        for i, assigns in param_to_rank.items():
            if not assigns:
                continue
            pretty = " | ".join([f"rank {r+1} ({b} bytes)" for r, b in assigns])
            print(f"Param {i+1} ({param_info[i]['nbytes']} bytes originally): {pretty}")
        

        print(self.ring_size, rank, self.chunk_ranges)
        for j in self.chunk_ranges:
            print(f"Rank {j} assigned bytes from {self.chunk_ranges[j][0]} to {self.chunk_ranges[j][1]}")
        '''
        
    def ring_all_reduce(self, parameters, param_info, total_bytes) -> None:
        """Perform ring all-reduce on the flattened gradient buffer."""
        self.populate_grad_buffer(parameters)
        self.comms_neighbor()

        # Update the local grad_buffer with received data
        for i, j in zip(param_info, parameters):
            grad_tensor = torch.tensor(self.recv_buffer[i['start']//i['dtype'].itemsize:i['end']//i['dtype'].itemsize], dtype=j.grad.dtype, device=j.grad.device).reshape(i['shape'])
            j.grad.copy_(grad_tensor)
        


