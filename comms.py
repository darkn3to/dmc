import sys
import torch
import numpy as np
from math import ceil
import mpi4py.MPI as MPI

class RingCommunicator:
    """"We would be flattening the list of parameters into one big buffer.
    Then we would be slicing this buffer into chunks and assign
    them to different devices based on the size in bytes (yay, load imbalance handled) while also ensuring that the edge gradients (the ones near the end of the chunk) don't get cut midway. This is more efficient than straight away assigning parameters to devices in a contiguous manner because communicating N such parameters would require multiple small messages thus contributing to a lot of overhead. """
    def __init__(self, debug=False):
        #self.rank = 0 
        #self.ring_size = 4 
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.ring_size = self.comm.Get_size()
        self.leftNeighbor = (self.rank - 1 + self.ring_size) % self.ring_size
        self.rightNeighbor = (self.rank + 1) % self.ring_size
        self.chunk_ranges = {0: [0, 0]}
        self.param_to_rank = {i: [] for i in range(self.ring_size)}
        self.grad_buffer, self.send_buffer, self.recv_buffer = None, None, None
        self.debug = debug
        
    def populate_grad_buffer(self, parameters) -> None:
        """Flatten all gradients into a single buffer."""
        tensors = torch.cat([p.grad.reshape(-1) for p in parameters])
        self.grad_buffer = tensors.detach().cpu().numpy()
        #print(self.grad_buffer)

    def comms_neighbor(self, sChunkIndex, rChunkIndex) -> None:
        """Send and receive gradients to/from neighbors."""
        # populate the send buffer
        self.send_buffer = self.grad_buffer[self.chunk_ranges[sChunkIndex][0]:self.chunk_ranges[sChunkIndex][1]]
        # create the recv buffer
        self.recv_buffer = np.empty(self.chunk_ranges[rChunkIndex][1] - self.chunk_ranges[rChunkIndex][0])
        if self.debug:
            print(f"[Rank {self.rank}] Sendrecv START: send {len(self.send_buffer)} bytes → dest {self.rightNeighbor}")
            print(f"[Rank {self.rank}] Receive {len(self.recv_buffer)} bytes ← source {self.leftNeighbor}")

        self.comm.Sendrecv(sendbuf=self.send_buffer, dest=self.rightNeighbor, recvbuf=self.recv_buffer, source=self.leftNeighbor)
        if self.debug:
            print(f"[Rank {self.rank}] Sendrecv DONE: recv_buffer first 20 bytes: {self.recv_buffer[:20]}")

    
    def assign_parameters(self, parameters, param_info, total_bytes) -> None:
        """Greedily assign chunks of parameters to all devices."""
        rank = 0
        current_load = 0
        target = ceil(total_bytes / self.ring_size)

        index, nbytes = 0, 0
        param_info_len = len(param_info)

        while index < param_info_len:
            if nbytes == 0:
                nbytes = param_info[index]["nbytes"]

            load_can_be_added = min(nbytes, target - current_load)
            current_load += load_can_be_added
            nbytes -= load_can_be_added
            self.chunk_ranges[rank][1] += load_can_be_added

            self.param_to_rank[rank].append((index, load_can_be_added))

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
        for i, assigns in self.param_to_rank.items():
            if not assigns:
                continue
            pretty = " | ".join([f"rank {i} (parameter {r} )" for r, b in assigns])
            print(pretty)
        
        
        print(self.ring_size, rank, self.chunk_ranges)
        for j in self.chunk_ranges:
            print(f"Rank {j} assigned bytes from {self.chunk_ranges[j][0]} to {self.chunk_ranges[j][1]}")
        '''
        
    def ring_all_reduce(self, parameters, param_info, total_bytes) -> None:
        """Perform ring all-reduce on the flattened gradient buffer."""
        self.populate_grad_buffer(parameters)
        if self.debug:
            print(f"[Rank {self.rank}] Initial grad_buffer len={len(self.grad_buffer)}")
            print(f"[Rank {self.rank}] First 50 bytes: {self.grad_buffer[:50]}")

        # Reduce-scatter phase
        for i in range(self.ring_size - 1):
            sChunkIndex = (self.rank - i) % self.ring_size
            rChunkIndex = (self.rank - i - 1) % self.ring_size
            if self.debug:
                print(f"\n[Rank {self.rank}] ---- RS ITER {i} ----")
                print(f"[Rank {self.rank}] sChunkIndex={sChunkIndex}, rChunkIndex={rChunkIndex}")
                print(f"[Rank {self.rank}] Sending bytes [{self.chunk_ranges[sChunkIndex][0]}, {self.chunk_ranges[sChunkIndex][1]})")
                print(f"[Rank {self.rank}] Expect recv bytes [{self.chunk_ranges[rChunkIndex][0]}, {self.chunk_ranges[rChunkIndex][1]})")

            self.comms_neighbor(sChunkIndex, rChunkIndex)
            for i in self.param_to_rank[rChunkIndex]:
                info = param_info[i[0]]
                param = parameters[i[0]]
                assigned_bytes = i[1]

                slice_lo = max(info['start'], self.chunk_ranges[rChunkIndex][0])
                slice_hi = min(info['end'], self.chunk_ranges[rChunkIndex][1])
                if slice_lo >= slice_hi:
                    continue

                recv_lo = (slice_lo - self.chunk_ranges[rChunkIndex][0]) // info['dtype'].itemsize
                recv_hi = (slice_hi - self.chunk_ranges[rChunkIndex][0]) // info['dtype'].itemsize
                if self.debug:
                    print(f"[Rank {self.rank}] Param {i[0]} slice_lo={slice_lo}, slice_hi={slice_hi}")
                    print(f"[Rank {self.rank}] recv_lo={recv_lo}, recv_hi={recv_hi}")
                    print(f"[Rank {self.rank}] Adding {recv_hi - recv_lo} elements to param")

                if recv_lo == recv_hi:
                    print(f"[Rank {self.rank}] EMPTY SLICE for param {i[0]} — skip")
                    continue

                grad_tensor = torch.tensor(self.recv_buffer[recv_lo:recv_hi], dtype=param.grad.dtype, device=param.grad.device).reshape(-1)
                flat = param.grad.view(-1)

                flat[(slice_lo - info['start'])//info['dtype'].itemsize:(slice_hi - info['start'])//info['dtype'].itemsize] += grad_tensor

        if self.debug:
            print(f"\n[Rank {self.rank}] ---- AFTER REDUCE-SCATTER ----")
            for r in range(self.ring_size):
                print(f"[Rank {self.rank}] chunk[{r}] = {self.chunk_ranges[r]}")

        '''
        # All-gather phase
        for i in range(self.ring_size - 1):
            sChunkIndex = (self.rank - i) % self.ring_size
            rChunkIndex = (self.rank - i - 1) % self.ring_size
            self.comms_neighbor(sChunkIndex, rChunkIndex)

            self.grad_buffer[self.chunk_ranges[rChunkIndex][0]:self.chunk_ranges[rChunkIndex][1]] = self.recv_buffer

        # now we have one large flattened gradient buffer with all reduced gradients
        # that can be used to train our model. Yay!!
        for i, j in zip(param_info, parameters):
            j.grad.copy_((torch.tensor(self.grad_buffer[i['start']//i['dtype'].itemsize:i['end']//i['dtype'].itemsize], dtype=j.grad.dtype, device=j.grad.device).reshape(i['shape'])) / self.ring_size)
        '''


            





        


