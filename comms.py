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

    def comms_neighbor(self, sChunkIndex, rChunkIndex, phase) -> None:
        """Send and receive gradients to/from neighbors."""
        if phase == "reduce-scatter":
            start_e = self.chunk_ranges[sChunkIndex][0] // self.elem_size
            end_e   = self.chunk_ranges[sChunkIndex][1] // self.elem_size
            self.send_buffer = self.grad_buffer[start_e:end_e]

        elif phase == "all-gather":
            # SEND ONLY MY REDUCED CHUNK
            self.send_buffer = self.reduced_chunk

        recv_bytes = self.chunk_ranges[rChunkIndex][1] - self.chunk_ranges[rChunkIndex][0]
        self.recv_buffer = np.empty(
            recv_bytes // self.elem_size,
            dtype=self.grad_buffer.dtype
        )

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
        if not hasattr(self, "elem_size"):
            self.elem_size = parameters[0].grad.element_size()
        elem_size = self.elem_size

        self.populate_grad_buffer(parameters)


        # buffer used ONLY for reduce-scatter accumulation
        if not hasattr(self, "rs_buffer"):
            self.rs_buffer = np.zeros_like(self.grad_buffer)
        else:
            self.rs_buffer.fill(0)

        # each rank contributes its own local chunk first
        my_start = self.chunk_ranges[self.rank][0] // elem_size
        my_end   = self.chunk_ranges[self.rank][1] // elem_size
        self.rs_buffer[my_start:my_end] += self.grad_buffer[my_start:my_end]

        if self.debug:
            print(f"[Rank {self.rank}] Initial grad_buffer len={len(self.grad_buffer)}")
            print(f"[Rank {self.rank}] First 50 bytes: {self.grad_buffer[:50]}")

        # Debug: Ensure assign_parameters was called
        assert self.chunk_ranges, "chunk_ranges is empty. Ensure assign_parameters is called before ring_all_reduce."

        # Reduce-scatter phase
        for i in range(self.ring_size - 1):
            sChunkIndex = (self.rank - i) % self.ring_size
            rChunkIndex = (self.rank - i - 1) % self.ring_size
            if self.debug:
                print(f"\n[Rank {self.rank}] ---- RS ITER {i} ----")
                print(f"[Rank {self.rank}] sChunkIndex={sChunkIndex}, rChunkIndex={rChunkIndex}")
                print(f"[Rank {self.rank}] Sending bytes [{self.chunk_ranges[sChunkIndex][0]}, {self.chunk_ranges[sChunkIndex][1]})")
                print(f"[Rank {self.rank}] Expect recv bytes [{self.chunk_ranges[rChunkIndex][0]}, {self.chunk_ranges[rChunkIndex][1]})")

            self.comms_neighbor(sChunkIndex, rChunkIndex, phase="reduce-scatter")
        
            recv_start = self.chunk_ranges[rChunkIndex][0] // elem_size
            recv_end   = self.chunk_ranges[rChunkIndex][1] // elem_size
            self.rs_buffer[recv_start:recv_end] += self.recv_buffer


  
        # ---- SAVE MY REDUCED CHUNK (ONLY ONCE) ----
        my_start, my_end = self.chunk_ranges[self.rank]
        my_start //= elem_size
        my_end   //= elem_size

        self.reduced_chunk = self.rs_buffer[my_start:my_end].copy()

        if self.debug:
            print(f"\n[Rank {self.rank}] ---- AFTER REDUCE-SCATTER ----")
            for r in range(self.ring_size):
                print(f"[Rank {self.rank}] chunk[{r}] = {self.chunk_ranges[r]}")

        # All-gather phase
        current_chunk = self.reduced_chunk

        for i in range(self.ring_size - 1):
            sChunkIndex = (self.rank - i) % self.ring_size
            rChunkIndex = (self.rank - i - 1) % self.ring_size

            self.send_buffer = current_chunk

            recv_bytes = self.chunk_ranges[rChunkIndex][1] - self.chunk_ranges[rChunkIndex][0]
            self.recv_buffer = np.empty(
                recv_bytes // elem_size,
                dtype=self.grad_buffer.dtype
            )

            self.comm.Sendrecv(
                sendbuf=self.send_buffer,
                dest=self.rightNeighbor,
                recvbuf=self.recv_buffer,
                source=self.leftNeighbor
            )

            # place received chunk
            start_b, end_b = self.chunk_ranges[rChunkIndex]

            start_e = start_b // elem_size
            end_e   = end_b   // elem_size

            self.grad_buffer[start_e:end_e] = self.recv_buffer


            # ownership rotates
            current_chunk = self.recv_buffer


        # now we have one large flattened gradient buffer with all reduced gradients
        # that can be used to train our model. Yay!!
        for i, j in zip(param_info, parameters):
            j.grad.copy_((torch.tensor(self.grad_buffer[i['start']//i['dtype'].itemsize:i['end']//i['dtype'].itemsize], dtype=j.grad.dtype, device=j.grad.device).reshape(i['shape'])) / self.ring_size)












