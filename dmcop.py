import torch
import mpi4py.MPI as MPI

comm = MPI.COMM_WORLD     
rank = comm.Get_rank()   
size = comm.Get_size()

class DMCOptimizer(torch.optim.Optimizer):
    """A wrapper around the optimizer used by user to perform all-reduce on gradients before parameter update"""
    def __init__(self, optimizer, grad_staleness):
        self.optimizer = optimizer
        self.step_count = 0 
        self.grad_staleness = grad_staleness
        super().__init__(optimizer.param_groups, defaults=getattr(optimizer, "defaults", {}))

    def step(self):
        self.step_count += 1

        for group in self.optimizer.param_groups:
            for p in group['params']:
               if p.grad is None:
                    continue

               # Yay, every parameter gets its own temp buffer (accum_grad) to accumulate gradients
               if not hasattr(p, 'accum_grad'):
                    p.accum_grad = torch.zeros_like(p.grad)

               # Accumulate gradients locally by adding
               # corresponding gradients to their personal accum_grad buffer
               p.accum_grad.add_(p.grad)
               #print(f"Grad before reduction → {p.grad.shape}")

        # Perform local gradient reduction to reduce network overhead. 
        # Accuracy loss is imminent but can be controlled by grad_staleness parameter
        if self.step_count % self.grad_staleness == 0:
           for group in self.optimizer.param_groups:
              for p in group['params']:
                if p.grad is None:
                    continue

                p.grad.copy_(p.accum_grad / self.grad_staleness)
                #print(f"Reduced (averaged) gradient → {p.grad.shape}")
                # Reset accumulator for next accumulation of gradients
                # for next cycle of grad_staleness steps
                p.accum_grad.zero_()

        
        # Inter-node gradient all-reduce begins here

        self.optimizer.step()
        self.optimizer.zero_grad()

    def zero_grad(self):
        self.optimizer.zero_grad()