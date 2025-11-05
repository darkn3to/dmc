import torch
import mpi4py

class DMCOptimizer(torch.optim.Optimizer):
    def __init__(self, optimizer):
        self.optimizer = optimizer
        super().__init__(optimizer.param_groups, defaults=getattr(optimizer, "defaults", {}))

    def step(self):
        for group in self.optimizer.param_groups:
            for p in group['params']:
                '''if p.grad is None:
                    continue
                else:
                    print(f"{p.data}'s gradient before allreduce: {p.grad}")'''

        # all-reduce logic here    
        self.optimizer.step()

    def zero_grad(self):
        self.optimizer.zero_grad()

        
