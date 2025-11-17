import torch
from comms import RingCommunicator

PARAM_INFO = [] # metadata for parameters
START = 0
METADATA_AVAIL = False
PARAMETERS_ASSIGNED = False

class DMCOptimizer(torch.optim.Optimizer):
    """A wrapper around the optimizer used by user to perform all-reduce on gradients before parameter update. """
    def __init__(self, optimizer, grad_staleness, debug=False):
        self.optimizer = optimizer
        self.grad_staleness = grad_staleness
        self.step_count = 0
        super().__init__(optimizer.param_groups, defaults=getattr(optimizer, "defaults", {}))
        params = []
        for g in optimizer.param_groups:
            params.extend(g['params'])
        self.all_params = params # Store the collected parameters
        self.comm = RingCommunicator(debug=debug)

    def step(self) -> None:
        global METADATA_AVAIL, START, PARAMETERS_ASSIGNED # Declare METADATA_AVAIL and START as global here

        self.step_count += 1

        for group in self.optimizer.param_groups:
            for p in group['params']:
                if METADATA_AVAIL == False:
                    nbytes = p.numel() * p.element_size()
                    PARAM_INFO.append({
                        'shape': p.shape,
                        'nbytes': nbytes,
                        'start': START,
                        'end': START + nbytes,
                        'dtype': p.dtype
                    })
                    START += nbytes

                if p.grad is None:
                    continue

                # Yay, every parameter gets its own temp buffer (accum_grad) to accumulate gradients
                if not hasattr(p, 'accum_grad'):
                    p.accum_grad = torch.zeros_like(p.grad)

                # Accumulate gradients locally by adding
                # corresponding gradients to their personal accum_grad buffer
                p.accum_grad.add_(p.grad)
                #print(f"Grad before reduction → {p.grad.shape}")
            METADATA_AVAIL = True
            if METADATA_AVAIL == True and PARAMETERS_ASSIGNED == False:
                self.comm.assign_parameters(self.all_params, PARAM_INFO, total_bytes=START)
                PARAMETERS_ASSIGNED = True

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
            self.comm.ring_all_reduce(self.all_params, PARAM_INFO, total_bytes=START)

        self.optimizer.step()
        self.optimizer.zero_grad()

    def zero_grad(self) -> None:
        self.optimizer.zero_grad()
