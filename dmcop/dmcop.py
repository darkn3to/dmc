import torch
import math
from tests.tests import Tests
from logger import Logger

class DMC(torch.optim.Optimizer):
    """
    The DMC class enhances a user-provided optimizer by incorporating distributed model compression (DMC) techniques.

    Functionalities:
    1. Periodically synchronizes model parameters across all processes using `torch.distributed.all_reduce`.
    2. Adapts the communication frequency (`tau`) dynamically based on the loss value, balancing communication overhead and model convergence.
    3. Reduces scalar values (e.g., loss) across all processes using an average reduction operation.
    4. Provides optional debugging logs and parameter consistency tests to ensure correctness during distributed training.
    """
    def __init__(self, model, optimizer, backend, tau, debug=False, tests=False, intercept_print=False):
        self.model = model
        self.inner_optimizer = optimizer
        self.tests = tests
        self.debug = debug

        super().__init__(optimizer.param_groups, defaults=getattr(optimizer, "defaults", {}))

        if not torch.distributed.is_initialized():
            torch.distributed.init_process_group(backend=backend)

        params = []
        for g in self.inner_optimizer.param_groups:
            params.extend(g['params'])

        self.step_count = 0
        self.all_params = params
        
        '''
        self._ddp = torch.nn.parallel.DistributedDataParallel(
                model, 
                device_ids = [torch.cuda.current_device()],
                broadcast_buffers = False
            )
        '''
        self.rank = torch.distributed.get_rank()
        self.world_size = torch.distributed.get_world_size()

        self.logger = Logger(self.rank, intercept_print=intercept_print)

        # tau parameters control the communication freq.
        # Inital tau is user-defined.
        # More the tau, lesser the communication freq.
        self.tau = tau
        self.tau_0 = tau
        self.loss_0 = None
        self.tau_min = 1
        self.tau_max = 100

        # Hard cap on max local steps to avoid local divergence.
        self.max_local_steps = 500

    def _log(self, *args, rank0_only=False) -> None:
        message = " ".join(map(str, args))
        self.logger.log(message, rank0_only=rank0_only)

    # Convert a scalar loss value to a tensor and reduce the 
    # loss by averaging across all processes. Collectives require tensors.   
    def reduce_scalar(self, value) -> float:
        t = torch.tensor(value, 
                        device=self.all_params[0].device,
                        dtype=torch.float32)
        torch.distributed.all_reduce(t, op=torch.distributed.ReduceOp.AVG)
        return t.item()
    
    def step(self, closure=None):
        loss = None
        if closure is not None:
            loss = closure()

        # Local optimizer step
        self.inner_optimizer.step()

        # DMC logic uses loss.item() if provided
        if loss is not None:
            loss_value = loss.item()
        else:
            loss_value = None

        self._dmc_step(loss_value)

        return loss


    def _dmc_step(self, loss) -> None:
        self.step_count += 1 

        if loss is not None:
            if not math.isfinite(loss):
                if self.debug and self.rank == 0:
                    print("[WARN] Non-finite loss detected. Forcing sync.")
                self.sync_parameters()
                self.tau = self.tau_min
                self.loss_0 = None
                return

        did_sync = False

        if self.step_count % self.tau != 0 and self.rank == 0:
            if self.debug:
                self.logger.log("[INFO] local step, no sync")
            
            '''
            with torch.no_grad():
                norm = torch.norm(self.all_params[0])
                if self.debug:
                    print(f"[DEBUG] step={self.step_count}, param_norm={norm:.4f}")
            '''

        # Hard cap case!
        if self.step_count % self.max_local_steps == 0:
            if self.rank == 0:
                if self.debug:
                    self.logger.log(f"[CAP SYNC] step={self.step_count}")
            self.sync_parameters()
            did_sync = True

        if (self.step_count % self.tau == 0) and not did_sync:
            self.sync_parameters()
            if loss is not None:
                avg_loss = self.reduce_scalar(loss)
                self.adapt_tau(avg_loss)
                self.logger.log(f"Trying to adapt tau: new tau = {self.tau}", rank0_only=True)
        
        if self.tests and did_sync:
            Tests(self.all_params, self.logger).param_consistency()  
               

    # Synchronize model parameters across all processes.
    def sync_parameters(self) -> None:
        with torch.no_grad():
            # Iterate over all parameters and perform all-reduce and averaging 
            for p in self.all_params:
                torch.distributed.all_reduce(p.data, op=torch.distributed.ReduceOp.SUM)  
                p.data /= self.world_size
        torch.distributed.barrier()

    # Adapt tau according to the loss value. 
    def adapt_tau(self, loss) -> None:
        # Initialize loss_0 (initial loss; starting loss) at first call. 
        # Can't adapt tau without a reference loss.
        if self.loss_0 is None:
            self.loss_0 = loss
            return

        tau_candidate = math.ceil(
            math.sqrt(loss / self.loss_0) * self.tau_0
        )

        # Strictly decreases tau when loss increases.
        # Safe to decrease rather than increase to avoid oscillatory behaviour.
        if tau_candidate >= self.tau:
            self.tau = max(self.tau_min, self.tau // 2)
        else:
            self.tau = tau_candidate

        self.tau = max(self.tau_min, min(self.tau_max, self.tau))

    def zero_grad(self, set_to_none: bool = False):
        self.inner_optimizer.zero_grad(set_to_none=set_to_none)
