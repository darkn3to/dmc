import torch
import torch.distributed as dist
from logger import Logger
import logger

class Tests:
    """
    Tests for DMC functionalities.
    """
    def __init__(self, all_params, logger: Logger):
        self.all_params = all_params
        self.logger = logger

    def flatten_parameters(self):
        with torch.no_grad():
            return torch.cat([
                p.detach().view(-1)
                for p in self.all_params
            ])

    # Test to ensure that all model parameters are consistent across all processes.
    # Raises an assertion error if relative difference is above tolerance. 
    # Indicates a synchronization failure if too high.
    def param_consistency(self, atol=1e-6, rtol=1e-6):
        rank = dist.get_rank()

        flat = self.flatten_parameters()
        ref = flat.clone() if rank == 0 else torch.empty_like(flat)

        dist.broadcast(ref, src=0)

        diff = torch.norm(flat - ref)
        rel = diff / (torch.norm(ref) + 1e-12)

        # Always log result (rank 0 only)
        self.logger.log(
            f"[TEST] Param consistency | abs diff={diff:.3e}, "
            f"rel diff={rel:.3e} ({rel*100:.4f}%)",
            rank0_only=True,
        )

        # Failure condition
        if diff >= atol and rel >= rtol:
            self.logger.log(
                f"[TEST][FAIL] Param mismatch | abs={diff:.3e}, rel={rel:.3e}",
                rank0_only=True,
            )
            raise AssertionError(
                f"Parameter mismatch on rank {rank}: abs={diff}, rel={rel}"
            )