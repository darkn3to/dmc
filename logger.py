import os

class Logger:
    def __init__(self, rank):
        self.rank = rank
        self._init_logger()

    def _init_logger(self):
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"dmc_rank_{self.rank}.log")
        self._log_file = open(log_file, "w", buffering=1)
        self.log(f"[INIT] Logger Initialized | Rank: {self.rank}")

    def log(self, message, rank0_only=False):
        if rank0_only and self.rank != 0:
            return
        print(message, file=self._log_file, flush=True)