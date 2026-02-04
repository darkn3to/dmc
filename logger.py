import os
import sys

class Logger:
    def __init__(self, rank, intercept_print=False):
        self.rank = rank
        self.intercept_print = intercept_print
        self._init_logger()
        if self.intercept_print:
            self._original_stdout = sys.stdout
            sys.stdout = self

    def _init_logger(self):
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"dmc_rank_{self.rank}.log")
        self._log_file = open(log_file, "w", buffering=1)
        self.log(f"[INIT] Logger Initialized | Rank: {self.rank}")

    def log(self, message, rank0_only=True):
        if rank0_only and self.rank != 0:
            return
        print(message, file=self._log_file, flush=True)

    def write(self, message):
        if message.strip():  # Avoid logging empty lines
            self.log(message.strip())

    def flush(self):
        self._log_file.flush()

    def __del__(self):
        if hasattr(self, '_log_file'):
            self._log_file.close()
        if self.intercept_print:
            sys.stdout = self._original_stdout