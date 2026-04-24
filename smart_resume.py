import torch
import signal
import sys
import os
import time
from torch.utils.data import DataLoader, Sampler

# 1. The Smart Sampler
class ResumeSampler(Sampler):
    """
    Generates a random shuffle but skips the first 'start_idx' samples.
    """
    def __init__(self, data_source, start_sample_idx=0, seed=42):
        self.data_source = data_source
        self.start_sample_idx = start_sample_idx
        self.seed = seed
        self.generator = torch.Generator()
        self.generator.manual_seed(self.seed)

    def __iter__(self):
        n = len(self.data_source)
        indices = torch.randperm(n, generator=self.generator).tolist()
        return iter(indices[self.start_sample_idx:])

    def __len__(self):
        return len(self.data_source) - self.start_sample_idx

# 2. The Manager Class (Main Interface)
class SmartManager:
    def __init__(self, checkpoint_path="checkpoint.pth", device="cpu", seed=42):
        self.path = checkpoint_path
        self.device = device
        self.seed = seed
        self.stop_requested = False 
        
        self.last_save_time = time.time()
        
        # State Variables
        self.start_epoch = 0
        self.start_sample_idx = 0
        self.elapsed_time = 0.0
        
        # Stats
        self.stats = {
            "correct": 0,
            "total": 0,
            "running_loss": 0.0
        }

        # Register Signal Handler
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        print(f"\nsReceived  {signum}.")
        self.stop_requested = True

    def load_checkpoint(self, model, optimizer):
        """Loads state if checkpoint exists. Returns True if resumed."""
        if os.path.exists(self.path):
            print(f"Found {self.path}.")
            checkpoint = torch.load(self.path, map_location=self.device)
            
            model.load_state_dict(checkpoint['model_state_dict'])
            optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
            
            self.start_epoch = checkpoint['epoch']
            self.elapsed_time = checkpoint.get('elapsed_time', 0.0)
            
            # Restore stats
            self.stats = checkpoint.get('stats', self.stats)
            
            # Calculate samples to skip
            batch_idx = checkpoint.get('batch_idx', -1)
            batch_size = checkpoint.get('batch_size', 1)
            if batch_idx != -1:
                self.start_sample_idx = (batch_idx + 1) * batch_size
                
            self.last_save_time = time.time()
            print(f"Resuming Epoch {self.start_epoch}, skipped {self.start_sample_idx} samples.")
            return True
        else:
            return False

    def get_loader(self, dataset, batch_size, epoch, num_workers=2):
        """Returns a DataLoader correctly set up for this specific epoch/resume state."""
        
        # If a new epoch, reset skip logic
        if epoch != self.start_epoch:
            self.start_sample_idx = 0
            self.elapsed_time = 0.0
            self.stats = {"correct": 0, "total": 0, "running_loss": 0.0}
            self.last_save_time = time.time()

        sampler = ResumeSampler(dataset, start_sample_idx=self.start_sample_idx, seed=self.seed + epoch)
        
        return DataLoader(dataset, batch_size=batch_size, sampler=sampler, num_workers=num_workers, pin_memory=True)

    def check_and_save(self, epoch, batch_idx, model, optimizer, current_session_time, batch_size):
        """Checks if stop signal was received. If yes, saves and exits."""
        
        global_batch_idx = (self.start_sample_idx // batch_size) + batch_idx
        total_time = self.elapsed_time + current_session_time
        
        if self.stop_requested:
            print(f"\nDone: Epoch {epoch}, Batch {batch_idx}.")
            
            torch.save({
                'epoch': epoch,
                'batch_idx': global_batch_idx,
                'batch_size': batch_size,
                'elapsed_time': total_time,
                'stats': self.stats,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
            }, self.path)
            
            print(f"Checkpoint saved. Total time so far: {total_time:.1f}s")
            sys.exit(0)
        
        now = time.time()
        if now - self.last_save_time >= self.autosave_interval:
            torch.save({
                'epoch': epoch,
                'batch_idx': global_batch_idx,
                'batch_size': batch_size,
                'elapsed_time': total_time,
                'stats': self.stats,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
            }, self.path)
            # update persisted elapsed_time and autosave timer
            self.elapsed_time = total_time
            self.last_save_time = now
            print(f"Autosaved checkpoint at Epoch {epoch}, Batch {batch_idx}. Total time: {total_time:.1f}s")

    def update_stats(self, loss_val, preds, labels):
        """Accumulates accuracy/loss stats."""
        self.stats["running_loss"] += loss_val * labels.size(0)
        self.stats["correct"] += torch.sum(preds == labels).item()
        self.stats["total"] += labels.size(0)

    def get_epoch_stats(self):
        """Returns (loss, accuracy) for the current epoch."""
        t = self.stats["total"]
        acc = 100 * self.stats["correct"] / t if t > 0 else 0
        loss = self.stats["running_loss"] / t if t > 0 else 0
        return loss, acc

    def get_total_time(self, current_session_time):
        return self.elapsed_time + current_session_time