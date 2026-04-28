import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.distributed as dist
import os
import csv
import time

torch.manual_seed(42)

SETUP_NAME = "1node"
FILENAME = f"metrics_{SETUP_NAME}.csv"

USE_CUDA = torch.cuda.is_available()

def sync():
    """Wall-clock reads are only meaningful after all CUDA kernels finish."""
    if USE_CUDA:
        torch.cuda.synchronize()

# ---------------------------
# DDP Setup
# ---------------------------
def setup():
    if "RANK" in os.environ:
        dist.init_process_group(
            backend="nccl" if USE_CUDA else "gloo"
        )
        local_rank = int(os.environ.get("LOCAL_RANK", 0))
        return local_rank, True
    else:
        return 0, False

def cleanup():
    dist.destroy_process_group()

# ---------------------------
# Data Loader
# ---------------------------
def get_dataloader(rank, world_size, use_ddp, batch_size=128):
    transform = transforms.Compose([
        transforms.Resize(200),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.5,0.5,0.5), (0.5,0.5,0.5))
    ])

    dataset = torchvision.datasets.CIFAR10(
        root="./data", train=True, download=True, transform=transform
    )

    if use_ddp:
        sampler = torch.utils.data.distributed.DistributedSampler(
            dataset, num_replicas=world_size, rank=rank
        )
    else:
        sampler = None

    loader = torch.utils.data.DataLoader(
        dataset, batch_size=batch_size, sampler=sampler, shuffle=(sampler is None), num_workers=4
    )

    return loader

# ---------------------------
# Model
# ---------------------------
def get_model(device):
    model = torchvision.models.resnet18(num_classes=10)
    model = model.to(device)
    return model

# ---------------------------
# Train Loop
# ---------------------------
def train():
    local_rank, use_ddp = setup()

    device = torch.device(f"cuda:{local_rank}" if USE_CUDA else "cpu")

    if use_ddp:
        world_size = dist.get_world_size()
        rank = dist.get_rank()
    else:
        world_size = 1
        rank = 0

    train_loader = get_dataloader(rank, world_size, use_ddp)

    model = get_model(device)

    if use_ddp:
        model = DDP(model, device_ids=[local_rank] if USE_CUDA else None)

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.9)

    epochs = 10
    global_start = time.time()

    if rank == 0 and not os.path.exists(FILENAME):
        with open(FILENAME, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "setup", "epoch", "loss",
                "epoch_time", "forward_time", "backward_time"
            ])

    for epoch in range(epochs):
        num_batches = 0
        if use_ddp:
            train_loader.sampler.set_epoch(epoch)

        total_loss = 0
        forward_time = 0
        backward_time = 0

        epoch_start = time.time()

        for images, labels in train_loader:
            images, labels = images.to(device), labels.to(device)

            optimizer.zero_grad()

            # Forward
            sync()
            start = time.time()
            outputs = model(images)
            loss = criterion(outputs, labels)
            sync()
            forward_time += time.time() - start

            # Backward (includes all-reduce communication in DDP)
            sync()
            start = time.time()
            loss.backward()
            sync()
            backward_time += time.time() - start

            optimizer.step()

            total_loss += loss.item()
            num_batches += 1

        avg_loss = total_loss / num_batches
        epoch_time = time.time() - epoch_start

        if rank == 0:
            print(f"\nEpoch {epoch+1}")
            print(f"  Avg. Loss: {avg_loss:.4f}")
            print(f"  Time: {epoch_time:.2f}s")
            print(f"  Forward Time: {forward_time:.2f}s")
            print(f"  Backward Time: {backward_time:.2f}s")
            with open(FILENAME, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    SETUP_NAME,
                    epoch,
                    avg_loss,
                    epoch_time,
                    forward_time,
                    backward_time
                ])

    total_time = time.time() - global_start

    if rank == 0:
        print(f"\nTotal Training Time: {total_time:.2f}s")
        with open(FILENAME, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                SETUP_NAME,
                "total",
                "-",
                total_time,
                "-",
                "-"
            ])

    if use_ddp:
        cleanup()

if __name__ == "__main__":
    train()