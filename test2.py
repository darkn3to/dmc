import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms, models
from torch.amp.autocast_mode import autocast
from torch.amp.grad_scaler import GradScaler
import time
from smart_resume import SmartManager

# CONFIG
BATCH_SIZE = 64
EPOCHS = 10
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 1. SETUP DATA
train_transforms = transforms.Compose([
    transforms.Resize(224), transforms.RandomHorizontalFlip(),
    transforms.ToTensor(), transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225))
])
val_transforms = transforms.Compose([
    transforms.Resize(224), transforms.ToTensor(),
    transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225))
])

train_dataset = datasets.CIFAR10(root='./data', train=True, download=True, transform=train_transforms)
val_dataset = datasets.CIFAR10(root='./data', train=False, download=True, transform=val_transforms)

# 2. SETUP MODEL
model = models.mobilenet_v3_small(weights='IMAGENET1K_V1')
model.classifier[3] = nn.Linear(model.classifier[3].in_features, 10)
model = model.to(DEVICE)
optimizer = optim.Adam(model.parameters(), lr=1e-4)
criterion = nn.CrossEntropyLoss()
scaler = GradScaler()

# ---------------------------------------------------------
# 3. INITIALIZE SMART MANAGER
# ---------------------------------------------------------
manager = SmartManager(checkpoint_path="checkpoint.pth", device=DEVICE)
manager.load_checkpoint(model, optimizer)

print(f"Training on {DEVICE}...")

# ---------------------------------------------------------
# 4. TRAINING LOOP
# ---------------------------------------------------------
for epoch in range(manager.start_epoch, EPOCHS):
    model.train()
    session_start = time.time()
    
    # ASK MANAGER FOR THE LOADER (Handles skipping automatically)
    train_loader = manager.get_loader(train_dataset, BATCH_SIZE, epoch)
    print(f"Epoch {epoch+1} starting...")
    
    for i, (images, labels) in enumerate(train_loader):
        images, labels = images.to(DEVICE), labels.to(DEVICE)
        optimizer.zero_grad()

        with autocast(device_type=DEVICE.type):
            outputs = model(images)
            loss = criterion(outputs, labels)

        scaler.scale(loss).backward()
        scaler.step(optimizer)
        scaler.update()

        # Update stats & check for stop signal
        _, preds = torch.max(outputs, 1)
        manager.update_stats(loss.item(), preds, labels)
        
        # CHECKPOINT TRIGGER
        manager.check_and_save(epoch, i, model, optimizer, time.time() - session_start, BATCH_SIZE)

    # End of Epoch Reporting
    train_loss, train_acc = manager.get_epoch_stats()
    total_time = manager.get_total_time(time.time() - session_start)
    
    print(f"Epoch [{epoch+1}/{EPOCHS}] Loss: {train_loss:.4f} | Acc: {train_acc:.2f}% | Time: {total_time:.1f}s")

# Final Save
torch.save(model.state_dict(), "final_model.pth")
print("Done.")