import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from torchvision import datasets, transforms, models
from torch.cuda.amp import autocast, GradScaler
import time
from dmcop import DMC

# -------------------------------
BATCH_SIZE = 64
EPOCHS = 10
LR = 1e-4
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# -------------------------------
# DATASET & TRANSFORMS
# -------------------------------
# CIFAR-10 images are 32x32 — we resize to 224x224 for MobileNetV3
train_transforms = transforms.Compose([
    transforms.Resize(224),
    transforms.RandomHorizontalFlip(),
    transforms.ToTensor(),
    transforms.Normalize((0.485, 0.456, 0.406),
                         (0.229, 0.224, 0.225))
])

val_transforms = transforms.Compose([
    transforms.Resize(224),
    transforms.ToTensor(),
    transforms.Normalize((0.485, 0.456, 0.406),
                         (0.229, 0.224, 0.225))
])

train_dataset = datasets.CIFAR10(root='./data', train=True,
                                 download=True, transform=train_transforms)
val_dataset = datasets.CIFAR10(root='./data', train=False,
                               download=True, transform=val_transforms)

train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE,
                          shuffle=True, num_workers=2, pin_memory=True)
val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE,
                        shuffle=False, num_workers=2, pin_memory=True)

# -------------------------------
# MODEL SETUP
# -------------------------------
model = models.mobilenet_v3_small(weights='IMAGENET1K_V1')

# Replace classifier for 10 CIFAR classes
model.classifier[3] = nn.Linear(model.classifier[3].in_features, 10)
model = model.to(DEVICE)

criterion = nn.CrossEntropyLoss()
optimizer = DMC(model=model, optimizer=optim.Adam(model.parameters(), lr=LR), backend="nccl", tau=3, debug=True, tests=True, intercept_print=True)
scaler = GradScaler()

# -------------------------------
# TRAINING LOOP
# -------------------------------
'''optimizer.logger.log("Using device: " + str(DEVICE), rank0_only=True)
optimizer.logger.log(f"Training MobileNetV3-Small on CIFAR-10 for {EPOCHS} epochs...", rank0_only=True)'''
print("Using device:", DEVICE)
print(f"Training MobileNetV3-Small on CIFAR-10 for {EPOCHS} epochs...")

for epoch in range(EPOCHS):
    model.train()
    running_loss = 0.0
    correct, total = 0, 0
    start_time = time.time()

    for images, labels in train_loader:
        images, labels = images.to(DEVICE), labels.to(DEVICE)
        optimizer.zero_grad()

        with autocast():
            outputs = model(images)
            loss = criterion(outputs, labels)

        scaler.scale(loss).backward()
        scaler.step(optimizer)
        scaler.update()

        running_loss += loss.item() * images.size(0)
        _, preds = torch.max(outputs, 1)
        correct += torch.sum(preds == labels).item()
        total += labels.size(0)

    train_acc = 100 * correct / total
    train_loss = running_loss / total

    # -------------------------------
    # VALIDATION LOOP
    # -------------------------------
    model.eval()
    val_loss, val_correct, val_total = 0.0, 0, 0
    with torch.no_grad():
        for images, labels in val_loader:
            images, labels = images.to(DEVICE), labels.to(DEVICE)
            with autocast():
                outputs = model(images)
                loss = criterion(outputs, labels)

            val_loss += loss.item() * images.size(0)
            _, preds = torch.max(outputs, 1)
            val_correct += torch.sum(preds == labels).item()
            val_total += labels.size(0)

    val_acc = 100 * val_correct / val_total
    val_loss /= val_total

    elapsed = time.time() - start_time

    print(f"Epoch [{epoch+1}/{EPOCHS}] "
          f"Train Loss: {train_loss:.4f} | Train Acc: {train_acc:.2f}% | "
          f"Val Loss: {val_loss:.4f} | Val Acc: {val_acc:.2f}% | "
          f"Time: {elapsed:.1f}s")
    

# -------------------------------
# SAVE MODEL
# -------------------------------
torch.save(model.state_dict(), "mobilenetv3_cifar10.pth")
optimizer.logger.log("✅ Training complete. Model saved as mobilenetv3_cifar10.pth", rank0_only=True)