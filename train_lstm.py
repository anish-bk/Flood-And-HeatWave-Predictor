"""
Train a simple LSTM sequence model. Requires more data. It builds sliding windows per district.
Usage:
python train_lstm.py --features data/processed/features.parquet --out_dir models/ --timesteps 14
"""
import argparse
import os
import numpy as np
import pandas as pd
from utils import read_parquet, ensure_dir
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

class LSTMModel(nn.Module):
    """LSTM model for binary classification."""
    def __init__(self, input_size, hidden_size=64, fc_size=32):
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True)
        self.fc1 = nn.Linear(hidden_size, fc_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(fc_size, 1)
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        # x shape: (batch, timesteps, features)
        lstm_out, (h_n, c_n) = self.lstm(x)
        # Use last hidden state
        out = self.fc1(h_n[-1])
        out = self.relu(out)
        out = self.fc2(out)
        out = self.sigmoid(out)
        return out.squeeze(-1)

def build_sequences(df, timesteps=14, target='heatwave'):
    df = df.sort_values(['District','Date']).copy()
    feature_cols = [c for c in df.columns if c not in ['Date','District','heatwave','flood_proxy']]
    sequences = []
    targets = []
    for district, g in df.groupby('District'):
        arr = g[feature_cols].values
        lab = g[target].fillna(False).astype(int).values
        if len(arr) < timesteps+1:
            continue
        for i in range(timesteps, len(arr)):
            seq = arr[i-timesteps:i]
            sequences.append(seq)
            targets.append(lab[i])
    X = np.array(sequences)
    y = np.array(targets)
    return X, y, feature_cols


def train_epoch(model, dataloader, criterion, optimizer, device):
    """Train for one epoch."""
    model.train()
    total_loss = 0.0
    for X_batch, y_batch in dataloader:
        X_batch, y_batch = X_batch.to(device), y_batch.to(device)
        optimizer.zero_grad()
        outputs = model(X_batch)
        loss = criterion(outputs, y_batch)
        loss.backward()
        optimizer.step()
        total_loss += loss.item() * X_batch.size(0)
    return total_loss / len(dataloader.dataset)


def evaluate(model, dataloader, criterion, device):
    """Evaluate model on validation/test data."""
    model.eval()
    total_loss = 0.0
    all_preds = []
    all_targets = []
    with torch.no_grad():
        for X_batch, y_batch in dataloader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            outputs = model(X_batch)
            loss = criterion(outputs, y_batch)
            total_loss += loss.item() * X_batch.size(0)
            all_preds.extend(outputs.cpu().numpy())
            all_targets.extend(y_batch.cpu().numpy())
    avg_loss = total_loss / len(dataloader.dataset)
    auc = roc_auc_score(all_targets, all_preds) if len(set(all_targets)) > 1 else 0.0
    return avg_loss, auc

def main(args):
    df = read_parquet(args.features)
    timesteps = args.timesteps
    X, y, feat_cols = build_sequences(df, timesteps=timesteps, target=args.target)
    # simple split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=True)
    nsamples, nt, nf = X_train.shape
    # scale features per-feature
    scaler = StandardScaler()
    X_train_flat = X_train.reshape(-1, nf)
    X_test_flat = X_test.reshape(-1, nf)
    scaler.fit(X_train_flat)
    X_train_scaled = scaler.transform(X_train_flat).reshape(X_train.shape)
    X_test_scaled = scaler.transform(X_test_flat).reshape(X_test.shape)

    # Convert to PyTorch tensors
    X_train_tensor = torch.FloatTensor(X_train_scaled)
    y_train_tensor = torch.FloatTensor(y_train)
    X_test_tensor = torch.FloatTensor(X_test_scaled)
    y_test_tensor = torch.FloatTensor(y_test)

    # Create DataLoaders
    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    test_dataset = TensorDataset(X_test_tensor, y_test_tensor)
    train_loader = DataLoader(train_dataset, batch_size=128, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=128, shuffle=False)

    # Setup device, model, loss, optimizer
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = LSTMModel(input_size=nf, hidden_size=64, fc_size=32).to(device)
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters())

    # Training loop
    epochs = 20
    for epoch in range(epochs):
        train_loss = train_epoch(model, train_loader, criterion, optimizer, device)
        val_loss, val_auc = evaluate(model, test_loader, criterion, device)
        print(f'Epoch {epoch+1}/{epochs} - Train Loss: {train_loss:.4f} - Val Loss: {val_loss:.4f} - Val AUC: {val_auc:.4f}')

    ensure_dir(args.out_dir)
    model_path = os.path.join(args.out_dir, f'lstm_{args.target}.pt')
    torch.save({
        'model_state_dict': model.state_dict(),
        'input_size': nf,
        'hidden_size': 64,
        'fc_size': 32,
        'timesteps': timesteps,
        'feature_cols': feat_cols
    }, model_path)
    print(f'Saved LSTM model to {model_path}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--features', required=True)
    parser.add_argument('--out_dir', required=True)
    parser.add_argument('--timesteps', type=int, default=14)
    parser.add_argument('--target', default='heatwave', choices=['heatwave','flood_proxy'])
    args = parser.parse_args()
    main(args)