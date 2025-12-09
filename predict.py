"""
Predict heatwave/flood using trained models (XGBoost or LSTM).
Usage:
  # Using XGBoost model
  python predict.py --model models/xgb_heatwave_model.joblib --features data/processed/features.parquet --target heatwave

  # Using LSTM model
  python predict.py --model models/lstm_heatwave.pt --features data/processed/features.parquet --target heatwave --timesteps 14
"""
import argparse
import os
import numpy as np
import pandas as pd
import joblib
import torch
import torch.nn as nn
import xgboost as xgb
from utils import read_parquet


class LSTMModel(nn.Module):
    """LSTM model for binary classification."""
    def __init__(self, input_size, hidden_size=64, fc_size=32):
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True)
        self.fc1 = nn.Linear(hidden_size, fc_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(fc_size, 1)
    
    def forward(self, x):
        lstm_out, (h_n, c_n) = self.lstm(x)
        out = self.fc1(h_n[-1])
        out = self.relu(out)
        out = self.fc2(out)
        return out.squeeze(-1)


def load_xgb_model(model_path):
    """Load XGBoost model."""
    return joblib.load(model_path)


def load_lstm_model(model_path, device):
    """Load LSTM model from checkpoint."""
    checkpoint = torch.load(model_path, map_location=device)
    model = LSTMModel(
        input_size=checkpoint['input_size'],
        hidden_size=checkpoint['hidden_size'],
        fc_size=checkpoint['fc_size']
    )
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()
    return model, checkpoint


def predict_xgb(model, df, target='heatwave'):
    """Make predictions using XGBoost model."""
    drop_cols = ['Date', 'District', 'heatwave', 'flood_proxy', 'Latitude', 'Longitude']
    X = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')
    X = X.fillna(-999)
    
    if isinstance(model, xgb.Booster):
        dmatrix = xgb.DMatrix(X)
        preds = model.predict(dmatrix)
    elif hasattr(model, 'predict_proba'):
        preds = model.predict_proba(X)[:, 1]
    else:
        preds = model.predict(X)
    
    return preds


def predict_lstm(model, df, checkpoint, device, timesteps=14):
    """Make predictions using LSTM model."""
    df = df.sort_values(['District', 'Date']).copy()
    feature_cols = checkpoint.get('feature_cols', 
        [c for c in df.columns if c not in ['Date', 'District', 'heatwave', 'flood_proxy']])
    
    predictions = []
    indices = []
    
    for district, g in df.groupby('District'):
        arr = g[feature_cols].values.astype(np.float32)
        arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
        
        if len(arr) < timesteps:
            continue
        
        for i in range(timesteps, len(arr) + 1):
            seq = arr[i-timesteps:i]
            seq_tensor = torch.FloatTensor(seq).unsqueeze(0).to(device)
            
            with torch.no_grad():
                logit = model(seq_tensor)
                prob = torch.sigmoid(logit).item()
            
            predictions.append(prob)
            indices.append(g.index[i-1])
    
    return np.array(predictions), indices


def main(args):
    # Load data
    df = read_parquet(args.features)
    print(f"Loaded {len(df)} rows from {args.features}")
    
    # Detect model type and load
    model_path = args.model
    is_lstm = model_path.endswith('.pt')
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    if is_lstm:
        print(f"Loading LSTM model from {model_path}")
        model, checkpoint = load_lstm_model(model_path, device)
        timesteps = args.timesteps or checkpoint.get('timesteps', 14)
        preds, indices = predict_lstm(model, df, checkpoint, device, timesteps)
        
        # Create results DataFrame
        results = pd.DataFrame({
            'prediction_prob': preds
        }, index=indices)
        results = df.loc[indices, ['Date', 'District']].copy()
        results['prediction_prob'] = preds
    else:
        print(f"Loading XGBoost model from {model_path}")
        model = load_xgb_model(model_path)
        preds = predict_xgb(model, df, args.target)
        
        # Create results DataFrame
        results = df[['Date', 'District']].copy()
        results['prediction_prob'] = preds
    
    # Add binary prediction with threshold
    threshold = args.threshold
    results['prediction'] = (results['prediction_prob'] >= threshold).astype(int)
    
    # Add actual labels if available
    if args.target in df.columns:
        if is_lstm:
            results['actual'] = df.loc[results.index, args.target].astype(int).values
        else:
            results['actual'] = df[args.target].astype(int).values
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Prediction Summary for {args.target}")
    print(f"{'='*50}")
    print(f"Total predictions: {len(results)}")
    print(f"Positive predictions (>= {threshold}): {results['prediction'].sum()}")
    print(f"Negative predictions: {(results['prediction'] == 0).sum()}")
    
    if 'actual' in results.columns:
        accuracy = (results['prediction'] == results['actual']).mean()
        print(f"Accuracy: {accuracy:.4f}")
    
    # Save results
    if args.output:
        results.to_csv(args.output, index=False)
        print(f"\nResults saved to {args.output}")
    else:
        print(f"\nSample predictions (first 10 rows):")
        print(results.head(10).to_string())
    
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make predictions using trained models')
    parser.add_argument('--model', required=True, help='Path to trained model (.joblib for XGBoost, .pt for LSTM)')
    parser.add_argument('--features', required=True, help='Path to features parquet file')
    parser.add_argument('--target', default='heatwave', choices=['heatwave', 'flood_proxy'], help='Target variable')
    parser.add_argument('--timesteps', type=int, default=None, help='Timesteps for LSTM (default: from model checkpoint)')
    parser.add_argument('--threshold', type=float, default=0.5, help='Classification threshold (default: 0.5)')
    parser.add_argument('--output', type=str, default=None, help='Output CSV file path (optional)')
    args = parser.parse_args()
    main(args)