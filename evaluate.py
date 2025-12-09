"""
Evaluate model predictions and produce simple plots.
Usage:
python evaluate.py --features data/processed/features.parquet --model models/xgb_heatwave_model.joblib --out_dir results/
"""
import argparse
import os
import pandas as pd
import numpy as np
import joblib
from utils import read_parquet, ensure_dir
from sklearn.metrics import average_precision_score, roc_auc_score, precision_recall_curve, roc_curve
import matplotlib.pyplot as plt

def prepare_Xy(df, target='heatwave'):
    df = df.sort_values('Date').copy()
    df = df[~df[target].isna()].copy()
    y = df[target].astype(int)
    drop_cols = ['Date','District','heatwave','flood_proxy','Latitude','Longitude']
    X = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')
    X = X.fillna(-999)
    return X, y, df

def plot_pr_roc(y, preds, out_dir, prefix=''):
    ensure_dir(out_dir)
    pr_auc = average_precision_score(y, preds)
    roc_auc = roc_auc_score(y, preds)
    precision, recall, _ = precision_recall_curve(y, preds)
    fpr, tpr, _ = roc_curve(y, preds)
    plt.figure()
    plt.plot(recall, precision)
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title(f'PR curve (AP={pr_auc:.3f})')
    plt.savefig(os.path.join(out_dir, f'{prefix}pr_curve.png'))
    plt.close()
    plt.figure()
    plt.plot(fpr, tpr)
    plt.xlabel('FPR')
    plt.ylabel('TPR')
    plt.title(f'ROC curve (AUC={roc_auc:.3f})')
    plt.savefig(os.path.join(out_dir, f'{prefix}roc_curve.png'))
    plt.close()
    return pr_auc, roc_auc

def main(args):
    df = read_parquet(args.features)
    model = joblib.load(args.model)
    X, y, df_used = prepare_Xy(df, target=args.target)
    # if it's xgboost booster object
    if hasattr(model, 'predict'):
        preds = model.predict(X)
    else:
        # fallback
        preds = model.predict_proba(X)[:,1]
    pr, roc = plot_pr_roc(y, preds, args.out_dir, prefix=args.target+'_')
    print(f'PR AUC: {pr:.4f}, ROC AUC: {roc:.4f}')
    # Save a small results csv with dates and preds
    out_df = df_used[['Date','District']].copy()
    out_df['y_true'] = y.values
    out_df['y_pred'] = preds
    out_df.to_csv(os.path.join(args.out_dir, f'predictions_{args.target}.csv'), index=False)
    print('Saved predictions and plots to', args.out_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--features', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--out_dir', required=True)
    parser.add_argument('--target', default='heatwave', choices=['heatwave','flood_proxy'])
    args = parser.parse_args()
    main(args)