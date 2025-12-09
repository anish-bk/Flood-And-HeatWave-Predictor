"""
Train an XGBoost classifier baseline for heatwave (or flood) prediction.
Usage:
python train_xgb.py --features data/processed/features.parquet --out_dir models/
"""
import argparse
import os
import pandas as pd
import numpy as np
from utils import read_parquet, ensure_dir
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score, roc_auc_score, precision_recall_fscore_support
import joblib

def prepare_data(df, target='heatwave'):
    df = df.sort_values('Date').copy()
    # Drop rows with NaN target
    df = df[~df[target].isna()].copy()
    # define features (drop identifiers)
    drop_cols = ['Date','District','heatwave','flood_proxy','Latitude','Longitude']
    X = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')
    y = df[target].astype(int)
    # simple imputation
    X = X.fillna(-999)
    return X, y

def time_train_test_split(df, test_size_days=365):
    # split by date: last N days as test
    max_date = df['Date'].max()
    cutoff = max_date - pd.Timedelta(days=test_size_days)
    train = df[df['Date'] < cutoff]
    test = df[df['Date'] >= cutoff]
    return train, test

def main(args):
    df = read_parquet(args.features)
    # choose target
    target = args.target
    # split
    train_df, test_df = time_train_test_split(df, test_size_days=args.test_days)
    X_train, y_train = prepare_data(train_df, target=target)
    X_test, y_test = prepare_data(test_df, target=target)

    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)

    params = {
    'objective': 'binary:logistic',
    'eval_metric': 'aucpr',
    'eta': 0.05,
    'max_depth': 6,
    'subsample': 0.8,
    'scale_pos_weight': max(1, (len(y_train)-y_train.sum())/max(1,y_train.sum()))
    }
    evals = [(dtrain,'train'),(dtest,'test')]
    bst = xgb.train(params, dtrain, num_boost_round=1000, evals=evals, early_stopping_rounds=50, verbose_eval=20)

    # predict
    preds = bst.predict(dtest)
    pr_auc = average_precision_score(y_test, preds)
    roc = roc_auc_score(y_test, preds)
    print(f'PR AUC: {pr_auc:.4f}, ROC AUC: {roc:.4f}')
    # compute classification metrics at default 0.5
    yhat = (preds >= 0.5).astype(int)
    prec, rec, f1, _ = precision_recall_fscore_support(y_test, yhat, average='binary', zero_division=0)
    print(f'Precision {prec:.3f}, Recall {rec:.3f}, F1 {f1:.3f}')

    ensure_dir(args.out_dir)
    model_path = os.path.join(args.out_dir, f'xgb_{target}_model.joblib')
    joblib.dump(bst, model_path)
    print('Saved model to', model_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--features', required=True)
    parser.add_argument('--out_dir', required=True)
    parser.add_argument('--target', default='heatwave', choices=['heatwave','flood_proxy'])
    parser.add_argument('--test_days', type=int, default=365)
    args = parser.parse_args()
    main(args)