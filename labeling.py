"""
python labeling.py --in_dir data/processed --out_dir data/processed
"""
import argparse
import os
import pandas as pd
import numpy as np
from utils import read_parquet, save_parquet, ensure_dir

def label_heatwave(df, pct=0.95, min_run=3, temp_col='MaxTemp_2m'):
    df = df.sort_values(['District','Date']).copy()
    # compute district-specific percentile
    p = df.groupby('District')[temp_col].transform(lambda s: s.quantile(pct))
    df['heat_exceed'] = df[temp_col] > p
    # compute run length of consecutive True values
    def run_len(s):
        runs = s.astype(int).groupby((s != s.shift()).cumsum()).cumsum() * s
        return runs
    df['heat_run'] = df.groupby('District')['heat_exceed'].transform(lambda s: run_len(s))
    df['heatwave'] = df['heat_run'] >= min_run
    return df

def label_flood(df, p1_pct=0.99, p3_pct=0.98):
    df = df.sort_values(['District','Date']).copy()
    g = df.groupby('District')
    df['precip_1d'] = df['Precip'].fillna(0)
    df['precip_3d'] = g['precip_1d'].rolling(3, min_periods=1).sum().reset_index(0,drop=True)
    df['precip_7d'] = g['precip_1d'].rolling(7, min_periods=1).sum().reset_index(0,drop=True)
    # thresholds
    p99 = g['precip_1d'].transform(lambda s: s.quantile(p1_pct))
    p98_3d = g['precip_3d'].transform(lambda s: s.quantile(p3_pct))
    df['precip_p99'] = p99
    df['precip3_p98'] = p98_3d
    # wetness flag
    df['RH_2m_filled'] = df['RH_2m'].fillna(method='ffill').fillna(method='bfill')
    df['wetness_flag'] = df['RH_2m_filled'] > g['RH_2m_filled'].transform(lambda s: s.quantile(0.9))
    df['flood_proxy'] = ((df['precip_1d'] > df['precip_p99']) | (df['precip_3d'] > df['precip3_p98'])) & df['wetness_flag']
    return df

def main(args):
    in_path = os.path.join(args.in_dir, 'weather.parquet')
    df = read_parquet(in_path)
    df = label_heatwave(df)
    df = label_flood(df)
    ensure_dir(args.out_dir)
    out_path = os.path.join(args.out_dir, 'weather_labeled.parquet')
    save_parquet(df, out_path)
    print('Saved labeled data to', out_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_dir', required=True)
    parser.add_argument('--out_dir', required=True)
    args = parser.parse_args()
    main(args)