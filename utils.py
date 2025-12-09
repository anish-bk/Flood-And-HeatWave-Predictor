"""
Utility functions used by the pipeline.
"""
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def load_csv(path, parse_dates=['Date']):
    df = pd.read_csv(path, parse_dates=parse_dates)
    return df

def save_parquet(df, path):
    ensure_dir(os.path.dirname(path))
    df.to_parquet(path, index=False)

def read_parquet(path):
    return pd.read_parquet(path)

def date_sanity_checks(df):
    if 'Date' not in df.columns:
        raise ValueError('Date column missing')
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    return df