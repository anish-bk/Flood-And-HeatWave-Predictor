"""
Create rolling, lag, anomaly and seasonal features. Save features parquet.
Usage:
python features.py --in_dir data/processed --out_dir data/processed
"""
import argparse
import pandas as pd
import numpy as np
from utils import read_parquet, save_parquet, ensure_dir

def add_time_features(df):
    df = df.copy()
    df['doy'] = df['Date'].dt.dayofyear
    df['doy_sin'] = np.sin(2*np.pi*df['doy']/365.25)
    df['doy_cos'] = np.cos(2*np.pi*df['doy']/365.25)
    df['year'] = df['Date'].dt.year
    df['month'] = df['Date'].dt.month
    return df

def rolling_lags(df, group='District'):
    df = df.sort_values([group,'Date']).copy()
    g = df.groupby(group)
    # rolling sums/means
    df['precip_3d'] = g['Precip'].rolling(3, min_periods=1).sum().reset_index(0,drop=True)
    df['precip_7d'] = g['Precip'].rolling(7, min_periods=1).sum().reset_index(0,drop=True)
    df['maxT_3d_mean'] = g['MaxTemp_2m'].rolling(3, min_periods=1).mean().reset_index(0,drop=True)
    df['temp_range_3d_max'] = g['TempRange_2m'].rolling(3, min_periods=1).max().reset_index(0,drop=True)
    # lags for precip and temp
    for lag in [1,2,3,7,14]:
        df[f'precip_lag_{lag}'] = g['Precip'].shift(lag)
        df[f'maxT_lag_{lag}'] = g['MaxTemp_2m'].shift(lag)
    # API-like index
    alpha = 0.8
    def compute_api(s):
        api = []
        prev = 0.0
        for v in s.fillna(0):
            prev = alpha*prev + v
            api.append(prev)
        return pd.Series(api, index=s.index)
    df['API'] = g['Precip'].apply(lambda s: compute_api(s)).reset_index(0,drop=True)
    return df

def climatology_anomaly(df):
    df = df.copy()
    # compute day-of-year climatology per district for MaxTemp_2m and Precip
    df['doy'] = df['Date'].dt.dayofyear
    climatology = df.groupby(['District','doy'])[['MaxTemp_2m','Precip']].median().rename(columns={'MaxTemp_2m':'clim_maxT','Precip':'clim_precip'}).reset_index()
    df = df.merge(climatology, on=['District','doy'], how='left')
    df['anom_maxT'] = df['MaxTemp_2m'] - df['clim_maxT']
    df['anom_precip'] = df['Precip'] - df['clim_precip']
    return df

def build_features(df):
    df = add_time_features(df)
    df = rolling_lags(df)
    df = climatology_anomaly(df)
    # select a reasonable feature set
    feature_cols = [
    'Date','District','Latitude','Longitude',
    'Precip','precip_3d','precip_7d','precip_lag_1','precip_lag_3','precip_lag_7',
    'MaxTemp_2m','maxT_3d_mean','maxT_lag_1','maxT_lag_3', 'anom_maxT',
    'RH_2m','wetness_flag','API','TempRange_2m','WindSpeed_10m','WindSpeed_50m',
    'doy_sin','doy_cos','month','year','heatwave','flood_proxy'
    ]
    existing = [c for c in feature_cols if c in df.columns]
    feats = df[existing].copy()
    return feats

def main(args):
    in_path = args.in_dir.rstrip('/') + '/weather_labeled.parquet'
    df = read_parquet(in_path)
    feats = build_features(df)
    ensure_dir(args.out_dir)
    out_path = args.out_dir.rstrip('/') + '/features.parquet'
    save_parquet(feats, out_path)
    print('Saved features to', out_path)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_dir', required=True)
    parser.add_argument('--out_dir', required=True)
    args = parser.parse_args()
    main(args)