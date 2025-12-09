"""
Load raw CSV, basic cleaning, save a canonical parquet file.
Usage:
python data_ingest.py --input data/weather.csv --out_dir data/processed
"""
import argparse
import os
from utils import ensure_dir, load_csv, save_parquet, date_sanity_checks




def main(args):
    df = load_csv(args.input)
    df = date_sanity_checks(df)
    # Normalize column names (strip spaces)
    df.columns = [c.strip() for c in df.columns]
    # Keep only expected columns (if extra columns exist we keep them but it's fine)
    ensure_dir(args.out_dir)
    out_path = os.path.join(args.out_dir, 'weather.parquet')
    save_parquet(df, out_path)
    print('Saved canonical parquet to', out_path)




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--out_dir', required=True)
    args = parser.parse_args()
    main(args)