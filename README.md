# Heatwave & Flood Prediction Pipeline


This repo provides a complete pipeline (separate Python scripts) to: ingest data, label heatwave and flood proxy targets, engineer features, train XGBoost baseline, optionally train an LSTM sequence model, evaluate and produce reports.

How to run (example):
1. Create virtualenv and install requirements:
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2. Prepare input CSV `data/weather.csv` with columns you provided.

3. Run pipeline steps:
python data_ingest.py --input data/weather.csv --out_dir data/processed
python labeling.py --in_dir data/processed --out_dir data/processed
python features.py --in_dir data/processed --out_dir data/processed
python train_xgb.py --features data/processed/features.parquet --out_dir models/
python evaluate.py --features data/processed/features.parquet --model models/xgb_model.joblib --out_dir results/

Optional: train LSTM (needs more data):
python train_lstm.py --features data/processed/features.parquet --out_dir models/