"""
Gradio App for Heatwave and Flood Prediction
Using Gradio 6.x with the latest features
Usage:
    python app.py
"""
import gradio as gr
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import joblib
import xgboost as xgb
import os
from datetime import datetime, timedelta


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


# Global variables to store loaded models
models = {
    'heatwave_xgb': None,
    'flood_xgb': None,
    'heatwave_lstm': None,
    'flood_lstm': None,
    'lstm_checkpoint_heatwave': None,
    'lstm_checkpoint_flood': None
}
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')


def load_models():
    """Load all available models at startup."""
    model_paths = {
        'heatwave_xgb': 'models/xgb_heatwave_model.joblib',
        'flood_xgb': 'models/xgb_flood_proxy_model.joblib',
        'heatwave_lstm': 'models/lstm_heatwave.pt',
        'flood_lstm': 'models/lstm_flood_proxy.pt'
    }
    
    for key, path in model_paths.items():
        if os.path.exists(path):
            try:
                if path.endswith('.joblib'):
                    models[key] = joblib.load(path)
                    print(f"✅ Loaded {key} from {path}")
                elif path.endswith('.pt'):
                    checkpoint = torch.load(path, map_location=device, weights_only=False)
                    model = LSTMModel(
                        input_size=checkpoint['input_size'],
                        hidden_size=checkpoint['hidden_size'],
                        fc_size=checkpoint['fc_size']
                    )
                    model.load_state_dict(checkpoint['model_state_dict'])
                    model.to(device)
                    model.eval()
                    models[key] = model
                    models[f'lstm_checkpoint_{key.replace("_lstm", "")}'] = checkpoint
                    print(f"✅ Loaded {key} from {path}")
            except Exception as e:
                print(f"❌ Failed to load {key}: {e}")


def create_features(precip, max_temp, rh, temp_range, wind_10m, wind_50m, 
                    month, day_of_year, precip_history=None, temp_history=None):
    """Create feature vector from input data."""
    doy_sin = np.sin(2 * np.pi * day_of_year / 365.25)
    doy_cos = np.cos(2 * np.pi * day_of_year / 365.25)
    
    if precip_history is not None and len(precip_history) >= 7:
        precip_3d = sum(precip_history[-3:])
        precip_7d = sum(precip_history[-7:])
        precip_lag_1 = precip_history[-1] if len(precip_history) >= 1 else 0
        precip_lag_3 = precip_history[-3] if len(precip_history) >= 3 else 0
        precip_lag_7 = precip_history[-7] if len(precip_history) >= 7 else 0
    else:
        precip_3d = precip * 3
        precip_7d = precip * 7
        precip_lag_1 = precip
        precip_lag_3 = precip
        precip_lag_7 = precip
    
    if temp_history is not None and len(temp_history) >= 3:
        maxT_3d_mean = np.mean(temp_history[-3:])
        maxT_lag_1 = temp_history[-1] if len(temp_history) >= 1 else max_temp
        maxT_lag_3 = temp_history[-3] if len(temp_history) >= 3 else max_temp
    else:
        maxT_3d_mean = max_temp
        maxT_lag_1 = max_temp
        maxT_lag_3 = max_temp
    
    anom_maxT = max_temp - 30
    wetness_flag = 1 if rh > 80 else 0
    api = precip * 1.5
    
    features = {
        'Precip': precip,
        'precip_3d': precip_3d,
        'precip_7d': precip_7d,
        'precip_lag_1': precip_lag_1,
        'precip_lag_3': precip_lag_3,
        'precip_lag_7': precip_lag_7,
        'MaxTemp_2m': max_temp,
        'maxT_3d_mean': maxT_3d_mean,
        'maxT_lag_1': maxT_lag_1,
        'maxT_lag_3': maxT_lag_3,
        'anom_maxT': anom_maxT,
        'RH_2m': rh,
        'wetness_flag': wetness_flag,
        'API': api,
        'TempRange_2m': temp_range,
        'WindSpeed_10m': wind_10m,
        'WindSpeed_50m': wind_50m,
        'doy_sin': doy_sin,
        'doy_cos': doy_cos,
        'month': month,
        'year': 2025
    }
    
    return features


def predict_xgb(model, features):
    """Make prediction using XGBoost model."""
    feature_cols = ['Precip', 'precip_3d', 'precip_7d', 'precip_lag_1', 'precip_lag_3', 
                    'precip_lag_7', 'MaxTemp_2m', 'maxT_3d_mean', 'maxT_lag_1', 'maxT_lag_3',
                    'anom_maxT', 'RH_2m', 'wetness_flag', 'API', 'TempRange_2m', 
                    'WindSpeed_10m', 'WindSpeed_50m', 'doy_sin', 'doy_cos', 'month', 'year']
    
    X = pd.DataFrame([features])[feature_cols]
    X = X.fillna(-999)
    
    if isinstance(model, xgb.Booster):
        dmatrix = xgb.DMatrix(X)
        pred = model.predict(dmatrix)[0]
    elif hasattr(model, 'predict_proba'):
        pred = model.predict_proba(X)[0, 1]
    else:
        pred = model.predict(X)[0]
    
    return float(pred)


def get_risk_level(prob):
    """Get risk level and color based on probability."""
    if prob >= 0.8:
        return "🔴 EXTREME RISK", "red", "Immediate action required!"
    elif prob >= 0.6:
        return "🟠 HIGH RISK", "orange", "High risk - Take precautions"
    elif prob >= 0.4:
        return "🟡 MODERATE RISK", "yellow", "Moderate risk - Stay alert"
    elif prob >= 0.2:
        return "🟢 LOW RISK", "green", "Low risk - Normal conditions"
    else:
        return "🔵 MINIMAL RISK", "blue", "Minimal risk - Safe conditions"


def create_gauge_html(prob, title, icon):
    """Create a visual gauge for probability display."""
    percentage = prob * 100
    risk_level, color_name, message = get_risk_level(prob)
    
    color_map = {
        "red": "#ef4444",
        "orange": "#f97316",
        "yellow": "#eab308",
        "green": "#22c55e",
        "blue": "#3b82f6"
    }
    color = color_map.get(color_name, "#6b7280")
    
    html = f"""
    <div style="background: linear-gradient(145deg, #1a1a2e 0%, #16213e 100%); 
                border-radius: 24px; padding: 30px; margin: 10px; 
                box-shadow: 0 20px 60px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.1);
                text-align: center; border: 1px solid rgba(255,255,255,0.1);">
        <div style="font-size: 60px; margin-bottom: 15px; filter: drop-shadow(0 4px 8px rgba(0,0,0,0.3));">{icon}</div>
        <h2 style="color: #fff; margin: 10px 0; font-size: 22px; font-weight: 600; letter-spacing: 0.5px;">{title}</h2>
        
        <div style="position: relative; width: 180px; height: 180px; margin: 25px auto;">
            <svg viewBox="0 0 200 200" style="transform: rotate(-90deg); filter: drop-shadow(0 4px 12px {color}40);">
                <circle cx="100" cy="100" r="85" fill="none" stroke="rgba(255,255,255,0.1)" stroke-width="15"/>
                <circle cx="100" cy="100" r="85" fill="none" stroke="{color}" stroke-width="15"
                        stroke-dasharray="{percentage * 5.34} 534"
                        stroke-linecap="round"
                        style="transition: stroke-dasharray 0.8s cubic-bezier(0.4, 0, 0.2, 1);"/>
            </svg>
            <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center;">
                <div style="font-size: 48px; font-weight: 700; color: {color}; 
                            text-shadow: 0 0 30px {color}60;">{percentage:.1f}%</div>
                <div style="font-size: 12px; color: rgba(255,255,255,0.6); text-transform: uppercase; 
                            letter-spacing: 2px; margin-top: 5px;">Probability</div>
            </div>
        </div>
        
        <div style="background: linear-gradient(135deg, {color}20 0%, {color}10 100%); 
                    border: 1px solid {color}40; border-radius: 16px; 
                    padding: 18px; margin-top: 20px; backdrop-filter: blur(10px);">
            <div style="font-size: 18px; font-weight: 700; color: {color}; letter-spacing: 1px;">{risk_level}</div>
            <div style="font-size: 13px; color: rgba(255,255,255,0.7); margin-top: 8px;">{message}</div>
        </div>
    </div>
    """
    return html


def predict(district, date, precip, max_temp, rh, temp_range, wind_10m, wind_50m):
    """Main prediction function."""
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        month = date_obj.month
        day_of_year = date_obj.timetuple().tm_yday
        
        features = create_features(
            precip=precip, max_temp=max_temp, rh=rh,
            temp_range=temp_range, wind_10m=wind_10m, wind_50m=wind_50m,
            month=month, day_of_year=day_of_year
        )
        
        heatwave_prob = 0.0
        if models['heatwave_xgb'] is not None:
            heatwave_prob = predict_xgb(models['heatwave_xgb'], features)
        
        flood_prob = 0.0
        if models['flood_xgb'] is not None:
            flood_prob = predict_xgb(models['flood_xgb'], features)
        
        heatwave_html = create_gauge_html(heatwave_prob, "Heatwave Risk", "🌡️")
        flood_html = create_gauge_html(flood_prob, "Flood Risk", "🌊")
        
        summary_html = f"""
        <div style="background: linear-gradient(145deg, #0f0f23 0%, #1a1a3e 100%); 
                    border-radius: 24px; padding: 30px; margin: 10px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.1);
                    border: 1px solid rgba(255,255,255,0.1);">
            <h2 style="color: #fff; text-align: center; margin-bottom: 25px; font-size: 24px; font-weight: 600;">
                📍 Prediction Summary for <span style="color: #a78bfa;">{district}</span>
            </h2>
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px;">
                <div style="background: linear-gradient(135deg, rgba(99,102,241,0.2) 0%, rgba(99,102,241,0.1) 100%); 
                            padding: 20px; border-radius: 16px; border: 1px solid rgba(99,102,241,0.3);">
                    <div style="color: rgba(255,255,255,0.6); font-size: 12px; text-transform: uppercase; letter-spacing: 1px;">📅 Date</div>
                    <div style="color: #fff; font-size: 18px; font-weight: 600; margin-top: 8px;">{date}</div>
                </div>
                <div style="background: linear-gradient(135deg, rgba(239,68,68,0.2) 0%, rgba(239,68,68,0.1) 100%); 
                            padding: 20px; border-radius: 16px; border: 1px solid rgba(239,68,68,0.3);">
                    <div style="color: rgba(255,255,255,0.6); font-size: 12px; text-transform: uppercase; letter-spacing: 1px;">🌡️ Max Temp</div>
                    <div style="color: #fff; font-size: 18px; font-weight: 600; margin-top: 8px;">{max_temp}°C</div>
                </div>
                <div style="background: linear-gradient(135deg, rgba(59,130,246,0.2) 0%, rgba(59,130,246,0.1) 100%); 
                            padding: 20px; border-radius: 16px; border: 1px solid rgba(59,130,246,0.3);">
                    <div style="color: rgba(255,255,255,0.6); font-size: 12px; text-transform: uppercase; letter-spacing: 1px;">🌧️ Precipitation</div>
                    <div style="color: #fff; font-size: 18px; font-weight: 600; margin-top: 8px;">{precip} mm</div>
                </div>
                <div style="background: linear-gradient(135deg, rgba(34,197,94,0.2) 0%, rgba(34,197,94,0.1) 100%); 
                            padding: 20px; border-radius: 16px; border: 1px solid rgba(34,197,94,0.3);">
                    <div style="color: rgba(255,255,255,0.6); font-size: 12px; text-transform: uppercase; letter-spacing: 1px;">💧 Humidity</div>
                    <div style="color: #fff; font-size: 18px; font-weight: 600; margin-top: 8px;">{rh}%</div>
                </div>
            </div>
        </div>
        """
        
        return summary_html, heatwave_html, flood_html
        
    except Exception as e:
        error_html = f"""
        <div style="background: linear-gradient(135deg, #7f1d1d 0%, #450a0a 100%); 
                    border: 2px solid #dc2626; border-radius: 16px; 
                    padding: 30px; text-align: center;">
            <div style="font-size: 50px; margin-bottom: 15px;">⚠️</div>
            <div style="color: #fca5a5; font-weight: 700; font-size: 20px;">Error Occurred</div>
            <div style="color: #fecaca; margin-top: 10px;">{str(e)}</div>
        </div>
        """
        return error_html, error_html, error_html


# Load models at startup
load_models()

# Create Gradio 6.x interface with latest features
app = gr.Blocks(
    title="🌡️ Heatwave & Flood Predictor",
    theme=gr.themes.Soft(
        primary_hue="indigo",
        secondary_hue="purple",
        neutral_hue="slate",
        font=gr.themes.GoogleFont("Inter")
    )
)

with app:
    # Header
    gr.HTML("""
        <div style="text-align: center; padding: 40px 20px; 
                    background: linear-gradient(135deg, rgba(102,126,234,0.3) 0%, rgba(118,75,162,0.3) 100%); 
                    border-radius: 24px; margin-bottom: 30px;
                    border: 1px solid rgba(255,255,255,0.1);
                    box-shadow: 0 20px 60px rgba(102,126,234,0.2);">
            <h1 style="font-size: 48px; margin: 0; color: white; font-weight: 700;
                       text-shadow: 0 4px 20px rgba(0,0,0,0.3);">
                🌡️ Heatwave & Flood Predictor 🌊
            </h1>
            <p style="font-size: 18px; color: rgba(255,255,255,0.8); margin-top: 15px; font-weight: 400;">
                AI-Powered Weather Risk Assessment System • Built with XGBoost & LSTM
            </p>
        </div>
    """)
    
    with gr.Row():
        # Left Column - Inputs
        with gr.Column(scale=1):
            gr.Markdown("## 📝 Input Parameters")
            
            with gr.Accordion("📍 Location & Date", open=True):
                district = gr.Textbox(
                    label="District/Location Name",
                    placeholder="e.g., Bara, Sarlahi, Mahottari",
                    value="Parsa",
                    info="Enter the name of the location for prediction"
                )
                date = gr.Textbox(
                    label="Date",
                    placeholder="YYYY-MM-DD",
                    value=datetime.now().strftime("%Y-%m-%d"),
                    info="Date for which you want to predict"
                )
            
            with gr.Accordion("🌡️ Temperature Data", open=True):
                max_temp = gr.Slider(
                    label="Maximum Temperature (°C)",
                    minimum=-10, maximum=55, value=35, step=0.5,
                    info="Expected maximum temperature for the day"
                )
                temp_range = gr.Slider(
                    label="Temperature Range (°C)",
                    minimum=0, maximum=30, value=12, step=0.5,
                    info="Difference between max and min temperature"
                )
            
            with gr.Accordion("💧 Precipitation & Humidity", open=True):
                precip = gr.Slider(
                    label="Precipitation (mm)",
                    minimum=0, maximum=500, value=0, step=1,
                    info="Expected rainfall amount"
                )
                rh = gr.Slider(
                    label="Relative Humidity (%)",
                    minimum=0, maximum=100, value=65, step=1,
                    info="Average relative humidity"
                )
            
            with gr.Accordion("💨 Wind Data", open=True):
                wind_10m = gr.Slider(
                    label="Wind Speed at 10m (m/s)",
                    minimum=0, maximum=50, value=5, step=0.5,
                    info="Wind speed measured at 10 meters height"
                )
                wind_50m = gr.Slider(
                    label="Wind Speed at 50m (m/s)",
                    minimum=0, maximum=70, value=8, step=0.5,
                    info="Wind speed measured at 50 meters height"
                )
            
            predict_btn = gr.Button(
                "🔮 Predict Risk", 
                variant="primary", 
                size="lg"
            )
        
        # Right Column - Outputs
        with gr.Column(scale=2):
            gr.Markdown("## 📊 Prediction Results")
            summary_output = gr.HTML(label="Summary")
            
            with gr.Row(equal_height=True):
                heatwave_output = gr.HTML(label="Heatwave Prediction")
                flood_output = gr.HTML(label="Flood Prediction")
    
    # Connect prediction function
    predict_btn.click(
        fn=predict,
        inputs=[district, date, precip, max_temp, rh, temp_range, wind_10m, wind_50m],
        outputs=[summary_output, heatwave_output, flood_output],
        api_name="predict"
    )
    
    # Example inputs
    gr.Examples(
        examples=[
            ["Parsa", "2025-06-15", 0, 45, 30, 15, 3, 5],
            ["Sarlahi", "2025-07-20", 150, 32, 90, 8, 12, 18],
            ["Mahottari", "2025-05-10", 5, 40, 70, 10, 8, 12],
            ["Bara", "2025-08-15", 200, 34, 95, 6, 15, 22],
        ],
        inputs=[district, date, precip, max_temp, rh, temp_range, wind_10m, wind_50m],
        label="🎯 Quick Examples - Click to load sample data"
    )
    
    # Footer
    gr.HTML("""
        <div style="text-align: center; padding: 30px; margin-top: 40px; 
                    border-top: 1px solid rgba(255,255,255,0.1);">
            <p style="color: rgba(255,255,255,0.6); font-size: 14px;">
                Built with 🤖 Machine Learning | Models: XGBoost & LSTM (PyTorch) | Powered by Gradio 6.x
            </p>
            <p style="color: rgba(255,255,255,0.4); font-size: 12px; margin-top: 10px;">
                ⚠️ This is a predictive tool for educational purposes. Always refer to official weather advisories for critical decisions.
            </p>
        </div>
    """)


if __name__ == "__main__":
    print("🚀 Starting Heatwave & Flood Predictor...")
    print(f"📍 Device: {device}")
    app.launch(
        share=True,
        server_name="localhost",
        server_port=7860,
        show_api=True,
        show_error=True
    )
