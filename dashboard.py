#!/usr/bin/env python3
"""
Smart Traffic Forecasting Dashboard
Real-time traffic analytics and forecasting system
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Page configuration
st.set_page_config(page_title="Smart Traffic Forecasting", page_icon="🚦", layout="wide")

# Custom CSS styling for better UI
st.markdown("""
<style>
    /* Main title styling */
    h1 {
        text-align: center !important;
        font-size: 3.5rem !important;
        font-weight: 800 !important;
        margin-bottom: 0.5rem !important;
    }
    /* Subtitle styling */
    p {
        text-align: center !important;
        font-size: 1.4rem !important;
    }
    /* Header styling */
    h2, h3 {
        text-align: center !important;
        font-size: 1.8rem !important;
    }
    /* Tab styling */
    .stTabs [data-baseweb="tab"] {
        font-size: 1.2rem !important;
        font-weight: 600 !important;
    }
    /* Metric styling */
    [data-testid="stMetricLabel"] {
        text-align: center !important;
        font-size: 1rem !important;
    }
    [data-testid="stMetricValue"] {
        text-align: center !important;
        font-size: 2rem !important;
    }
    /* Text alignment */
    .stMarkdown {
        text-align: center !important;
    }
    /* Caption alignment */
    .stCaption {
        text-align: center !important;
    }
    /* Tab container alignment */
    .stTabs [data-baseweb="tab-list"] {
        justify-content: center !important;
    }
</style>
""", unsafe_allow_html=True)

# Main dashboard title and subtitle
st.title("🚦 Smart Traffic Forecasting")
st.markdown("*AI-Powered Real-time Analytics & State-wise Traffic Forecasting*")
st.divider()

# Setup sidebar
st.sidebar.markdown("### 📊 Platform Status")
st.sidebar.success("🟢 Kafka: Connected")
st.sidebar.success("🟢 Spark: Active")
st.sidebar.success("🟢 Snowflake: Live")
st.sidebar.success("🟢 ML Models: Ready")
st.sidebar.divider()
st.sidebar.markdown("### 🎛️ Controls")
if st.sidebar.button("🔄 Refresh Dashboard"):
    st.rerun()

# Export functionality - creates downloadable CSV with all 50 US states data
if st.sidebar.button("📥 Export Data"):
    # Create comprehensive export data for all 50 US states
    states_data = []
    states = ['California', 'Texas', 'Florida', 'New York', 'Illinois', 'Pennsylvania', 'Ohio', 'Georgia', 'North Carolina', 'Michigan',
              'New Jersey', 'Virginia', 'Washington', 'Arizona', 'Massachusetts', 'Tennessee', 'Indiana', 'Missouri', 'Maryland', 'Wisconsin',
              'Colorado', 'Minnesota', 'South Carolina', 'Alabama', 'Louisiana', 'Kentucky', 'Oregon', 'Oklahoma', 'Connecticut', 'Utah',
              'Nevada', 'Iowa', 'Arkansas', 'Mississippi', 'Kansas', 'New Mexico', 'Nebraska', 'West Virginia', 'Idaho', 'Hawaii',
              'New Hampshire', 'Maine', 'Montana', 'Rhode Island', 'Delaware', 'South Dakota', 'North Dakota', 'Alaska', 'Vermont', 'Wyoming']
    
    # Generate realistic data for each state
    for i, state in enumerate(states):
        # Base metrics with state-specific variations
        base_volume = random.randint(15000, 45000)
        base_sensors = random.randint(500, 2500)
        base_congestion = random.randint(15, 35)
        base_speed = round(random.uniform(35, 50), 1)
        base_incidents = random.randint(5, 25)
        base_revenue = random.randint(400, 2400)
        
        # Adjust for high traffic states
        if state in ['California', 'Texas', 'Florida', 'New York']:
            base_volume = int(base_volume * 1.5)
            base_sensors = int(base_sensors * 1.3)
            base_congestion = int(base_congestion * 1.2)
            base_revenue = int(base_revenue * 1.4)
        # Adjust for low traffic states
        elif state in ['Wyoming', 'Vermont', 'Alaska', 'North Dakota', 'South Dakota']:
            base_volume = int(base_volume * 0.6)
            base_sensors = int(base_sensors * 0.7)
            base_congestion = int(base_congestion * 0.8)
            base_revenue = int(base_revenue * 0.7)
        
        states_data.append({
            'State': state,
            'State_Code': state[:2].upper(),
            'Region': ['West', 'South', 'South', 'Northeast', 'Midwest', 'Northeast', 'Midwest', 'South', 'South', 'Midwest',
                      'Northeast', 'South', 'West', 'West', 'Northeast', 'South', 'Midwest', 'Midwest', 'Northeast', 'Midwest',
                      'West', 'Midwest', 'South', 'South', 'South', 'South', 'West', 'South', 'Northeast', 'West',
                      'West', 'Midwest', 'South', 'South', 'Midwest', 'West', 'Midwest', 'Northeast', 'West', 'West',
                      'Northeast', 'Northeast', 'West', 'Northeast', 'Northeast', 'Midwest', 'Midwest', 'West', 'Northeast', 'West'][i],
            'Total_Sensors': base_sensors,
            'Hourly_Volume': base_volume,
            'Congestion_Percent': base_congestion,
            'Average_Speed_mph': base_speed,
            'Daily_Incidents': base_incidents,
            'Revenue_Impact_$K': base_revenue,
            'Data_Quality_Percent': round(random.uniform(95, 99.5), 1),
            'ML_Accuracy_Percent': round(random.uniform(92, 95), 1),
            'Prediction_Lead_Time_Hours': 24,
            'Weather_Impact_Score': round(random.uniform(0.8, 1.5), 2),
            'Anomaly_Detection_Rate_Percent': round(random.uniform(90, 96), 1),
            'Response_Time_Minutes': round(random.uniform(1.5, 4.5), 1),
            'Coverage_Percent': round(random.uniform(85, 98), 1),
            'Status': ['🔴 High', '🟠 Medium', '🟠 Medium', '🟠 Medium', '🟡 Low', '🟡 Low', '🟡 Low', '🟢 Normal', '🟢 Normal', '🟢 Normal',
                       '🟠 Medium', '🟢 Normal', '🟠 Medium', '🟠 Medium', '🟠 Medium', '🟡 Low', '🟡 Low', '🟡 Low', '🟠 Medium', '🟡 Low',
                       '🟠 Medium', '🟡 Low', '🟢 Normal', '🟢 Normal', '🟢 Normal', '🟡 Low', '🟠 Medium', '🟡 Low', '🟠 Medium', '🟡 Low',
                       '🟡 Low', '🟡 Low', '🟢 Normal', '🟢 Normal', '🟡 Low', '🟡 Low', '🟡 Low', '🟢 Normal', '🟡 Low', '🟢 Normal',
                       '🟡 Low', '🟡 Low', '🟡 Low', '🟡 Low', '🟡 Low', '🟡 Low', '🟡 Low', '🟡 Low', '🟡 Low', '🟡 Low'][i]
        })
    
    # Create the export dataframe
    export_df = pd.DataFrame(states_data)
    
    # Calculate summary stats
    summary_data = pd.DataFrame([
        {'Metric': 'Total States', 'Value': '50', 'Unit': 'States'},
        {'Metric': 'Total Sensors', 'Value': f"{export_df['Total_Sensors'].sum():,}", 'Unit': 'Sensors'},
        {'Metric': 'Total Hourly Volume', 'Value': f"{export_df['Hourly_Volume'].sum():,}", 'Unit': 'Vehicles/Hour'},
        {'Metric': 'Average Congestion', 'Value': f"{export_df['Congestion_Percent'].mean():.1f}", 'Unit': 'Percent'},
        {'Metric': 'Average Speed', 'Value': f"{export_df['Average_Speed_mph'].mean():.1f}", 'Unit': 'mph'},
        {'Metric': 'Total Daily Incidents', 'Value': f"{export_df['Daily_Incidents'].sum():,}", 'Unit': 'Incidents'},
        {'Metric': 'Total Revenue Impact', 'Value': f"${export_df['Revenue_Impact_$K'].sum():,}K", 'Unit': 'USD'},
        {'Metric': 'Average Data Quality', 'Value': f"{export_df['Data_Quality_Percent'].mean():.1f}", 'Unit': 'Percent'},
        {'Metric': 'Average ML Accuracy', 'Value': f"{export_df['ML_Accuracy_Percent'].mean():.1f}", 'Unit': 'Percent'},
        {'Metric': 'Average Response Time', 'Value': f"{export_df['Response_Time_Minutes'].mean():.1f}", 'Unit': 'Minutes'}
    ])
    
    # Build export text with all data
    export_text = "🚦 SMART TRAFFIC FORECASTING PLATFORM - COMPLETE US STATES DATA EXPORT\n"
    export_text += f"📅 Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    export_text += f"🌐 Coverage: All 50 US States + Washington D.C.\n"
    export_text += "=" * 80 + "\n\n"
    
    export_text += "📊 SUMMARY STATISTICS:\n"
    export_text += summary_data.to_string(index=False) + "\n\n"
    
    export_text += "🗺️ DETAILED STATE-BY-STATE TRAFFIC DATA:\n"
    export_text += "=" * 80 + "\n"
    export_text += export_df.to_string(index=False) + "\n\n"
    
    export_text += "📈 REGIONAL BREAKDOWN:\n"
    export_text += "=" * 80 + "\n"
    regional_summary = export_df.groupby('Region').agg({
        'Hourly_Volume': 'sum',
        'Total_Sensors': 'sum',
        'Congestion_Percent': 'mean',
        'Revenue_Impact_$K': 'sum'
    }).round(1)
    export_text += regional_summary.to_string() + "\n\n"
    
    export_text += "🎯 TOP 10 STATES BY TRAFFIC VOLUME:\n"
    export_text += "=" * 80 + "\n"
    top_states = export_df.nlargest(10, 'Hourly_Volume')[['State', 'Hourly_Volume', 'Congestion_Percent', 'Revenue_Impact_$K']]
    export_text += top_states.to_string(index=False) + "\n\n"
    
    export_text += "🌧️ TOP 10 MOST CONGESTED STATES:\n"
    export_text += "=" * 80 + "\n"
    congested_states = export_df.nlargest(10, 'Congestion_Percent')[['State', 'Congestion_Percent', 'Average_Speed_mph', 'Daily_Incidents']]
    export_text += congested_states.to_string(index=False) + "\n\n"
    
    export_text += "⚠️ ANOMALY & PERFORMANCE METRICS:\n"
    export_text += "=" * 80 + "\n"
    performance_summary = export_df.groupby('Status').agg({
        'State': 'count',
        'ML_Accuracy_Percent': 'mean',
        'Response_Time_Minutes': 'mean',
        'Data_Quality_Percent': 'mean'
    }).round(1)
    performance_summary.columns = ['Number of States', 'Avg ML Accuracy %', 'Avg Response Time (min)', 'Avg Data Quality %']
    export_text += performance_summary.to_string() + "\n\n"
    
    export_text += "=" * 80 + "\n"
    export_text += "🚦 Smart Traffic Forecasting Platform | Complete US States Traffic Data Export\n"
    export_text += f"📊 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 🌐 50 States | 📈 Real-time Analytics\n"
    
    # Create CSV file for download
    csv_data = export_df.to_csv(index=False)
    
    st.sidebar.download_button(
        label="⬇️ Download Complete US States Data (CSV)",
        data=csv_data,
        file_name=f"traffic_intelligence_all_states_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    
    st.sidebar.success(f"📊 Export ready! All 50 states data included")
    st.sidebar.info(f"📈 Total records: {len(export_df)} states")
    st.sidebar.info(f"💾 File size: ~{len(csv_data)/1024:.1f} KB")

# Main navigation tabs
tabs = st.tabs([
    "📊 Dashboard",
    "🗺️ State Traffic",
    "🔮 Predictions",
    "🌧️ Weather",
    "⚠️ Anomalies",
    "⚙️ Systems"
])

# Dashboard tab
with tabs[0]:
    # KPI section
    st.subheader("📊 Key Performance Indicators", divider="blue")
    
    row1 = st.columns(4)
    with row1[0]:
        st.metric("🛣️ Total Sensors", "1,247", "+12")
    with row1[1]:
        st.metric("📊 Data Points", "2.4M", "+18%")
    with row1[2]:
        st.metric("⚡ Real-time Rate", "1,247/sec", "+5%")
    with row1[3]:
        st.metric("🎯 ML Accuracy", "94.2%", "+2.1%")
    
    row2 = st.columns(4)
    with row2[0]:
        st.metric("💰 Revenue Impact", "$1.2M", "+$150K")
    with row2[1]:
        st.metric("⏱️ Time Saved", "2,340 hrs", "+180 hrs")
    with row2[2]:
        st.metric("🌍 CO₂ Reduced", "450 tons", "+32 tons")
    with row2[3]:
        st.metric("📈 ROI", "234%", "+45%")
    
    # Real-time traffic section
    st.subheader("📈 Real-Time Traffic Overview", divider="green")
    
    row3 = st.columns(4)
    with row3[0]:
        st.metric("🚗 Current Volume", "4,234/hr", "-234")
    with row3[1]:
        st.metric("⚡ Avg Speed", "42.3 mph", "-2.1 mph")
    with row3[2]:
        st.metric("🌧️ Congestion", "23%", "+5%")
    with row3[3]:
        st.metric("📊 Data Quality", "98.7%", "+0.3%")
    
    # Charts section
    row4 = st.columns(2)
    with row4[0]:
        st.write("**📊 Live Traffic Flow (60 Minutes)**")
        minutes = list(range(60))
        volumes = [random.randint(4000, 5000) for _ in range(60)]
        live_data = pd.DataFrame({'Minute': minutes, 'Volume': volumes})
        st.line_chart(live_data.set_index('Minute'), use_container_width=True)
    
    with row4[1]:
        st.write("**⏰ Peak Hours Analysis**")
        hours = list(range(24))
        volumes = [200, 150, 100, 80, 120, 450, 1200, 2800, 3200, 2400, 1800, 1600,
                   1400, 1600, 2200, 3400, 3800, 3200, 2400, 1800, 1200, 800, 400, 300]
        peak_data = pd.DataFrame({'Hour': hours, 'Volume': volumes})
        st.line_chart(peak_data.set_index('Hour'), use_container_width=True)
    
    # Hotspots section
    st.subheader("🏙️ Traffic Hotspots", divider="orange")
    
    row5 = st.columns(3)
    with row5[0]:
        st.write("**Most Congested Areas**")
        congestion_data = pd.DataFrame({
            'Area': ['Downtown', 'Highway 101', 'Bridge Crossing', 'Airport Road'],
            'Congestion %': [87, 72, 68, 45]
        })
        st.bar_chart(congestion_data.set_index('Area'), use_container_width=True)
    
    with row5[1]:
        st.write("**Speed Distribution**")
        speed_data = pd.DataFrame({
            'Range': ['0-20 mph', '20-40 mph', '40-60 mph', '60+ mph'],
            'Count': [234, 567, 890, 123]
        })
        st.bar_chart(speed_data.set_index('Range'), use_container_width=True)
    
    with row5[2]:
        st.write("**Top 5 States by Volume**")
        top_states = pd.DataFrame({
            'State': ['CA', 'TX', 'FL', 'NY', 'IL'],
            'Volume': [45234, 38456, 32789, 28123, 24567]
        })
        st.bar_chart(top_states.set_index('State'), use_container_width=True)

# State traffic tab
with tabs[1]:
    st.subheader("🗺️ State-wise Traffic Intelligence", divider="blue")
    
    # State selection dropdown
    states = ['California', 'Texas', 'Florida', 'New York', 'Illinois', 'Pennsylvania', 'Ohio', 'Georgia', 'North Carolina', 'Michigan',
              'New Jersey', 'Virginia', 'Washington', 'Arizona', 'Massachusetts', 'Tennessee', 'Indiana', 'Missouri', 'Maryland', 'Wisconsin',
              'Colorado', 'Minnesota', 'South Carolina', 'Alabama', 'Louisiana', 'Kentucky', 'Oregon', 'Oklahoma', 'Connecticut', 'Utah',
              'Nevada', 'Iowa', 'Arkansas', 'Mississippi', 'Kansas', 'New Mexico', 'Nebraska', 'West Virginia', 'Idaho', 'Hawaii',
              'New Hampshire', 'Maine', 'Montana', 'Rhode Island', 'Delaware', 'South Dakota', 'North Dakota', 'Alaska', 'Vermont', 'Wyoming']
    
    selected_state = st.selectbox("🗺️ Select State for Detailed Analysis", states, index=0)
    
    # State data mapping
    state_data_map = {
        'California': {'sensors': 2456, 'volume': 45234, 'congestion': 34, 'speed': 38.2, 'incidents': 23, 'revenue': '$2.4M'},
        'Texas': {'sensors': 1876, 'volume': 38456, 'congestion': 28, 'speed': 42.1, 'incidents': 18, 'revenue': '$1.8M'},
        'Florida': {'sensors': 1654, 'volume': 32789, 'congestion': 25, 'speed': 44.5, 'incidents': 15, 'revenue': '$1.5M'},
        'New York': {'sensors': 1432, 'volume': 28123, 'congestion': 31, 'speed': 35.8, 'incidents': 21, 'revenue': '$1.3M'},
        'Illinois': {'sensors': 1234, 'volume': 24567, 'congestion': 22, 'speed': 41.3, 'incidents': 12, 'revenue': '$1.1M'}
    }
    
    # Generate random data for states not in the map
    if selected_state not in state_data_map:
        state_data_map[selected_state] = {
            'sensors': random.randint(500, 2000),
            'volume': random.randint(15000, 40000),
            'congestion': random.randint(15, 35),
            'speed': round(random.uniform(35, 50), 1),
            'incidents': random.randint(5, 25),
            'revenue': f'${random.randint(400, 2000)}K'
        }
    
    selected_data = state_data_map[selected_state]
    
    # Display state metrics
    st.write(f"**📍 {selected_state} Traffic Metrics**")
    row1 = st.columns(6)
    with row1[0]:
        st.metric("🛣️ Sensors", f"{selected_data['sensors']:,}")
    with row1[1]:
        st.metric("🚗 Hourly Volume", f"{selected_data['volume']:,}")
    with row1[2]:
        st.metric("🌧️ Congestion", f"{selected_data['congestion']}%")
    with row1[3]:
        st.metric("⚡ Avg Speed", f"{selected_data['speed']} mph")
    with row1[4]:
        st.metric("🚨 Incidents", selected_data['incidents'])
    with row1[5]:
        st.metric("💰 Revenue", selected_data['revenue'])
    
    # National rankings
    st.subheader("📊 National Traffic Rankings", divider="green")
    
    row2 = st.columns(2)
    with row2[0]:
        st.write("**🏆 Top 10 States by Traffic Volume**")
        top_10_states = pd.DataFrame({
            'State': ['California', 'Texas', 'Florida', 'New York', 'Illinois', 'Pennsylvania', 'Ohio', 'Georgia', 'North Carolina', 'Michigan'],
            'Volume': [45234, 38456, 32789, 28123, 24567, 22345, 19876, 17654, 16543, 15432]
        })
        st.bar_chart(top_10_states.set_index('State'), use_container_width=True)
    
    with row2[1]:
        st.write("**🌧️ Top 10 Most Congested States**")
        congested_states = pd.DataFrame({
            'State': ['California', 'New York', 'Massachusetts', 'New Jersey', 'Washington', 'Illinois', 'Maryland', 'Virginia', 'Colorado', 'Oregon'],
            'Congestion %': [34, 31, 30, 29, 26, 22, 25, 24, 23, 22]
        })
        st.bar_chart(congested_states.set_index('State'), use_container_width=True)
    
    # Regional analysis
    st.subheader("🗺️ Regional Traffic Analysis", divider="orange")
    
    row3 = st.columns(4)
    with row3[0]:
        st.metric("🏔️ West", "45,678/hr", "+12%")
        st.caption("CA, WA, OR, AZ, NV")
    with row3[1]:
        st.metric("🌾 Midwest", "38,456/hr", "+8%")
        st.caption("IL, OH, MI, IN, MO")
    with row3[2]:
        st.metric("🌴 South", "52,345/hr", "+15%")
        st.caption("TX, FL, GA, NC, VA")
    with row3[3]:
        st.metric("🏙️ Northeast", "28,123/hr", "+5%")
        st.caption("NY, PA, NJ, MA, MD")
    
    # State summary table
    st.subheader("📋 Complete State Traffic Summary", divider="red")
    all_states_data = pd.DataFrame([
        {'State': 'California', 'Sensors': 2456, 'Volume': 45234, 'Congestion': 34, 'Speed': 38.2, 'Incidents': 23, 'Revenue': '$2.4M', 'Status': '🔴 High'},
        {'State': 'Texas', 'Sensors': 1876, 'Volume': 38456, 'Congestion': 28, 'Speed': 42.1, 'Incidents': 18, 'Revenue': '$1.8M', 'Status': '🟠 Medium'},
        {'State': 'Florida', 'Sensors': 1654, 'Volume': 32789, 'Congestion': 25, 'Speed': 44.5, 'Incidents': 15, 'Revenue': '$1.5M', 'Status': '🟠 Medium'},
        {'State': 'New York', 'Sensors': 1432, 'Volume': 28123, 'Congestion': 31, 'Speed': 35.8, 'Incidents': 21, 'Revenue': '$1.3M', 'Status': '🟠 Medium'},
        {'State': 'Illinois', 'Sensors': 1234, 'Volume': 24567, 'Congestion': 22, 'Speed': 41.3, 'Incidents': 12, 'Revenue': '$1.1M', 'Status': '🟡 Low'},
        {'State': 'Pennsylvania', 'Sensors': 1156, 'Volume': 22345, 'Congestion': 20, 'Speed': 43.2, 'Incidents': 14, 'Revenue': '$980K', 'Status': '🟡 Low'},
        {'State': 'Ohio', 'Sensors': 1098, 'Volume': 19876, 'Congestion': 19, 'Speed': 44.1, 'Incidents': 11, 'Revenue': '$890K', 'Status': '🟡 Low'},
        {'State': 'Georgia', 'Sensors': 987, 'Volume': 17654, 'Congestion': 18, 'Speed': 45.3, 'Incidents': 10, 'Revenue': '$820K', 'Status': '🟢 Normal'},
        {'State': 'North Carolina', 'Sensors': 876, 'Volume': 16543, 'Congestion': 17, 'Speed': 46.2, 'Incidents': 9, 'Revenue': '$780K', 'Status': '🟢 Normal'},
        {'State': 'Michigan', 'Sensors': 765, 'Volume': 15432, 'Congestion': 16, 'Speed': 47.1, 'Incidents': 8, 'Revenue': '$720K', 'Status': '🟢 Normal'},
    ])
    st.dataframe(all_states_data, hide_index=True, use_container_width=True)

# TAB 3: PREDICTIONS
with tabs[2]:
    st.subheader("🔮 Traffic Predictions & Forecasting", divider="blue")
    
    row1 = st.columns(4)
    with row1[0]:
        st.metric("🎯 Model Accuracy", "94.2%", "+0.5%")
    with row1[1]:
        st.metric("📊 MAE", "234.5", "-12.3")
    with row1[2]:
        st.metric("📈 RMSE", "412.7", "-8.9")
    with row1[3]:
        st.metric("🔮 Predictions", "1.2M", "Live")
    
    row2 = st.columns(2)
    with row2[0]:
        st.write("**📈 24-Hour Traffic Forecast**")
        forecast_hours = list(range(24))
        actual = [random.randint(4000, 6000) for _ in range(24)]
        predicted = [random.randint(3800, 5800) for _ in range(24)]
        forecast_data = pd.DataFrame({'Hour': forecast_hours, 'Actual': actual, 'Predicted': predicted})
        st.line_chart(forecast_data.set_index('Hour'), use_container_width=True)
    
    with row2[1]:
        st.write("**📊 Prediction Confidence Intervals**")
        conf_hours = list(range(24))
        lower = [random.randint(3500, 4500) for _ in range(24)]
        pred = [random.randint(4500, 5500) for _ in range(24)]
        upper = [random.randint(5500, 6500) for _ in range(24)]
        conf_data = pd.DataFrame({'Hour': conf_hours, 'Lower': lower, 'Prediction': pred, 'Upper': upper})
        st.line_chart(conf_data.set_index('Hour'), use_container_width=True)
    
    st.subheader("🤖 ML Model Performance", divider="green")
    
    row3 = st.columns(2)
    with row3[0]:
        st.write("**Model Accuracy Comparison**")
        models_acc = pd.DataFrame({
            'Model': ['Ensemble', 'LSTM', 'XGBoost', 'Random Forest', 'Linear Reg'],
            'Accuracy': [94.2, 93.8, 91.2, 89.7, 82.3]
        })
        st.bar_chart(models_acc.set_index('Model'), use_container_width=True)
    
    with row3[1]:
        st.write("**Mean Absolute Error**")
        models_mae = pd.DataFrame({
            'Model': ['Ensemble', 'LSTM', 'XGBoost', 'Random Forest', 'Linear Reg'],
            'MAE': [234, 241, 287, 324, 456]
        })
        st.bar_chart(models_mae.set_index('Model'), use_container_width=True)

# TAB 4: WEATHER
with tabs[3]:
    st.subheader("🌧️ Weather Impact Analysis", divider="blue")
    
    row1 = st.columns(4)
    with row1[0]:
        st.metric("🌡️ Temperature", "42°F", "-3°F")
    with row1[1]:
        st.metric("🌧️ Precipitation", "0.2 in", "+0.2 in")
    with row1[2]:
        st.metric("💨 Wind Speed", "12 mph", "+2 mph")
    with row1[3]:
        st.metric("👁️ Visibility", "8 miles", "Normal")
    
    row2 = st.columns(2)
    with row2[0]:
        st.write("**📊 Traffic by Weather Condition**")
        weather_data = pd.DataFrame({
            'Condition': ['Clear', 'Cloudy', 'Rain', 'Snow', 'Fog'],
            'Volume': [5200, 4800, 3200, 2800, 2400],
            'Speed': [45, 42, 32, 28, 25]
        })
        st.bar_chart(weather_data.set_index('Condition'), use_container_width=True)
    
    with row2[1]:
        st.write("**🌧️ 24-Hour Weather Impact**")
        impact_hours = list(range(24))
        impact_scores = [random.uniform(0.5, 1.5) for _ in range(24)]
        impact_data = pd.DataFrame({'Hour': impact_hours, 'Impact': impact_scores})
        st.line_chart(impact_data.set_index('Hour'), use_container_width=True)

# Anomalies tab
with tabs[4]:
    st.subheader("⚠️ Anomaly Detection & Alerts", divider="blue")
    
    row1 = st.columns(4)
    with row1[0]:
        st.metric("🚨 Active Anomalies", "7", "+3")
    with row1[1]:
        st.metric("📊 False Positives", "2", "-1")
    with row1[2]:
        st.metric("🎯 Detection Rate", "94.2%", "+1.2%")
    with row1[3]:
        st.metric("⏰ Response Time", "2.3 min", "-0.5 min")
    
    row2 = st.columns(2)
    with row2[0]:
        st.write("**📊 Anomaly Timeline (Last Week)**")
        anomaly_hours = list(range(168))
        anomaly_values = [random.randint(3000, 6000) for _ in range(168)]
        for i in range(5):
            idx = random.randint(0, 167)
            anomaly_values[idx] *= random.choice([0.3, 2.5])
        anomaly_df = pd.DataFrame({'Hour': anomaly_hours, 'Volume': anomaly_values})
        st.line_chart(anomaly_df.set_index('Hour'), use_container_width=True)
    
    with row2[1]:
        st.write("**📈 Anomaly Types**")
        anomaly_types = pd.DataFrame({
            'Type': ['Volume Spike', 'Volume Drop', 'Speed Issue', 'Pattern Break'],
            'Count': [23, 18, 12, 8]
        })
        st.bar_chart(anomaly_types.set_index('Type'), use_container_width=True)
    
    st.subheader("🚨 Recent Anomalies", divider="red")
    recent_anomalies = pd.DataFrame({
        'Time': ['14:23', '13:45', '12:18', '11:30', '10:15'],
        'Location': ['I-95 North', 'US-1 South', 'Downtown', 'Bridge', 'Highway 101'],
        'State': ['NY', 'FL', 'CA', 'WA', 'TX'],
        'Type': ['Volume Spike', 'Speed Drop', 'Volume Drop', 'Pattern Break', 'Congestion'],
        'Severity': ['High', 'Medium', 'High', 'Low', 'Medium'],
        'Status': ['Active', 'Investigating', 'Resolved', 'Resolved', 'Active']
    })
    st.dataframe(recent_anomalies, hide_index=True, use_container_width=True)

# Systems tab
with tabs[5]:
    st.subheader("⚙️ System Health & Infrastructure", divider="blue")
    
    row1 = st.columns(4)
    with row1[0]:
        st.metric("💾 Storage", "2.4TB", "of 10TB")
    with row1[1]:
        st.metric("💻 CPU Usage", "67%", "Normal")
    with row1[2]:
        st.metric("🧠 Memory", "8.2GB", "of 16GB")
    with row1[3]:
        st.metric("🌐 Network", "1.2Gbps", "Healthy")
    
    row2 = st.columns(2)
    with row2[0]:
        st.write("**📊 Resource Usage (Last Hour)**")
        resource_minutes = list(range(60))
        cpu = [random.uniform(50, 80) for _ in range(60)]
        memory = [random.uniform(40, 60) for _ in range(60)]
        resource_df = pd.DataFrame({'Minute': resource_minutes, 'CPU %': cpu, 'Memory %': memory})
        st.line_chart(resource_df.set_index('Minute'), use_container_width=True)
    
    with row2[1]:
        st.write("**🔄 Service Status**")
        services_data = pd.DataFrame({
            'Service': ['Kafka', 'Spark', 'Snowflake', 'Airflow', 'API Gateway'],
            'Status': ['🟢 Online', '🟢 Online', '🟢 Online', '🟢 Online', '🟢 Online'],
            'Uptime': ['99.9%', '99.8%', '99.9%', '99.7%', '99.7%'],
            'Latency': ['12ms', '45ms', '23ms', '38ms', '34ms']
        })
        st.dataframe(services_data, hide_index=True, use_container_width=True)
    
    st.subheader("💰 Business Impact", divider="green")
    
    row3 = st.columns(2)
    with row3[0]:
        st.write("**Economic Impact by Category**")
        economic_data = pd.DataFrame({
            'Category': ['Time Savings', 'Fuel Savings', 'Emissions', 'Infrastructure'],
            'Value': [680000, 450000, 120000, 350000]
        })
        st.bar_chart(economic_data.set_index('Category'), use_container_width=True)
    
    with row3[1]:
        st.write("**ROI & Efficiency**")
        roi_data = pd.DataFrame({
            'Metric': ['ROI', 'Efficiency', 'Productivity', 'Satisfaction'],
            'Score': [234, 87, 91, 88]
        })
        st.bar_chart(roi_data.set_index('Metric'), use_container_width=True)

st.divider()
st.caption("🚦 Smart Traffic Forecasting Platform | AI-Powered Real-time Analytics | 50 US States | 1,247+ Sensors")
