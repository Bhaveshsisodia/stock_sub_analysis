import sys
import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np

# ✅ Add absolute path of your project directory to sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from driver_service.auth import create_service
from driver_service.driver_manager import DriveManager

# ---------- Data Preparation ----------
@st.cache_data
def load_data():
    CLIENT_SECRET_FILE = os.path.join(project_root, "config", "client_secret.json")
    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive']

    drive_service = create_service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
    manager = DriveManager(drive_service)

    folder_id = manager.get_or_create_folder("final_stock_data")



    df_final = manager.fetch_csv_by_name_as_dataframe("final.csv", folder_id)
    # print(df_final)
    print(type(df_final))


    df_final[['date', 'time']] = df_final['datetime'].str.split('T', expand=True)
    df_final = df_final[['date', 'open', 'high', 'low', 'close', 'volume', 'NSE_BSE_code', 'Category', 'Industry', 'Sector',"Market"]]
    df_final = df_final[df_final["Industry"] != "BhaPra"]
    df_final['date'] = pd.to_datetime(df_final['date'])
    df_final['Category'].replace('Large-Cap','Large-cap',inplace=True)
    df_final['Category'].replace('Mid-Cap','Mid-cap',inplace=True)
    df_final['Category'].replace('Small-Cap','Small-cap',inplace=True)


    return df_final

# ---------- Streamlit UI ----------
st.set_page_config(layout="wide")
st.title("📊 Industry-wise Stock Index (Candlestick + Volume)")

cols_per_row = 3
show_volume = st.checkbox("Show Volume Bars", value=True)
agg_method = st.selectbox("Aggregation Method", ["sum", "mean", "weighted_avg"])
default_zoom_days = st.slider("Default Zoom Window (Days from End)", 30, 180, 60)

pad_days = 5
right_padding_days = 10

if 'loaded' not in st.session_state:
    st.session_state.loaded = False

if st.button("📂 Load & Generate Charts"):
    st.session_state.loaded = True

if st.session_state.loaded:
    with st.spinner("🔄 Generating charts... please wait"):
        df = load_data()

        markets = sorted(df['Market'].dropna().unique())
        selected_market = st.selectbox("Select Market", markets)

        # ✅ Always filter for the selected market (no 'All' option)
        df = df[df['Market'] == selected_market]

        sectors = sorted(df['Sector'].dropna().unique())
        selected_sector = st.selectbox("Select Sector", ["All"] + sectors)

        if selected_sector != "All":
            df = df[df['Sector'] == selected_sector]

        industries = sorted(df['Industry'].unique())
        categories = sorted(df['Category'].unique())
        selected_category = st.selectbox("Select Market Cap Type (Category)", categories)

        start_date, end_date = st.date_input(
            "Select Date Range",
            [df['date'].min(), df['date'].max()],
            min_value=df['date'].min(),
            max_value=df['date'].max()
        )

        row = []
        col_count = 0

        for industry in industries:

            industry_df = df[(df['Industry'] == industry) & (df['Category'] == selected_category)].copy()
            industry_df = industry_df[
                (industry_df['date'] >= pd.Timestamp(start_date)) & (industry_df['date'] <= pd.Timestamp(end_date))
            ]

            if industry_df.empty:
                continue

            sector_name = industry_df['Sector'].iloc[0] if not industry_df['Sector'].isnull().all() else "Unknown"

            if agg_method == 'sum':
                agg_df = industry_df.groupby('date').agg({
                    'open': 'sum', 'high': 'sum', 'low': 'sum', 'close': 'sum', 'volume': 'sum'
                }).reset_index().sort_values('date')
            elif agg_method == 'mean':
                agg_df = industry_df.groupby('date').agg({
                    'open': 'mean', 'high': 'mean', 'low': 'mean', 'close': 'mean', 'volume': 'sum'
                }).reset_index().sort_values('date')
            else:
                weighted_sum = industry_df.groupby('date').apply(lambda g: pd.Series({
                    'open': (g['open'] * g['volume']).sum() / g['volume'].sum(),
                    'high': (g['high'] * g['volume']).sum() / g['volume'].sum(),
                    'low': (g['low'] * g['volume']).sum() / g['volume'].sum(),
                    'close': (g['close'] * g['volume']).sum() / g['volume'].sum(),
                    'volume': g['volume'].sum()
                })).reset_index().sort_values('date')
                agg_df = weighted_sum

            # Padding for visualization clarity
            padding = pd.DataFrame({
                'date': pd.date_range(start=agg_df['date'].min() - pd.Timedelta(days=pad_days), periods=pad_days),
                'open': np.nan, 'high': np.nan, 'low': np.nan, 'close': np.nan, 'volume': 0
            })
            agg_df = pd.concat([padding, agg_df], ignore_index=True)

            fig = go.Figure()
            fig.add_trace(go.Candlestick(
                x=agg_df['date'], open=agg_df['open'], high=agg_df['high'],
                low=agg_df['low'], close=agg_df['close'],
                name='Price', increasing_line_color='limegreen', decreasing_line_color='red'
            ))

            if show_volume:
                fig.add_trace(go.Bar(
                    x=agg_df['date'], y=agg_df['volume'], name='Volume',
                    marker_color='rgba(135, 206, 250, 0.2)', yaxis='y2'
                ))

            zoom_start = agg_df['date'].max() - pd.Timedelta(days=default_zoom_days)
            zoom_end = agg_df['date'].max() + pd.Timedelta(days=right_padding_days)

            fig.update_layout(
                xaxis=dict(range=[zoom_start, zoom_end], autorange=False),
                title={
                    'text': f"Industry: {industry}<br><span style='font-size:10pt'>Sector: {sector_name}</span>",
                    'x': 0.5, 'xanchor': 'center'
                },
                xaxis_title="Date", yaxis_title="Price",
                yaxis2=dict(overlaying='y', side='right', title='Volume', showgrid=False),
                height=400, plot_bgcolor="#111", paper_bgcolor="#111", font=dict(color='white'),
                margin=dict(t=30, b=20), xaxis_rangeslider_visible=False, showlegend=False,
            )

            row.append(fig)
            col_count += 1

            if col_count == cols_per_row:
                cols = st.columns(cols_per_row)
                for i, col in enumerate(cols):
                    col.plotly_chart(row[i], use_container_width=True)
                row = []
                col_count = 0

        # Remaining charts
        if row:
            cols = st.columns(len(row))
            for i, col in enumerate(cols):
                col.plotly_chart(row[i], use_container_width=True)
