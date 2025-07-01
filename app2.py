import sys
import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
# ✅ Add absolute path of your project directory to sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from driver_service.auth import create_service
from driver_service.driver_manager import DriveManager

import subprocess



# ---------- Data Preparation ----------
@st.cache_data
def load_data():
    CLIENT_SECRET_FILE = os.path.join(project_root, "config", "client_secret.json")
    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive']

    drive_service = create_service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
    manager = DriveManager(drive_service)

    folder_id = manager.get_or_create_folder("bhavcopy_stock_data")
    df_final = manager.fetch_csv_by_name_as_dataframe("complete_data1.csv", folder_id)

    folder_id_vcp = manager.get_or_create_folder("vcp_folder")
    vcp = manager.fetch_csv_by_name_as_dataframe(f'{(datetime.today()-timedelta(0)).strftime("%d%m%Y")}.csv', folder_id_vcp)




    df_final[['date', 'time']] = df_final['datetime'].str.split('T', expand=True)
    df_final = df_final[['date', 'open', 'high', 'low', 'close', 'volume', 'NSE_BSE_code', 'Category',
                         'Industry', 'Mapped Sector', "market", "Sub Industry"]]
    df_final = df_final[df_final["Industry"] != "BhaPra"]
    df_final['date'] = pd.to_datetime(df_final['date'])

    return df_final , vcp

# ---------- Streamlit UI ----------
st.set_page_config(layout="wide")
st.title("📊 Industry/Sub-Industry Stock Index (Candlestick + Volume)")

st.title("🔘 Run Vcp and NSE BSE Data Pushing to Drive Script on Button Click")

if st.button("🚀 Refresh Data "):
    with st.spinner("Running your script..."):
        try:
            result = subprocess.run(["python", "server.py"], check=True, capture_output=True, text=True)
            st.code(result.stdout)  # Optionally show output
            st.success("✅ Script executed successfully!")
        except subprocess.CalledProcessError as e:
            st.error("❌ Script failed to run.")
            st.code(e.stderr)

cols_per_row = 3
show_volume = st.checkbox("Show Volume Bars", value=True)
agg_method = st.selectbox("Aggregation Method", ["sum", "mean", "weighted_avg"])
default_zoom_days = st.slider("Default Zoom Window (Days from End)", 10, 180, 60)

aggregation_level = st.radio("📌 Select Aggregation Level:", ["Industry", "Sub Industry"])

pad_days = 5
right_padding_days = 10

if 'loaded' not in st.session_state:
    st.session_state.loaded = False

if st.button("📂 Load & Generate Charts"):
    st.session_state.loaded = True

if st.session_state.loaded:
    with st.spinner("🔄 Generating charts... please wait"):
        df, vcp = load_data()

        markets = sorted(df['market'].dropna().unique())
        selected_market = st.selectbox("Select Market", ["All"] + markets)
        if selected_market != "All":
            df = df[df['market'] == selected_market]

        sectors = sorted(df['Mapped Sector'].dropna().unique())
        selected_sector = st.selectbox("Select Sector", ["All"] + sectors)
        if selected_sector != "All":
            df = df[df['Mapped Sector'] == selected_sector]

        industries = sorted(df['Industry'].dropna().unique())
        selected_industry = st.selectbox("Select Industry", ["All"] + industries)
        if selected_industry != "All":
            df = df[df['Industry'] == selected_industry]

        sub_industries = sorted(df['Sub Industry'].dropna().unique())
        selected_sub_industry = st.selectbox("Select Sub Industry", ["All"] + sub_industries)
        if selected_sub_industry != "All":
            df = df[df['Sub Industry'] == selected_sub_industry]

        categories = sorted(df['Category'].dropna().unique())
        selected_category = st.selectbox("Select Market Cap Type (Category)", ["All"] + list(categories))

        start_date, end_date = st.date_input(
            "Select Date Range",
            [df['date'].min(), df['date'].max()],
            min_value=df['date'].min(),
            max_value=df['date'].max()
        )

        if aggregation_level == "Sub Industry":
            hierarchy_df = df[['Industry', 'Sub Industry']].drop_duplicates().dropna().sort_values(['Industry', 'Sub Industry'])
        else:
            hierarchy_df = df[['Industry']].drop_duplicates().dropna().sort_values(['Industry'])

        row = []
        col_count = 0

        for _, row_info in hierarchy_df.iterrows():
            if aggregation_level == "Sub Industry":
                industry_name = row_info['Industry']
                sub_industry_name = row_info['Sub Industry']
                plot_df = df[(df['Industry'] == industry_name) & (df['Sub Industry'] == sub_industry_name)].copy()
            else:
                industry_name = row_info['Industry']
                sub_industry_name = None
                plot_df = df[df['Industry'] == industry_name].copy()

            if selected_category != "All":
                plot_df = plot_df[plot_df['Category'] == selected_category]

            plot_df = plot_df[
                (plot_df['date'] >= pd.Timestamp(start_date)) & (plot_df['date'] <= pd.Timestamp(end_date))
            ]

            if plot_df.empty:
                continue

            sector_name = plot_df['Mapped Sector'].iloc[0] if not plot_df['Mapped Sector'].isnull().all() else "Unknown"

            if agg_method == 'sum':
                agg_df = plot_df.groupby('date').agg({
                    'open': 'sum', 'high': 'sum', 'low': 'sum', 'close': 'sum', 'volume': 'sum'
                }).reset_index().sort_values('date')
            elif agg_method == 'mean':
                agg_df = plot_df.groupby('date').agg({
                    'open': 'mean', 'high': 'mean', 'low': 'mean', 'close': 'mean', 'volume': 'sum'
                }).reset_index().sort_values('date')
            else:
                weighted_sum = plot_df.groupby('date').apply(lambda g: pd.Series({
                    'open': (g['open'] * g['volume']).sum() / g['volume'].sum(),
                    'high': (g['high'] * g['volume']).sum() / g['volume'].sum(),
                    'low': (g['low'] * g['volume']).sum() / g['volume'].sum(),
                    'close': (g['close'] * g['volume']).sum() / g['volume'].sum(),
                    'volume': g['volume'].sum()
                })).reset_index().sort_values('date')
                agg_df = weighted_sum

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

            title_text = (
                f"<b>Industry:</b> {industry_name}<br>"
                f"<b>Sector:</b> {sector_name}"
            ) if aggregation_level == "Industry" else (
                f"<b>Sub Industry:</b> {sub_industry_name}<br>"
                f"<b>Industry:</b> {industry_name}<br>"
                f"<b>Sector:</b> {sector_name}"
            )

            fig.update_layout(
                xaxis=dict(range=[zoom_start, zoom_end], autorange=False),
                title={
                    'text': title_text,
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title="Date",
                yaxis_title="Price",
                yaxis2=dict(overlaying='y', side='right', title='Volume', showgrid=False),
                height=450,
                margin=dict(t=100, b=20),
                plot_bgcolor="#111",
                paper_bgcolor="#111",
                font=dict(color='white'),
                xaxis_rangeslider_visible=False,
                showlegend=False,
            )

            row.append(fig)
            col_count += 1

            if col_count == cols_per_row:
                cols = st.columns(cols_per_row)
                for i, col in enumerate(cols):
                    col.plotly_chart(row[i], use_container_width=True)
                row = []
                col_count = 0

        if row:
            cols = st.columns(len(row))
            for i, col in enumerate(cols):
                col.plotly_chart(row[i], use_container_width=True)



            # ---------------- Linked Chart & Table Viewer Section ----------------
        st.markdown("---")
        st.subheader("📄 View Filtered VCP Stock Data")

        # Shared filters for Industry, Sub-Industry, Category
        table_df = vcp.copy()


        # Filter level selection
        # table_level = st.radio("🔎 Filter Table By", ["Industry", "Sub Industry"], horizontal=True)

        # if table_level == "Industry":
        industry_options = ["All"] + sorted(table_df['Industry'].dropna().unique())
        # selected_table_industry = st.selectbox("Select Industry", industry_options)
        if selected_industry != "All":
            table_df = table_df[table_df['Industry'] == selected_industry]
    # else:
        sub_industry_options = ["All"] + sorted(table_df['Sub Industry'].dropna().unique())
        if selected_sub_industry != "All":
            table_df = table_df[table_df['Sub Industry'] == selected_sub_industry]

        # Category filter
        category_options = ["All"] + sorted(table_df['category'].dropna().unique())
        selected_category_filter = st.selectbox("Select VCP Type (category column)", category_options)
        if selected_category_filter != "All":
            table_df = table_df[table_df["category"] == selected_category_filter]

        # Final output
        st.dataframe(
            table_df.reset_index(drop=True),
            use_container_width=True
        )
