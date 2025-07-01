
import sys
import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from datetime import datetime

# ✅ Add absolute path of your project directory to sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
from driver_service.vcp import fetch_data
from driver_service.angel_api_fetch import AngelOneDataFetcher
from driver_service.constant import *
from driver_service.driver_manager import DriveManager
from driver_service.auth import create_service
from driver_service.bhavcopy_data import BhavcopyDownloader


CLIENT_SECRET_FILE = os.path.join(project_root, "config", "client_secret.json")
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

drive = create_service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
manager = DriveManager(drive)

folder_id = manager.get_or_create_folder('bhavcopy_stock_data')

prev_data=manager.fetch_csv_by_name_as_dataframe('complete_data1.csv',folder_id)
# Index(['NSE_BSE_code', 'open', 'close', 'low', 'high', 'volume', 'datetime',
#        'market', 'Name', 'BSE Code', 'NSE Code', 'Industry', 'Current Price',
#        'Market Capitalization', 'Mapped Sector', 'Category'],
#       dtype='object')
# df[(df['Industry']=="Petroleum Products") | (df['Industry']=="Agricultural Food & other Products")].to_csv(r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\test2.csv",index=False)
prev_data[['date', 'time']] = prev_data['datetime'].str.split('T', expand=True)

prev_data = prev_data[prev_data["Industry"] != "BhaPra"]
#  ✅ Create temporary datetime series for sorting ONLY
temp_dates = pd.to_datetime(prev_data['date'], format="%Y-%m-%d")

# ✅ Group by original 'date' (string), use temp_dates for sorting
date_counts = prev_data.groupby('date')['NSE_BSE_code'].nunique()
date_counts = date_counts.loc[pd.Series(prev_data['date'].unique(), index=prev_data['date'].unique()).sort_values(ascending=False, key=lambda x: pd.to_datetime(x, format='%Y-%m-%d')).index]

# ✅ Find last date with stock count >= 4000
last_date = ""
for date, count in date_counts.items():
    if count >= 4000:
        last_date = date  # already in string format: 'YYYY-MM-DD'
        last_date = pd.to_datetime(last_date, format='%Y-%m-%d').strftime('%d-%m-%Y')

        break
else:
    print("❗ No date found with stock count >= 4000")

print("Last Date:", last_date)
# Convert 'date' column to datetime
prev_data['date'] = pd.to_datetime(prev_data['datetime'].str.split('T').str[0], format="%Y-%m-%d")

# Set how many days to keep
days_to_keep = 180 # You can change this as needed

# Find max date and cutoff date
max_date = prev_data['date'].max()
cutoff_date = max_date - pd.Timedelta(days=days_to_keep)

# Filter out data older than cutoff
prev_data = prev_data[prev_data['date'] >= cutoff_date]
print(prev_data['date'].min(), prev_data['date'].max())

prev_data.drop(columns=['date', 'time'],inplace=True)

if last_date != datetime.today().strftime('%d-%m-%Y'):
    downloader = BhavcopyDownloader(
        download_dir=r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\Bhav_copy_data",
        all_stock_path=r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\all-stocks (2).csv",
        mapping_sheet_path=r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\map_indus_sector.xlsx"
    )

    # last_date = "01-06-2025"
    url = "https://www.samco.in/bhavcopy-nse-bse-mcx"
    dfk = pd.read_csv(r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\sub_industry_mapping.csv")

    downloader.download_bhavcopy(url, last_date)
    df_bhavcopy = downloader.extract_bhavcopy(last_date)
    df_final_output = downloader.process_all_stock()
    today_data = downloader.merge_bhavcopy_with_mapping(df_bhavcopy, df_final_output)
    print("today_data",today_data.columns)
    print("previous_data",prev_data.columns)
    prev_data = prev_data[['NSE_BSE_code', 'open', 'close', 'low', 'high', 'volume', 'datetime',
       'market', 'Name', 'BSE Code', 'NSE Code', 'Industry', 'Current Price',
       'Market Capitalization', 'Mapped Sector', 'Category']]
    final_data=pd.concat([prev_data,today_data])

    final_data=pd.merge(final_data, dfk[['NSE_BSE_code', 'consumer_discretionary','Sub Industry']], on='NSE_BSE_code', how='left')
    final_data['Sub Industry'] = final_data['Sub Industry'].fillna('BhaPra')
    final_data.drop_duplicates(subset=["NSE_BSE_code",'datetime'],keep='last',inplace=True)
    final_data.to_csv(r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\final_data.csv",index=False)

    manager.upload_dataframe_in_memory(final_data,'complete_data1.csv', folder_id)
    print("NSE BSE data uploaded successfully")

    ######################################### vcp data ######################################################
    # Call the function and upload the data
    vcp = fetch_data()

    folder_id_vcp = manager.get_or_create_folder('vcp_folder')

    manager.upload_dataframe_in_memory(vcp,f'{datetime.today().strftime("%d%m%Y")}.csv',folder_id_vcp)
    print("Vcp uploaded successfully")

    # Do anything with the returned dataframe













