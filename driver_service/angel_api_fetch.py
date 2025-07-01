import os
import re
import time
import numpy as np
import pandas as pd
import requests
import pyotp
from datetime import datetime
from tqdm import tqdm
from logzero import logger
from SmartApi import SmartConnect  # or from smartapi.smartConnect import SmartConnect


class AngelOneDataFetcher:
    def __init__(self, api_key, username, pin, totpkey, map_file, stock_file):
        self.api_key = api_key
        self.username = username
        self.pin = pin
        self.totpkey = totpkey
        self.session = None
        self.refresh_token = None
        self.client = None

        self.mapping_sheet_path = map_file
        self.stock_file_path = stock_file

        self.tokendf = None
        self.df_final_output = None

        self.authenticate()
        self.load_token_master()
        self.prepare_stock_data()

    def authenticate(self):
        self.client = SmartConnect(self.api_key)
        data = self.client.generateSession(self.username, self.pin, pyotp.TOTP(self.totpkey).now())
        self.refresh_token = data['data']['refreshToken']
        self.client.getProfile(self.refresh_token)

    def load_token_master(self):
        url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        d = requests.get(url).json()
        self.tokendf = pd.DataFrame.from_dict(d)

    def prepare_stock_data(self):
        all_stock = pd.read_csv(self.stock_file_path)
        all_stock = all_stock[['Name', 'BSE Code', 'NSE Code', 'Industry', 'Current Price', 'Market Capitalization']]
        mapping_sheet = pd.read_excel(self.mapping_sheet_path)

        df = pd.merge(all_stock, mapping_sheet, on=['Industry'], how='left')
        df = df.sort_values("Market Capitalization", ascending=False)
        df['Rank'] = df["Market Capitalization"].rank(ascending=False, method='first')

        def categorize(rank):
            if rank <= 100:
                return 'Large-cap'
            elif 101 <= rank <= 250:
                return 'Mid-cap'
            else:
                return 'Small-cap'

        df['Category'] = df['Rank'].apply(categorize)
        df.replace(np.nan, 'BharPra', inplace=True)
        df['NSE_BSE_code'] = np.where(df['NSE Code'] == "BharPra", df['BSE Code'], df['NSE Code'])

        self.df_final_output = df

    @staticmethod
    def is_float(s):
        return bool(re.match(r'^-?\d+(\.\d+)?$', str(s)))

    def get_token_by_value(self, value):
        if self.is_float(value):
            value = str(int(float(value)))
            filtered_df = self.tokendf[self.tokendf['token'] == value]
        else:
            filtered_df = self.tokendf[self.tokendf['name'] == value]

        if filtered_df.empty:
            return None, None

        nse_row = filtered_df[filtered_df['exch_seg'] == 'NSE']
        if not nse_row.empty:
            return nse_row.iloc[0]['token'], "NSE"
        else:
            return filtered_df.iloc[0]['token'], "BSE"

    def fetch_market_data(self, from_date=None, to_date=None, stock_df=None):
        if from_date is None:
            from_date = "2023-06-02 09:15"
        if to_date is None:
            to_date = f"{datetime.today().strftime('%Y-%m-%d')} 15:00"
        if stock_df is None:
            stock_df = self.df_final_output.copy()

        df_final = pd.DataFrame()

        for _, row in tqdm(stock_df.iterrows(), total=len(stock_df)):
            code = row['NSE_BSE_code']
            token, market = self.get_token_by_value(code)

            if token:
                historic_param = {
                    "exchange": market,
                    "symboltoken": str(token),
                    "interval": "ONE_DAY",
                    "fromdate": from_date,
                    "todate": to_date
                }

                try:
                    data1 = self.client.getCandleData(historic_param)
                    data = pd.DataFrame(data1['data'], columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
                    if not data.empty:
                        data['NSE_BSE_code'] = code
                        data['Category'] = row['Category']
                        data['Industry'] = row['Industry']
                        data['Market'] = market
                        data['Sector'] = row['Mapped Sector']
                        data['Name'] = row['Name']
                        data['Market Capitalization'] = row['Market Capitalization']
                        df_final = pd.concat([df_final, data], ignore_index=True)
                except Exception as e:
                    logger.warning(f"Historic API failed for token {token}: {e}")

        return df_final
