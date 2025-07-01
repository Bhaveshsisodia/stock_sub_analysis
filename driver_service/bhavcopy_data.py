import os
import time
import zipfile
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

warnings.filterwarnings('ignore')


class BhavcopyDownloader:
    def __init__(self, download_dir, all_stock_path, mapping_sheet_path):
        self.download_dir = download_dir
        self.all_stock_path = all_stock_path
        self.mapping_sheet_path = mapping_sheet_path
        self.driver = self._setup_driver()

    def _setup_driver(self):

          # Resolve absolute path of chromedriver relative to this file
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        CHROMEDRIVER_PATH = os.path.join(BASE_DIR, 'drivers', 'chromedriver.exe')

        # Debugging: print path to confirm
        print(f"Using chromedriver at: {CHROMEDRIVER_PATH}")

        # ✅ Create Service with correct executable path

        chrome_options = Options()
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_argument("--headless")  # Optional: remove if you want GUI
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        chrome_options.add_experimental_option("prefs", prefs)
        service = Service(executable_path=CHROMEDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver

    def download_bhavcopy(self, url, last_date):
        self.driver.get(url)
        time.sleep(2)

        from_date_input = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/section[2]/div/div/div/div[2]/div/div/form/div/div/div[2]/div/input"))
        )
        from_date_input.clear()
        from_date_input.send_keys(last_date)

        to_date_input = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/section[2]/div/div/div/div[2]/div/div/form/div/div/div[3]/div/input"))
        )
        to_date_input.clear()
        to_date_input.send_keys(datetime.today().strftime("%d-%m-%Y"))

        download_button = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/section[2]/div/div/div/div[2]/div/div/form/div/div/div[4]/div/a[2]"))
        )
        download_button.click()

        time.sleep(10)

    def extract_bhavcopy(self, last_date):
        file_name = f"{datetime.strptime(last_date, '%d-%m-%Y').strftime('%Y-%m-%d')}-{datetime.today().strftime('%Y-%m-%d')}.zip"
        zip_file_path = os.path.join(self.download_dir, file_name)

        if not os.path.exists(zip_file_path):
            raise FileNotFoundError(f"ZIP file not found at: {zip_file_path}")

        all_data = []
        with zipfile.ZipFile(zip_file_path, 'r') as z:
            for csv_filename in z.namelist():
                for exchange in ["NSE.csv", "BSE.csv"]:
                    if exchange in csv_filename:
                        with z.open(csv_filename) as f:
                            stocks_data = pd.read_csv(f)

                            if exchange == "NSE.csv":
                                stocks_data.rename(columns={
                                    "SYMBOL": "NSE_BSE_code",
                                    "OPEN": "open",
                                    "CLOSE": "close",
                                    "HIGH": "high",
                                    "LOW": "low",
                                    "TOTTRDQTY": "volume"
                                }, inplace=True)
                            else:
                                stocks_data.rename(columns={
                                    "SC_CODE": "NSE_BSE_code",
                                    "OPEN": "open",
                                    "CLOSE": "close",
                                    "HIGH": "high",
                                    "LOW": "low",
                                    "NO_OF_SHRS": "volume"
                                }, inplace=True)

                            stocks_data = stocks_data[['NSE_BSE_code', 'open', 'close', 'low', 'high', 'volume']]

                            try:
                                date_part = csv_filename[:8]
                                bhavcopy_date = datetime.strptime(date_part, "%Y%m%d").date()
                            except:
                                bhavcopy_date = None

                            stocks_data['datetime'] = pd.to_datetime(bhavcopy_date, format="%Y-%m-%d").tz_localize('Asia/Kolkata')
                            stocks_data['datetime'] = stocks_data['datetime'].apply(lambda x: x.isoformat())
                            stocks_data['market'] = exchange.split('.')[0]

                            all_data.append(stocks_data)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
        else:
            combined_df = pd.DataFrame()

        os.remove(zip_file_path)
        return combined_df

    def process_all_stock(self):
        all_stock = pd.read_csv(self.all_stock_path)
        all_stock = all_stock[['Name', 'BSE Code', 'NSE Code', 'Industry', 'Current Price', 'Market Capitalization']]
        mapping_sheet = pd.read_excel(self.mapping_sheet_path)
        df_final_output = pd.merge(all_stock, mapping_sheet, on=['Industry'], how='left')

        df_test = df_final_output.dropna(subset=['Market Capitalization', 'Industry'])

        def assign_category(group):
            group = group.sort_values('Market Capitalization', ascending=False).reset_index(drop=True)
            n = len(group)
            for i in range(n):
                if i < n / 3:
                    group.loc[i, 'Category'] = 'Large-cap'
                elif i < 2 * n / 3:
                    group.loc[i, 'Category'] = 'Mid-cap'
                else:
                    group.loc[i, 'Category'] = 'Small-cap'
            return group

        df_test = df_test.groupby('Industry', group_keys=False).apply(assign_category)
        df_test = df_test[['Name', 'Category']]
        df_final_output = pd.merge(df_final_output, df_test, on=['Name'], how="left")

        df_final_output['BSE Code'] = df_final_output['BSE Code'].fillna(0).astype(int)
        df_final_output.replace(np.nan, 'BhaPra', inplace=True)
        df_final_output['NSE_BSE_code'] = np.where(
            df_final_output['NSE Code'] == "BhaPra", df_final_output['BSE Code'], df_final_output['NSE Code']
        )
        return df_final_output

    def merge_bhavcopy_with_mapping(self, df_bhavcopy, df_final_output):
        data_required = df_final_output['NSE_BSE_code'].tolist()
        df_filtered = df_bhavcopy[df_bhavcopy['NSE_BSE_code'].isin(data_required)]
        final_merged = pd.merge(df_filtered, df_final_output, on='NSE_BSE_code', how='left')
        return final_merged

    def close(self):
        self.driver.quit()
