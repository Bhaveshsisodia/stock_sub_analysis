
import requests
from bs4 import BeautifulSoup
import random
import urllib3
import json
# from pandas.io.json import json_normalize
import datetime
import random
import pandas as pd
import re
from ast import literal_eval
import numpy as np
import pymongo
from pymongo import MongoClient
import time
from datetime import date, timedelta
import urllib.request
import warnings
warnings.filterwarnings('ignore')

import pymysql
pymysql.install_as_MySQLdb()
import pymysql
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

import numpy as np
import seaborn as sns
import os
import re
# import pyautogui
import time
from itertools import cycle
import datetime
from datetime import timedelta,date
# import libraries
import pandas as pd
import urllib.request
import numpy as np
import matplotlib.pyplot as plt
from selenium import webdriver
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import mysql.connector
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from sqlalchemy import create_engine
import pyperclip
from datetime import date

today = date.today().strftime("%Y%m%d")





def vcp_data(url):

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CHROMEDRIVER_PATH = os.path.join(BASE_DIR, 'drivers', 'chromedriver.exe')

    # Debugging: print path to confirm
    print(f"Using chromedriver at: {CHROMEDRIVER_PATH}")
    chrome_options = Options()

    # chrome_options.add_argument("--headless")  # Optional: remove if you want GUI
    # chrome_options.add_argument("--window-size=1920,1080")
    # chrome_options.add_argument("--no-sandbox")
    # chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(executable_path=CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    copy=WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f"//*[@id='DataTables_Table_0_wrapper']/div[1]/div/button[2]"))).click()


    copied_data = pyperclip.paste()
    # print("Copied Data:", copied_data)
    data = copied_data.replace(",", "")
    import io
    # Load data into a pandas DataFrame
    df = pd.read_csv(io.StringIO(data), sep="\t")
    df
    df=df.reset_index()
    print(df)
    df=df.drop(columns=['level_0'])
    df.columns=df.iloc[0].to_list()
    df=df.iloc[1:,:]
    return df

def fetch_data():

    # https://chartink.com/screener/rb-stockexploder
    data1=vcp_data("https://chartink.com/screener/volatility-compression")
    data2=vcp_data("https://chartink.com/screener/mark-minervini-vcp-pattern")
    data3 = vcp_data("https://chartink.com/screener/stockexploder-vcp-2")
    datarb= vcp_data("https://chartink.com/screener/rb-stockexploder")
    data3['category'] = 'stockexploder_vcp'
    data2['category'] = 'mark-minervini-vcp-pattern'

    data1['category'] = 'volatility-compression'
    datarb['category'] = 'rocket_based'

    data=pd.concat([data1,data2,data3,datarb])

    df_sector = pd.read_csv(r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\all-stocks (2).csv")

    df_final=pd.merge(data,df_sector[['NSE Code','Industry']],left_on=['Symbol'],right_on=['NSE Code'], how='left')

    df_final['% Chg']=df_final['% Chg'].str.split("%",expand=True)[0]
    df_final['% Chg']=df_final['% Chg'].astype(float)


    df_sub_industry = pd.read_csv(r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\sub_industry_mapping.csv")
    indus_sec_map =pd.read_excel(rf"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\map_indus_sector.xlsx")
    df_map1=df_final.groupby(['category','Industry','Symbol','Stock Name','Price']).count().reset_index()[['category','Industry','Symbol','Stock Name','Price']]
    df_final_output =pd.merge(df_map1,indus_sec_map, on=['Industry'], how='left')



    df_cap=pd.read_csv(r"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\Data\marketcap.csv")

    df_final_output=pd.merge(df_final_output,df_cap[['NSE Code','Category']], right_on=['NSE Code'],left_on=['Symbol'], how='left')
    df_final_output
    df_sub_industry.rename(columns={'NSE_BSE_code':'Symbol'}, inplace=True)

    df_final_output=pd.merge(df_final_output,df_sub_industry[['Symbol','Sub Industry']], left_on=['Symbol'], right_on=['Symbol'], how='left')
    print(df_final_output.columns)
    df_final_output.to_csv(rf"D:\web Development using python\PROJECTS\Stocks\Fundamental Analysis\swing_trade\final_output\output_{today}.csv",index=False)
    rb=df_final_output[df_final_output['category']=='rocket_based']
    for i in rb['Industry'].unique():
        print(f"{i} :",rb[rb['Sub Industry']==i].shape[0])

    return df_final_output

if __name__ == "__main__":
    df_final_output = fetch_data()
    print(df_final_output.head())
    print("Data fetched and saved successfully.")
    # You can add more processing or saving logic here if needed.




