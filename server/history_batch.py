'''
Run this script to fetch historical data from Alpaca and store it in a CSV file
- It'll run a few symbols at once after initialization.
'''

from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd

import os

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed

from alpaca.data.historical.crypto import CryptoHistoricalDataClient, CryptoFeed
from alpaca.data.requests import CryptoBarsRequest

from datatools.fetchdata import *
from datatools.storage import *

###################### SETUP ######################
EXCHANGE_SKIP = ['OTC']  # All OTC data can't be fetched anyway, so just skip it

if __name__ == "__main__":
    ############################## LOAD VARIABLES ##############################
    load_dotenv()
    DIR_DATA = str(os.getenv('DIR_DATA'))
    DIR_SUB_DATA = str(os.getenv('DIR_SUB_DATA'))
    
    API_KEY = str(os.getenv('API_KEY'))
    API_SECRET = str(os.getenv('API_SECRET'))

    DB_USER = str(os.getenv('DB_USER'))
    DB_NAME = str(os.getenv('DB_NAME'))
    DB_PASSWORD = str(os.getenv('DB_PASSWORD'))
    DB_HOST = str(os.getenv('DB_HOST'))
    DB_PORT = str(os.getenv('DB_PORT'))
    DB_TABLE_NAME = str(os.getenv('DB_TABLE_NAME'))

    # Define the start and end dates for the historical data
    LAST_TRADING_DATE = csvGetLastWeekday()
    END_DATE = datetime.utcnow() - timedelta(minutes=15)
    START_DATE = END_DATE - timedelta(days=365 * 7)  # Max allowed duration = 7 years

    ############################## ALPACA INSTANCE SETUPS ##############################
    # Some alpaca class objects
    timeframe = TimeFrame(1, TimeFrameUnit.Day)  # Define a daily timeframe
    stock_client = StockHistoricalDataClient(API_KEY, API_SECRET)
    crypto_client = CryptoHistoricalDataClient(API_KEY, API_SECRET)
    
    # Initiate database connection - if we using database
    cur, conn = initializeDBTable()
    cur.execute("BEGIN")  # Set a rollback point

    # Get tradable assets based on relevant groupings and store them
    exchanges, all_symbols = alpacaLoadTradableAssets(API_KEY, API_SECRET)
    new_symbols, latest_dates = dbGetUniqueLatestDates(cur, exchanges, all_symbols, True)

    ############################## LOOP AND STORE DATA ##############################
    completed_symbols = []
    for curstart_date in latest_dates:
        # Get the max assets we can request for this date grouping
        curdate_symbols = latest_dates[curstart_date]
        max_assets = alpacaGetMaxRequestAssets(stock_client, curdate_symbols, curstart_date, END_DATE)

        # Get core variables
        symbol = asset['symbol']
        exchange = asset['exchange']
        if exchange in EXCHANGE_SKIP: continue
        if idx < latest_index: continue

        # Generate the require directory and file name
        csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, exchange, f"{symbol.replace('/', '-')}.csv")
        os.makedirs(os.path.join(DIR_DATA, DIR_SUB_DATA, exchange), exist_ok=True)

        # Get the latest date to load from
        csv_date = csvGetLatestDate(csv_file_path)
        if csv_date: curstart_date = csv_date + timedelta(days=1)
        else: curstart_date = START_DATE

        # If it's the same as the last trading date [i.e. script is updated, skip]
        if csv_date and csv_date.date() == LAST_TRADING_DATE.date():
            print(f"Skipping {symbol} as it's already up to date")
            continue
        
        # Fetch the ohlc data
        latest_index = idx + 5 if idx + 5 <= len(tradable_assets) - 1 else len(tradable_assets) - 1
        next_assets = tradable_assets[idx:latest_index]
        next_symbols = [asset['symbol'] for asset in next_assets]
        request = StockBarsRequest(
            symbol_or_symbols=next_symbols,
            start=curstart_date,
            end=END_DATE,
            timeframe=timeframe,
            limit=None,
            adjustment=Adjustment.ALL,
            feed=DataFeed.SIP
        )
        bars = stock_client.get_stock_bars(request)

        if DB_STORE_DATA: 
            df = bars.df
            df["exchange"] = [exchange] * len(df)
            slotDBData(df, cur)

        # Save to CSV
        bars.df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path))
        pass

    if DB_STORE_DATA or DB_INITIALIZE_FROM_CSV:
        cur.close()
        conn.close()
