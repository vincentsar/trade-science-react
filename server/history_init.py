'''
Run this script to fetch historical data from Alpaca and store it in a CSV file
- This is used for initialization where data amount is so huge we can only loop it once at a time.
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
# Store into DB when fetch from internet
DB_STORE_DATA = True

# Take CSV to DB [If ran, it doesn't fetch from internet to CSV]
DB_INITIALIZE_FROM_CSV = True
if DB_INITIALIZE_FROM_CSV: DB_STORE_DATA = True

if __name__ == "__main__":
    ############################## LOAD VARIABLES ##############################
    load_dotenv()
    DIR_DATA = str(os.getenv('DIR_DATA'))
    DIR_SUB_DATA = str(os.getenv('DIR_SUB_DATA'))
    
    API_KEY = str(os.getenv('API_KEY'))
    API_SECRET = str(os.getenv('API_SECRET'))
    BASE_URL = 'https://paper-api.alpaca.markets'  # Use this URL for paper trading

    if DB_STORE_DATA:
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
    # Get tradable assets and store them
    grouped_assets, tradable_assets = alpacaLoadTradableAssets(API_KEY, API_SECRET, True)

    # Some alpaca class objects
    timeframe = TimeFrame(1, TimeFrameUnit.Day)  # Define a daily timeframe
    stock_client = StockHistoricalDataClient(API_KEY, API_SECRET)
    crypto_client = CryptoHistoricalDataClient(API_KEY, API_SECRET)
    
    # Initiate database connection - if we using database
    if DB_STORE_DATA: 
        cur, conn = initializeDBTable()
        cur.execute("BEGIN")  # Set a rollback point

    ############################## LOOP AND STORE DATA ##############################
    for idx, asset in enumerate(tradable_assets):
        # Get core variables
        symbol = asset['symbol']
        exchange = asset['exchange']

        # Generate the require directory and file name
        csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, exchange, f"{symbol.replace('/', '-')}.csv")
        os.makedirs(os.path.join(DIR_DATA, DIR_SUB_DATA, exchange), exist_ok=True)

        # This is for a loading of CSV data into database
        if DB_INITIALIZE_FROM_CSV:
            if idx < 741: continue  # For a fast skip if I'm continuing from somewhere
            if not os.path.exists(csv_file_path): continue  # Skip if CSV doesn't exist
            df = pd.read_csv(csv_file_path)
            df["exchange"] = [exchange] * len(df)
            print(f"{idx} CSV: Adding {symbol} into database")
            slotDBData(df, cur, conn)
            continue  # Initialize from CSV = won't load from internet

        # Get the latest date to load from
        csv_date = csvGetLatestDate(csv_file_path)
        if csv_date: curstart_date = csv_date + timedelta(days=1)
        else: curstart_date = START_DATE

        # If it's the same as the last trading date [i.e. script is updated, skip]
        if csv_date and csv_date.date() == LAST_TRADING_DATE.date():
            print(f"Skipping {symbol} as it's already up to date")
            continue
        
        # Fetch the ohlc data
        try:
            if exchange.lower() == "crypto":
                request = CryptoBarsRequest(
                    symbol_or_symbols=symbol,
                    start=curstart_date,
                    end=END_DATE,
                    timeframe=timeframe,
                    limit=None,
                )
                bars = crypto_client.get_crypto_bars(request_params=request, feed=CryptoFeed.US)
            else:
                request = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    start=curstart_date,
                    end=END_DATE,
                    timeframe=timeframe,
                    limit=None,
                    adjustment=Adjustment.ALL,
                    feed=DataFeed.SIP
                )
                bars = stock_client.get_stock_bars(request)
            print(f"{idx} {exchange} - {symbol} fetched")
        except Exception as e:
            print(f"{idx} {exchange} - {symbol} error: {e}")
            continue

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
