'''
Run this script to fetch historical data from Alpaca and store it in a CSV file
'''

from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import pickle
import os

import psycopg2

import alpaca_trade_api as tradeapi

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed

from alpaca.data.historical.crypto import CryptoHistoricalDataClient, CryptoFeed
from alpaca.data.requests import CryptoBarsRequest

###################### SETUP ######################
DB_STORE_DATA = True
DB_INITIALIZE_FROM_CSV = True

EXCHANGE_SKIP = ['OTC']  # All OTC data can't be fetched anyway, so just skip it

###################### ALPACA RELATED FUNCTIONS ######################
def loadTradableAssets(API_KEY, API_SECRET, pickle_dir='tradable_assets.pkl'):
    # Define path for the pickle file
    os.makedirs(os.path.join(DIR_DATA, DIR_SUB_DATA), exist_ok=True)
    pickle_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, pickle_dir)

    try:
        with open(pickle_file_path, 'rb') as f:
            tradable_assets = pickle.load(f)
    except FileNotFoundError:
        # Initialize API
        trade_api = tradeapi.REST(API_KEY, API_SECRET)

        # If the file does not exist, fetch the assets and save them to a file
        assets = trade_api.list_assets()

        # Filter for assets that are tradable
        tradable_assets = [asset for asset in assets if asset.tradable]

        # Convert assets to dictionaries [to avoid pickle hit recursion limit]
        tradable_assets = [asset._raw for asset in tradable_assets]

        # Save tradable_assets to a file
        with open(pickle_file_path, 'wb') as f:
            pickle.dump(tradable_assets, f)
    
    grouped_assets = {}
    for asset in tradable_assets:
        symbol = asset['symbol']
        exchange = asset['exchange']
        
        # Format in our dictionary
        if exchange not in grouped_assets:
            grouped_assets[exchange] = []
        grouped_assets[exchange].append(symbol)

    return grouped_assets, tradable_assets

###################### DATABASE RELATED FUNCTIONS ######################
def initializeDBTable():
    # Connect to your postgres DB
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    cur.execute(f"SELECT EXISTS(SELECT FROM pg_tables WHERE tablename = '{DB_TABLE_NAME}')")
    exists = cur.fetchone()[0]

    # Execute a command: this creates a new table
    if not exists:
        print(f"No table found in Database, creating table {DB_TABLE_NAME}")
        cur.execute(f"""
            CREATE TABLE {DB_TABLE_NAME} (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                trade_count NUMERIC,
                vwap NUMERIC,
                UNIQUE(exchange, symbol, timestamp)
            );
        """)

        # Make the changes to the database persistent
        conn.commit()

    return cur, conn

def slotDBData(df, cur):
    # Adding the stock bars to database
    data = [(df.at[i, 'exchange'], df.at[i, 'symbol'], df.at[i, 'timestamp'], 
             df.at[i, 'open'], df.at[i, 'high'], df.at[i, 'low'], df.at[i, 'close'], 
             df.at[i, 'volume'], df.at[i, 'trade_count'], df.at[i, 'vwap'])
            for i in df.index]
    query = f"""
        INSERT INTO {DB_TABLE_NAME} (exchange, symbol, timestamp, open, high, low, close, volume, trade_count, vwap)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (exchange, symbol, timestamp) DO NOTHING
    """
    cur.executemany(query, data)

    # Attempt to commit data
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()  # If error, rollback to BEGIN
        raise ValueError(f"An error occurred: {e}")
    

###################### CSV RELATED FUNCTIONS ######################
def csvGetLatestDate(csv_file_path, csv_date_format='%Y-%m-%d %H:%M:%S%z'):
    # Check if the CSV file exists
    if os.path.exists(csv_file_path):
        # Load the existing data from CSV
        existing_data = pd.read_csv(csv_file_path)
        
        # Determine the latest date in the existing data
        latest_date_str = existing_data['timestamp'].max()
        latest_date = datetime.strptime(latest_date_str, csv_date_format)
        
        return latest_date
    else:
        return None

def csvGetLastWeekday():
    today = datetime.utcnow().date()
    if today.weekday() >= 5:  # If today is Saturday or Sunday
        last_weekday = today - timedelta(days=today.weekday() - 4)
    else:
        last_weekday = today

    last_weekday_datetime = datetime.combine(last_weekday, datetime.min.time())
    return last_weekday_datetime

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
    grouped_assets, tradable_assets = loadTradableAssets(API_KEY, API_SECRET)

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
        if exchange in EXCHANGE_SKIP: continue

        # Generate the require directory and file name
        csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, exchange, f"{symbol}.csv")
        os.makedirs(os.path.join(DIR_DATA, DIR_SUB_DATA, exchange), exist_ok=True)

        # This is for a loading of CSV data into database
        if DB_INITIALIZE_FROM_CSV:
            if idx <= 3642: continue  # For a fast skip if I'm continuing from somewhere
            if not os.file.exists(csv_file_path): continue  # Skip if CSV doesn't exist
            df = pd.read_csv(csv_file_path)
            df["exchange"] = [exchange] * len(df)
            print(f"{idx} CSV: Adding {symbol} into database")
            slotDBData(df, cur)
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
