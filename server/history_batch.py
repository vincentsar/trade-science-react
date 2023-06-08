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
    # Ensure that the day data is complete first before fetching
    END_DATE = datetime.utcnow() - timedelta(days=1)
    START_DATE = END_DATE - timedelta(days=365 * 7)  # Max allowed duration = 7 years

    ############################## ALPACA INSTANCE SETUPS ##############################
    # Some alpaca class objects
    timeframe = TimeFrame(1, TimeFrameUnit.Day)  # Define a daily timeframe
    stock_client = StockHistoricalDataClient(API_KEY, API_SECRET)
    crypto_client = CryptoHistoricalDataClient(API_KEY, API_SECRET)
    
    # Initiate database connection - if we using database
    cur, conn = dbInitializeTable()
    cur.execute("BEGIN")  # Set a rollback point

    # Get tradable assets based on relevant groupings and store them
    tupleSkipCSV = csvGetSkipSymbols()
    exchanges, all_symbols = alpacaLoadTradableAssets(API_KEY, API_SECRET, skipCSVData=tupleSkipCSV, groupAssets=False)
    new_exchangesymbols, stock_latestdates_exchangesymbols, crypto_latestdates_exchangesymbols = dbGetUniqueLatestDates(cur, exchanges, all_symbols)

    ############################## STOCK & CRYPTO: LOOP AND STORE NEW DATA ##############################
    for idx, exchangesymbols in enumerate(new_exchangesymbols):
        exchange, symbol = exchangesymbols
        
        # Fetch the ohlc data
        try:
            if exchange.lower() == "crypto":
                request = CryptoBarsRequest(
                    symbol_or_symbols=symbol,
                    start=START_DATE,
                    end=END_DATE,
                    timeframe=timeframe,
                    limit=None,
                )
                bars = crypto_client.get_crypto_bars(request_params=request, feed=CryptoFeed.US)
            else:
                request = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    start=START_DATE,
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

        # Slot into DB
        df = bars.df.copy()
        df = df.reset_index()
        df["exchange"] = [exchange] * len(df)
        dbSlotData(df, cur, conn)

        # Save to CSV
        csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, exchange, f"{symbol.replace('/', '-')}.csv")
        os.makedirs(os.path.join(DIR_DATA, DIR_SUB_DATA, exchange), exist_ok=True)
        bars.df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path))

        pass

    ############################## STOCK: LOOP AND STORE EXISTED DATA ##############################
    for curstart_date in stock_latestdates_exchangesymbols:
        if datetime.utcnow().weekday() >= 5 and curstart_date == LAST_TRADING_DATE.date():
            print(f"Weekend: Skipping {curstart_date} as match latest trading date")
            continue
        elif curstart_date.date() == datetime.utcnow().date() - timedelta(days=1):
            print(f"Skipping {curstart_date} as match latest trading date")
            continue

        # Get the max assets we can request for this date grouping
        cur_dateexchangesymbols = stock_latestdates_exchangesymbols[curstart_date]
        max_assets = alpacaGetMaxRequestAssets(stock_client, crypto_client, cur_dateexchangesymbols, curstart_date, END_DATE)

        # Break data down into relevant chunk to load
        chunked_dateexchangesymbols = listChunks(cur_dateexchangesymbols, max_assets)
        for exchangesymbols in chunked_dateexchangesymbols:
            exchanges, symbols = zip(*exchangesymbols)
            symbols = list(symbols)
            dict_exchanges = dict(zip(symbols, exchanges))

            # Fetch the ohlc data
            request = StockBarsRequest(
                symbol_or_symbols=symbols,
                start=curstart_date + timedelta(days=1),
                end=END_DATE,
                timeframe=timeframe,
                limit=None,
                adjustment=Adjustment.ALL,
                feed=DataFeed.SIP
            )
            bars = stock_client.get_stock_bars(request)

            # If fetched nothing - it means it's no longer a valid symbol
            if len(bars.data) == 0:
                csvAddSkipSymbols(exchangesymbols)
                raise ValueError(f"{curstart_date}: {exchangesymbols} fetched nothing.\nAdded to skip symbol. Please validate.")

            # Slot into DB
            df = dfProcessDBStorage(bars.df.copy(), dict_exchanges)
            dbSlotData(df, cur, conn)

            # Store into relevant CSVs
            dfGroupStoreCSVs(df)

    ############################## CRYPTO: LOOP AND STORE EXISTED DATA ##############################
    for curstart_date in crypto_latestdates_exchangesymbols:
        if curstart_date.date() == datetime.utcnow().date() - timedelta(days=1):
            print(f"Skipping {curstart_date} as match latest trading date")
            continue

        # Get the max assets we can request for this date grouping
        cur_dateexchangesymbols = crypto_latestdates_exchangesymbols[curstart_date]
        max_assets = alpacaGetMaxRequestAssets(stock_client, crypto_client, cur_dateexchangesymbols, curstart_date, END_DATE)

        # Break data down into relevant chunk to load
        chunked_dateexchangesymbols = listChunks(cur_dateexchangesymbols, max_assets)
        for exchangesymbols in chunked_dateexchangesymbols:
            exchanges, symbols = zip(*exchangesymbols)
            symbols = list(symbols)
            dict_exchanges = dict(zip(symbols, exchanges))

            # Fetch the ohlc data
            request = CryptoBarsRequest(
                symbol_or_symbols=symbols,
                start=curstart_date + timedelta(days=1),
                end=END_DATE,
                timeframe=timeframe,
                limit=None,
            )
            bars = crypto_client.get_crypto_bars(request_params=request, feed=CryptoFeed.US)

            # If fetched nothing - it means it's no longer a valid symbol
            if len(bars.data) == 0:
                csvAddSkipSymbols(exchangesymbols)
                raise ValueError(f"{curstart_date}: {exchangesymbols} fetched nothing.\nAdded to skip symbol. Please validate.")

            # Slot into DB
            df = dfProcessDBStorage(bars.df.copy(), dict_exchanges)
            dbSlotData(df, cur, conn)

            # Store into relevant CSVs
            dfGroupStoreCSVs(df)

    ############################## WRAP UP: CLOSE DB CONNECTIONS ##############################
    cur.close()
    conn.close()
