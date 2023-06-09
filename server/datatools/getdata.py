import os
import pickle
import pandas as pd
from datatools.constant import *

###################### ALPACA RELATED FUNCTIONS ######################
def alpacaLoadTradableAssets(API_KEY, API_SECRET, skipCSVData=None, groupAssets=False, pickle_dir='tradable_assets.pkl', EXCHANGE_SKIP = ['OTC']):
    import alpaca_trade_api as tradeapi

    global DIR_DATA, DIR_SUB_DATA

    # Define path for the pickle file
    os.makedirs(os.path.join(DIR_DATA, DIR_SUB_DATA), exist_ok=True)
    pickle_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, pickle_dir)

    try:
        with open(pickle_file_path, 'rb') as f:
            tradable_assets = pickle.load(f)
        
        # Used this to do a check and noticed alpaca tradable asset is not updated
        if skipCSVData:
            for asset in tradable_assets:
                for skipping in skipCSVData:
                    if asset['symbol'] == skipping[1] and asset['exchange'] == skipping[0]:
                        print(f"Removing {skipping[0]}, {skipping[1]} from tradable assets")
                        tradable_assets.remove(asset)
                        break
                
    except FileNotFoundError:
        # Initialize API
        trade_api = tradeapi.REST(API_KEY, API_SECRET)

        # If the file does not exist, fetch the assets and save them to a file
        assets = trade_api.list_assets()

        # Filter for assets that are tradable
        tradable_assets = [asset for asset in assets if asset.tradable]

        # Convert assets to dictionaries [to avoid pickle hit recursion limit]
        tradable_assets = [asset._raw for asset in tradable_assets]

        # Used this to do a check and noticed alpaca tradable asset is not updated
        if skipCSVData:
            for asset in tradable_assets:
                for skipping in skipCSVData:
                    if asset['symbol'] == skipping[1] and asset['exchange'] == skipping[0]:
                        print(f"Removing {skipping[0]}, {skipping[1]} from tradable assets")
                        tradable_assets.remove(asset)
                        break

        # Save tradable_assets to a file
        with open(pickle_file_path, 'wb') as f:
            pickle.dump(tradable_assets, f)
    
    if groupAssets:
        # This is the earlier version of return where I grouped things based on exchange
        grouped_assets = {}
        for asset in tradable_assets:
            symbol = asset['symbol']
            exchange = asset['exchange']
            
            # Format in our dictionary
            if exchange not in grouped_assets:
                grouped_assets[exchange] = []
            grouped_assets[exchange].append(symbol)

        return grouped_assets, tradable_assets
    else:
        # If exchange is in EXCHANGE_SKIP, skip it
        tradable_assets = [asset for asset in tradable_assets if asset['exchange'] not in EXCHANGE_SKIP]

        # Get the exchanges and symbols
        exchanges = [asset['exchange'] for asset in tradable_assets]
        symbols = [asset['symbol'] for asset in tradable_assets]

        return exchanges, symbols

def alpacaGetMaxRequestAssets(stock_client, crypto_client, exchangesymbols, startdate, enddate, max_assets=1000, exchange='NYSE'):
    from alpaca.data.requests import CryptoBarsRequest
    from alpaca.data.historical.crypto import CryptoFeed
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from alpaca.data.enums import Adjustment, DataFeed

    # Repeatedly fetch the maximum number of assets
    exchanges, symbols = zip(*exchangesymbols)
    symbols = list(symbols)
    current_symbols = symbols[0:max_assets]
    timeframe = TimeFrame(1, TimeFrameUnit.Day)  # Define a daily timeframe

    # Get the maximum number of assets we can request at once
    while True:
        try:
            if exchanges[0].lower() == "crypto":
                request = CryptoBarsRequest(
                    symbol_or_symbols=current_symbols,
                    start=startdate,
                    end=enddate,
                    timeframe=timeframe,
                    limit=None,
                )
                bars = crypto_client.get_crypto_bars(request_params=request, feed=CryptoFeed.US)
            else:
                request = StockBarsRequest(
                    symbol_or_symbols=current_symbols,
                    start=startdate,
                    end=enddate,
                    timeframe=timeframe,
                    limit=None,
                    adjustment=Adjustment.ALL,
                    feed=DataFeed.SIP
                )
                bars = stock_client.get_stock_bars(request)

            # If went through without error, we break with this max_assets amount
            print(f"Successfully fetched {max_assets} assets")
            return max_assets
        except Exception as e:
            # If we get an error, we reduce the number of assets by half
            max_assets = max_assets // 2
            current_symbols = symbols[0:max_assets]
            print(f"Error: {e}\nReducing max_assets to {max_assets}")

###################### DATABASE FUNCTIONS ######################
def dbGetUniqueLatestDates(cur, exchanges, symbols, print_dates=False):
    global DB_MAIN_TABLE

    newsymbols = []
    stock_latest_dates = {}
    crypto_latest_dates = {}  #  Split them out because the dates are different
    for idx, symbol in enumerate(symbols):
        exchange = exchanges[idx]
        cur.execute(f"""
            SELECT MAX(timestamp) 
            FROM {DB_MAIN_TABLE} 
            WHERE exchange = %s AND symbol = %s
        """, (exchange, symbol))
        
        # Fetch latest date for each symbol
        result = cur.fetchone()
        latest_date = result[0]
        if latest_date is None:
            # Slot into brand symbols to create
            newsymbols.append((exchange, symbol))
        else:
            if exchange.lower() == 'crypto':
                # Slot into the grouping of latest dates
                if latest_date not in crypto_latest_dates:
                    crypto_latest_dates[latest_date] = [(exchange, symbol)]
                else:
                    crypto_latest_dates[latest_date].append((exchange, symbol))
            else:
                # Slot into the grouping of latest dates
                if latest_date not in stock_latest_dates:
                    stock_latest_dates[latest_date] = [(exchange, symbol)]
                else:
                    stock_latest_dates[latest_date].append((exchange, symbol))

    if print_dates:
        for date, symbols in crypto_latest_dates.items():
            print(f"Crypto Date: {date}, Symbols: {symbols}")
        for date, symbols in stock_latest_dates.items():
            print(f"Stock Date: {date}, Symbols: {symbols}")
        print(f"New exchange/symbols: {newsymbols}")

    return newsymbols, stock_latest_dates, crypto_latest_dates

##### DB GET FUNCTIONS #####
def dbGetColumns(cur, conn, table):
    cur.execute(f"SELECT * FROM {table} LIMIT 0")
    return [desc[0] for desc in cur.description]

def dbGetUniqueData(cur, conn, table, command="exchange, symbol"):
    sql_query = f"SELECT DISTINCT {command} FROM {table}"
    cur.execute(sql_query)
    unique_pairs = cur.fetchall()

    return unique_pairs

def dbGetDataWhere(cur, conn, table, where, datafetch="*"):
    sql_query = f"SELECT {datafetch} FROM {table} WHERE {where}"
    df = pd.read_sql_query(sql_query, conn)
    return df

###################### GENERAL FUNCTIONS ######################
def listChunks(lst, split_amount):
    """
    Yield successive n-sized chunks from lst.

    Example usage:
    symbol_chunks = list(listChunks(symbols, 20))
    """
    return [lst[i:i + split_amount] for i in range(0, len(lst), split_amount)]
