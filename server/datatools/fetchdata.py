import os
import pickle
from datatools.constant import *

###################### ALPACA RELATED FUNCTIONS ######################
def alpacaLoadTradableAssets(API_KEY, API_SECRET, groupAssets=False, pickle_dir='tradable_assets.pkl', EXCHANGE_SKIP = ['OTC']):
    import alpaca_trade_api as tradeapi

    global DIR_DATA, DIR_SUB_DATA

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

def alpacaGetMaxRequestAssets(stock_client, symbols, startdate, enddate, max_assets=500, exchange='NYSE'):
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from alpaca.data.enums import Adjustment, DataFeed

    # Repeatedly fetch the maximum number of assets
    current_symbols = symbols[0, max_assets]
    timeframe = TimeFrame(1, TimeFrameUnit.Day)  # Define a daily timeframe

    # Get the maximum number of assets we can request at once
    while True:
        try:
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
            current_symbols = symbols[0, max_assets]
            print(f"Error: {e}\nReducing max_assets to {max_assets}")

###################### DATABASE FUNCTIONS ######################
def dbGetUniqueLatestDates(cur, exchanges, symbols, print_dates=False):
    global DB_TABLE_NAME

    latest_dates = {}
    newsymbols = []
    for idx, symbol in enumerate(symbols):
        exchange = exchanges[idx]
        cur.execute(f"""
            SELECT MAX(timestamp) 
            FROM {DB_TABLE_NAME} 
            WHERE exchange = ? AND symbol = ?
        """, (exchange, symbol))
        
        # Fetch latest date for each symbol
        result = cur.fetchone()
        if result is None:
            # Slot into brand symbols to create
            print(f"No data found for exchange={exchange}, symbol={symbol}")
            newsymbols.append((exchange, symbol))
            continue
        else:
            # Slot into the grouping of latest dates
            latest_date = result[0]

            if latest_date not in latest_dates:
                latest_dates[latest_date] = [(exchange, symbol)]
            else:
                latest_dates[latest_date].append((exchange, symbol))

    if print_dates:
        for date, symbols in latest_dates.items():
            print(f"Date: {date}, Symbols: {symbols}")
        print(f"New symbols: {newsymbols}")

    return newsymbols, latest_dates

###################### GENERAL FUNCTIONS ######################
def list_chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.

    Example usage:
    symbol_chunks = list(list_chunks(symbols, 20))
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
