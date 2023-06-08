'''
Run this script to modify, remove CSV beyond certain rows
'''

from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
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

if __name__ == "__main__":
    ############################## LOAD VARIABLES ##############################
    load_dotenv()
    DIR_DATA = str(os.getenv('DIR_DATA'))
    DIR_SUB_DATA = str(os.getenv('DIR_SUB_DATA'))

    TRIMOFF_DATE = datetime(2023, 5, 26, tzinfo=timezone.utc)

    tupleSkipCSV = csvGetSkipSymbols()
    exchanges, all_symbols = alpacaLoadTradableAssets(API_KEY, API_SECRET, skipCSVData=tupleSkipCSV, groupAssets=False)

    ############################## CRYPTO: CLEAR THE LATEST FILE ##############################
    for idx, exchange in enumerate(exchanges):
        if exchange != 'CRYPTO':
            continue
        symbol = all_symbols[idx]
        
        # Remove unnecessary lines off the csv
        csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, exchange, f"{symbol.replace('/', '-')}.csv")
        print(f"Removing items past date {TRIMOFF_DATE} for {symbol}...")
        csvRemovePastDate(csv_file_path, TRIMOFF_DATE)
        