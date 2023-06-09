from dotenv import load_dotenv
from backtest.minervini import *

from datatools.getdata import *
from datatools.storedata import *
import warnings

# Mute the warning of database should use SQLAlechemy
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# For testing, we do the operation only on X amount of symbols before batching the rest
CONST_STARTSKIP = -1
CONST_CUTOFF = 1500

###################### MAIN ######################
if __name__ == "__main__":
    # Main variables
    load_dotenv()
    DIR_DATA = str(os.getenv('DIR_DATA'))
    DIR_SUB_DATA = str(os.getenv('DIR_SUB_DATA'))

    DB_USER = str(os.getenv('DB_USER'))
    DB_NAME = str(os.getenv('DB_NAME'))
    DB_PASSWORD = str(os.getenv('DB_PASSWORD'))
    DB_HOST = str(os.getenv('DB_HOST'))
    DB_PORT = str(os.getenv('DB_PORT'))

    DB_MAIN_TABLE = str(os.getenv('DB_MAIN_TABLE'))
    DB_BACKTEST_TABLE = str(os.getenv('DB_MINERVINI_TABLE'))

    # Load the available exhange:symbol
    ori_cur, ori_conn = dbInitializeTable()
    exchange_symbol = dbGetUniqueData(ori_cur, ori_conn, DB_MAIN_TABLE)

    # New table that'll store our formatted backtest data
    columns_handled = False
    bt_cur, bt_conn = dbInitializeTable(DB_BACKTEST_TABLE)

    # Get all csvs under each folder
    for idx, (exchange, symbol) in enumerate(exchange_symbol):
        if idx <= CONST_STARTSKIP:
            continue
        if idx > CONST_CUTOFF:
            print(f"Reached cutoff amount {CONST_CUTOFF} symbols")
            break
        print(f"{idx}/{CONST_CUTOFF} Working on {exchange}:{symbol}")

        df = dbGetDataWhere(ori_cur, ori_conn, DB_MAIN_TABLE, f"exchange = '{exchange}' AND symbol = '{symbol}'")
        minervini_df = dfSetupMinervini(df.copy()).reset_index()
        
        # Create columns in backtest database
        if not columns_handled:
            backtest_columns = dfInferColumnDBTypes(minervini_df)
            db_minervini_columns = dbGetColumns(bt_cur, bt_conn, DB_BACKTEST_TABLE)
            new_columns = {k: v for k, v in backtest_columns.items() if k.lower() not in db_minervini_columns}
            if len(new_columns) > 0:
                dbSlotColumns(bt_cur, bt_conn, DB_BACKTEST_TABLE, new_columns)
            columns_handled = True

        if not minervini_df.empty:
            dbSlotDataDynamic(minervini_df, bt_cur, bt_conn, DB_BACKTEST_TABLE)
