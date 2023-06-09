from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os

from datatools.constant import *

###################### DATABASE RELATED FUNCTIONS ######################
##### DB INITIALIATION FUNCTIONS #####
def dbInitializeTable(table=DB_MAIN_TABLE):
    # Connect to your postgres DB
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    cur.execute(f"SELECT EXISTS(SELECT FROM pg_tables WHERE tablename = '{table}')")
    exists = cur.fetchone()[0]

    # Execute a command: this creates a new table
    if not exists:
        print(f"No table found in Database, creating table {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
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

##### DB SLOTTING FUNCTIONS #####
def dbSlotColumns(cur, conn, table, column_dict):
    # Iterate over your new columns
    for k, v in column_dict.items():
        # Add this column to the database table
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {k} {v}")
        print(f"Added column {k} {v} to {table}")

    # Commit the changes
    conn.commit()

def dbSlotDataOHLC(df, cur, conn, table=DB_MAIN_TABLE):
    # There's a case where ticker 'True' got interpreted as Boolean
    if df.at[0, 'symbol'] == True:
        df['symbol'] = df['symbol'].astype(str)

    # Adding the stock bars to database
    data = [(df.at[i, 'exchange'], df.at[i, 'symbol'], df.at[i, 'timestamp'], 
             df.at[i, 'open'], df.at[i, 'high'], df.at[i, 'low'], df.at[i, 'close'], 
             df.at[i, 'volume'], df.at[i, 'trade_count'], df.at[i, 'vwap'])
            for i in df.index]
    query = f"""
        INSERT INTO {table} (exchange, symbol, timestamp, open, high, low, close, volume, trade_count, vwap)
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

def dbSlotDataDynamic(df, cur, conn, table=DB_MAIN_TABLE):
    # There's a case where ticker 'True' got interpreted as Boolean
    if df.at[0, 'symbol'] == True:
        df['symbol'] = df['symbol'].astype(str)

    # Generate the column names and placeholders for the SQL statement
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    # Prepare the data for insertion
    data = [tuple(row) for row in df.values]

    # Generate the SQL statement
    query = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (exchange, symbol, timestamp) DO NOTHING
    """

    # Execute the SQL statement
    cur.executemany(query, data)

    # Attempt to commit data
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()  # If error, rollback to BEGIN
        raise ValueError(f"An error occurred: {e}")

###################### DF FORMATTING RELATED FUNCTIONS ######################
def dfInferColumnDBTypes(df):
    # Define a mapping from pandas data types to SQL data types
    type_mapping = {
        'bool': 'BOOLEAN',
        'int64': 'NUMERIC',
        'float64': 'NUMERIC',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'TEXT',
    }

    # Get the pandas data type of each column
    pandas_types = df.dtypes

    # Map the pandas data types to SQL data types
    sql_types = {column: type_mapping[str(pandas_type)]
                 for column, pandas_type in pandas_types.items()}

    return sql_types

def dfProcessDBStorage(df, dict_exchanges):
    df = df.reset_index()
    df["exchange"] = df['symbol'].map(dict_exchanges)
    return df

def dfGroupStoreCSVs(df):
    # Save to CSV
    grouped_exchange = df.groupby('exchange')
    for exchange, exchange_df in grouped_exchange:
        grouped_symbol = exchange_df.groupby('symbol')
        for symbol, symbol_df in grouped_symbol:
            csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, exchange, f"{symbol.replace('/', '-')}.csv")
            print(f"Storing to CSV: {csv_file_path}")
            symbol_df = symbol_df.drop(columns=['exchange'])
            symbol_df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path), index=False)
            pass

###################### CSV RELATED FUNCTIONS ######################
def csvGetSkipSymbols(csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, "skip_symbols.csv")):
    import csv 
    with open(csv_file_path, 'r') as f:
        reader = csv.reader(f)
        data = [tuple(row) for row in reader]
    return data

def csvAddSkipSymbols(data, csv_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, "skip_symbols.csv")):
    import csv     
    with open(csv_file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)

def csvRemovePastDate(csv_file_path, date, date_column_name="timestamp", csv_date_format='%Y-%m-%d %H:%M:%S%z'):
    # Remove all rows where column timestamp exceeded the date
    df = pd.read_csv(csv_file_path)
    df[date_column_name] = pd.to_datetime(df[date_column_name], format=csv_date_format)
    df = df[df[date_column_name] < date]
    df.to_csv(csv_file_path, index=False)
    print(f"{os.path.basename(csv_file_path)}: Keeping only rows where {date_column_name} is below {date}")

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
