from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os

from datatools.constant import *

###################### DATABASE RELATED FUNCTIONS ######################
def initializeDBTable():
    global DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_TABLE_NAME

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

def slotDBData(df, cur, conn):
    global DB_TABLE_NAME
    
    # There's a case where ticker 'True' got interpreted as Boolean
    if df.at[0, 'symbol'] == True:
        df['symbol'] = df['symbol'].astype(str)

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
