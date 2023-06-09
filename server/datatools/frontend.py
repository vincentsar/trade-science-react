import os 
import pickle
import pandas as pd
import json
from datatools.constant import *

###################### FRONT-END RELATED FUNCTION ######################
def jsonAssets(pickle_dir='tradable_assets.pkl'):
    global DIR_DATA, DIR_SUB_DATA
    pickle_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, pickle_dir)
    with open(pickle_file_path, 'rb') as f:
        tradable_assets = pickle.load(f)
    
    # Format into JSON
    json_asset = {}
    for asset in tradable_assets:
        symbol = asset['symbol']
        exchange = asset['exchange']
        if exchange == 'OTC':
            continue
        
        # Format in our dictionary
        if exchange not in json_asset:
            json_asset[exchange] = []
        json_asset[exchange].append(symbol)
    
    return json_asset   

def jsonDF(df):
    json_str = df.to_json(orient='records')
    json_obj = json.loads(json_str)
    return json_obj

###################### SPECIFIC DATABASE FUNCTION ######################
##### DB MINERVINI FUNCTIONS #####
def dbGetDataWhereDefault(cur, conn, table, where, datafetch="*"):
    '''
    Could be slightly faster compared to: pd.read_sql_query(sql_query, conn)
    - With lesser readability
    '''
    sql_query = f"SELECT {datafetch} FROM {table} WHERE {where}"
    cur.execute(sql_query)
    column_names = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=column_names)

    return df

###################### SPECIFIC PANDAS FUNCTION ######################
def dfGroupGetFirstDate(df):
    '''
    Given a mix of symbols with different time stamp, group them together. 
    - Then, store only the first date of each group.
    - Used to display the list of grouping available. 
    '''
    df.sort_values(by=['symbol', 'timestamp'], inplace=True)
    # df['timestamp'] = pd.to_datetime(df['timestamp'])

    # create a new column 'group' that increments every time there's a gap of more than one day or different symbol
    df['group'] = ((df['symbol'] != df['symbol'].shift()) | (df['timestamp'] - df['timestamp'].shift() > pd.Timedelta(days=1))).cumsum()

    # get the first row of each group
    df = df.groupby('group').first().reset_index(drop=True)

    return df