import os 
import pickle
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