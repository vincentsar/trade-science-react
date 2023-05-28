from dotenv import load_dotenv
import os
import pickle

###################### SETUP ######################
load_dotenv()
DIR_DATA = str(os.getenv('DIR_DATA'))
DIR_SUB_DATA = str(os.getenv('DIR_SUB_DATA'))

###################### FUNCTION ######################
def jsonAssets(pickle_dir='tradable_assets.pkl'):
    pickle_file_path = os.path.join(DIR_DATA, DIR_SUB_DATA, pickle_dir)
    with open(pickle_file_path, 'rb') as f:
        tradable_assets = pickle.load(f)
    
    # Format into JSON
    json_asset = {}
    for asset in tradable_assets:
        symbol = asset['symbol']
        exchange = asset['exchange']
        
        # Format in our dictionary
        if exchange not in json_asset:
            json_asset[exchange] = []
        json_asset[exchange].append(symbol)
    
    return json_asset