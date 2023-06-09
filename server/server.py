from flask_cors import CORS
from flask import Flask


from dotenv import load_dotenv
from datatools.storedata import dbInitializeTable
from datatools.getdata import *
from datatools.frontend import *
import warnings

'''
Run script to enable csv data fetching into local server for front end web app.

Select interpretor:
3.10

Terminal:
cd server
source venv/bin/activate
python3 server.py
'''

# Mute the warning of database should use SQLAlechemy
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

app = Flask(__name__)
CORS(app)

@app.route("/")
def main():
    return { "main page": ["Main1", "Main2", "Main3"] }

@app.route("/members")
def getMembers():
    return { "members": ["Member2", "Member1", "Member3"] }

#### MINERVINI RELATED ####
@app.route("/minervini", methods=['GET'])
def getMinervini():
    df = dbGetUniqueData(bt_cur, bt_conn, DB_MINERVINI_TABLE, command="symbol", where="long_sma = TRUE AND long_hhhl = TRUE AND long_vspike = TRUE AND long_week_vup_lt_vdn = TRUE")
    return jsonDF(df)

@app.route('/minervini/<symbol>', methods=['GET'])
def getStock(symbol):
    df = dbGetDataWhereDefault(bt_cur, bt_conn, DB_MINERVINI_TABLE, f"long_sma = TRUE AND long_hhhl = TRUE AND long_vspike = TRUE AND long_week_vup_lt_vdn = TRUE AND symbol = {symbol}")
    df = dfGroupGetFirstDate(df)
    return jsonDF(df)

#### STOCK SYMBOL ####
@app.route('/stocks/<symbol>', methods=['GET'])
def getStock(symbol):
    df = dbGetDataWhereDefault(bt_cur, bt_conn, DB_MINERVINI_TABLE, f"symbol = {symbol}")
    return jsonDF(df)

@app.route("/assets")
def assets():
    return jsonAssets()

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
    DB_MINERVINI_TABLE = str(os.getenv('DB_MINERVINI_TABLE'))

    # First establish a connection to our database
    bt_cur, bt_conn = dbInitializeTable(DB_MINERVINI_TABLE)
    
    pass 

    # Then run the server
    # app.run(host="localhost", port=6050, debug=True)