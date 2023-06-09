from flask_cors import CORS
from flask import Flask

from dotenv import load_dotenv
from datatools.getdata import *
from datatools.frontend import *

'''
Run script to enable csv data fetching into local server for front end web app.

Select interpretor:
3.10

Terminal:
cd server
source venv/bin/activate
python3 server.py
'''

app = Flask(__name__)
CORS(app)

@app.route("/")
def main():
    return { "members": ["Member2", "Member1", "Member3"] }

@app.route("/members")
def members():
    return { "members": ["Member2", "Member1", "Member3"] }

@app.route("/assets")
def assets():
    return jsonAssets()

if __name__ == "__main__":
    app.run(host="localhost", port=6050, debug=True)