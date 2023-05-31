from dotenv import load_dotenv
import os

load_dotenv()
DIR_DATA = str(os.getenv('DIR_DATA'))
DIR_SUB_DATA = str(os.getenv('DIR_SUB_DATA'))

API_KEY = str(os.getenv('API_KEY'))
API_SECRET = str(os.getenv('API_SECRET'))

DB_USER = str(os.getenv('DB_USER'))
DB_NAME = str(os.getenv('DB_NAME'))
DB_PASSWORD = str(os.getenv('DB_PASSWORD'))
DB_HOST = str(os.getenv('DB_HOST'))
DB_PORT = str(os.getenv('DB_PORT'))
DB_TABLE_NAME = str(os.getenv('DB_TABLE_NAME'))