import psycopg2
import os, sys, json
from datetime import datetime, date, timedelta

path = os.path.abspath(__file__)
dirname = os.path.dirname(path)
fileName = 'config.json'
config_file = os.path.join(dirname,fileName)
yesterday = (date.today() - timedelta(2)).strftime('%Y-%m-%d')
csvFolderName = 'data/'+yesterday
makeCsvFoler = os.path.join(dirname,csvFolderName)

if not os.path.exists(makeCsvFoler):
    os.makedirs(makeCsvFoler)

with open(config_file, 'r', encoding="utf-8") as f:
    config = json.load(f)

CONNECTION = config['local']['connection']
APPLICATIONBOOK_COL = config['local']['applicationbook_col']

DELETE_DATA_MONTH = config['delete_data_month']

ONEDAY = config['datetime']['oneday']

def getConnection():
    conn = psycopg2.connect(**CONNECTION)
    return conn