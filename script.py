import os
import signal
from JsonToCSVConverter import JsonToCSVConverter
from APIDataFetcher import APIDataFecher
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000

url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
next_url_property_name = 'next_url'
results_json_property_name = 'results'
apiDataFetcher = APIDataFecher()
tickers = apiDataFetcher.Fetch(
    url, results_json_property_name,
    next_url_property_name, POLYGON_API_KEY)

example_ticker = {'ticker': 'ZWS',
                  'name': 'Zurn Elkay Water Solutions Corporation',
                  'market': 'stocks',
                  'locale': 'us',
                  'primary_exchange': 'XNYS',
                  'type': 'CS',
                  'active': True,
                  'currency_name': 'usd',
                  'cik': '0001439288',
                  'composite_figi': 'BBG000H8R0N8',
                  'share_class_figi': 'BBG001T36GB5',
                  'last_updated_utc': '2025-09-11T06:11:10.586204443Z'}


output_csv = 'tickers.csv'

jsonTocsvConverter = JsonToCSVConverter()
jsonTocsvConverter.Convert(example_ticker, tickers, output_csv)
