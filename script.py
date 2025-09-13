import requests
import os
import csv

from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []
data = response.json()
next_url = data["next_url"]

for ticker in data['results']:
    tickers.append(ticker)
    
while "next_url" in data:
    print('requesting next page',data['next_url'])
    response = requests.get(data['next_url']+f'&apiKey={POLYGON_API_KEY}')
    for ticker in data['results']:
        tickers.append(ticker)

example_ticker = {
    'ticker':'ZWS',
    'name': 'Zurn Elkay Water Solutions Corporation',
    'market': 'stocks',
    'locale':'us',
    'primary_exchange':'XNYS',
    'type':'CS',
    'active': True,
    'currency_name':'usd',
    'cik':'0001439288',
    'composite_figi':'BBG000H8R0N8',
    'share_class_figi':"BBG00"
}

#WRITE THE TICKERS INTO A CSV FILE UNDER THE EXAMPLE SCHEMA MADE BEFORE

fieldnames = list(example_ticker.keys())
output_csv = 'tickers.csv'

with open(output_csv,mode='w',newline='',encoding='utf-8') as file:
    writer = csv.DictWriter(file,fieldnames=fieldnames)
    writer.writeheader()
    for ticker in tickers:
        row = {key: ticker.get(key,'') for key in fieldnames}
        writer.writerow(row)
print(f'Wrote {len(tickers)} rows to {output_csv}')