import os
import datetime
import snowflake.connector
from JsonToCSVConverter import JsonToCSVConverter
from APIDataFetcher import APIDataFecher
from dotenv import load_dotenv


def run_stock_job():
    load_dotenv()

    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    DS = datetime.datetime.now().strftime('%Y-%m-%d')
    print(DS)
    LIMIT = 1000

    url = (
        f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true'
        f'&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    )
    next_url_property_name = 'next_url'
    results_json_property_name = 'results'
    apiDataFetcher = APIDataFecher()
    tickers = apiDataFetcher.Fetch(
        url, results_json_property_name,
        next_url_property_name, POLYGON_API_KEY, DS)

    example_ticker = {
        'ticker': 'ZWS',
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
        'last_updated_utc': '2025-09-11T06:11:10.586204443Z',
        'ds': '2025-09-26'
    }

    output_csv = 'tickers.csv'

    jsonTocsvConverter = JsonToCSVConverter()
    jsonTocsvConverter.Convert(example_ticker, tickers, output_csv)

    fieldnames = list(example_ticker.keys())

    load_to_snowflake(tickers, fieldnames)
    print(f'Loaded {len(tickers)} rows to Snowflake')


def load_to_snowflake(rows, fieldnames):
    connect_kwargs = {
        'user': os.getenv('SNOWFLAKE_USR'),
        'password': os.getenv('SNOWFLAKE_PSWD')
    }
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    if account:
        connect_kwargs['account'] = account
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')

    if warehouse:
        connect_kwargs['warehouse'] = warehouse
    if database:
        connect_kwargs['database'] = database
    if schema:
        connect_kwargs['schema'] = schema
    if role:
        connect_kwargs['role'] = role

    conn = snowflake.connector.connect(
        user=connect_kwargs['user'],
        password=connect_kwargs['password'],
        account=connect_kwargs['account'],
        database=connect_kwargs.get('database'),
        schema=connect_kwargs.get('schema'),
        warehouse=connect_kwargs.get('warehouse'),
        role=connect_kwargs.get('role'),
        session_parameters={
            "CLIENT_TELEMETRY_ENABLED": False,
        })
    try:
        cs = conn.cursor()
        try:
            table_name = os.getenv('SNOWFLAKE_TABLE', 'stock_tickers')

            type_overrides = {
                'ticker': 'varchar',
                'name': 'varchar',
                'market': 'varchar',
                'locale': 'varchar',
                'primary_exchange': 'varchar',
                'type': 'varchar',
                'active': 'boolean',
                'currency_name': 'varchar',
                'cik': 'varchar',
                'composite_figi': 'varchar',
                'share_class_figi': 'varchar',
                'last_updated_utc': 'TIMESTAMP',
                'ds': 'varchar'
            }

            columns_sql_parts = []
            for col in fieldnames:
                col_type = type_overrides.get(col, 'varchar')
                columns_sql_parts.append(f'"{col.upper()}" {col_type}')

            create_table_sql = (
                f'CREATE TABLE IF NOT EXISTS {table_name} ('
                + ', '.join(columns_sql_parts) + ')'
            )
            cs.execute(create_table_sql)

            column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
            placeholders = ', '.join(
                [f'%({c})s' for c in fieldnames])
            insert_sql = f'INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})'

            transformed = []
            for t in rows:
                row = {}
                for k in fieldnames:
                    row[k] = t.get(k, None)
                transformed.append(row)

            if transformed:
                cs.executemany(insert_sql, transformed)
        finally:
            cs.close()
    finally:
        conn.close()


run_stock_job()
