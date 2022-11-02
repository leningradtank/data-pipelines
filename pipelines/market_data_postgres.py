import requests
import psycopg2
from datetime import datetime, timedelta
import pytz
import os
import logging
import csv
from io import StringIO
import sys
import re


db_uri = os.environ.get('URI_DWH', 'postgresql://postgres@localhost:5432/db')
api_host = 'https://api.polygon.io'
api_key = 'abc321'

def grouped_daily(date, unadjusted=True):
    locale = 'US'
    market = 'STOCKS'
    params = {
        'apiKey': api_key,
    }

    url = f'{api_host}/v2/aggs/grouped/locale/{locale}/market/{market}/{date}'

    resp = requests.get(url, params=params)
    return resp.json()


def load_db(date):
    logging.info(f'loading {date}')
    resp = grouped_daily(date)
    with psycopg2.connect(db_uri) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE SCHEMA IF NOT EXISTS polygon;
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS polygon.grouped_daily(
                asof date,
                symbol text,
                open decimal,
                high decimal,
                low decimal,
                close decimal,
                volume decimal
            );
        ''')
        cursor.execute('''
            DROP TABLE IF EXISTS polygon._temp_grouped_daily;
            CREATE TABLE polygon._temp_grouped_daily(
                LIKE polygon.grouped_daily
            );
        ''')

        buf = StringIO()
        w = csv.writer(buf)
        pat = re.compile(r'^[A-Z\.]+$')
        for res in resp['results']:
            if not pat.match(res['T']):
                # garbage
                continue
            w.writerow((
                date,
                res['T'],
                str(res['o']),
                str(res['h']),
                str(res['l']),
                str(res['c']),
                str(res['v']),
            ))
        buf.seek(0)
        cursor.copy_expert('COPY polygon._temp_grouped_daily FROM STDIN CSV', buf)

        cursor.execute('''
            INSERT INTO polygon.grouped_daily
            SELECT * FROM polygon._temp_grouped_daily
            WHERE (asof, symbol) NOT IN (SELECT asof, symbol FROM polygon.grouped_daily);
        ''')
        cursor.execute('''
            DROP TABLE IF EXISTS polygon.data_version;
            CREATE TABLE polygon.data_version AS SELECT current_timestamp AS loaded_asof;
            DROP TABLE polygon._temp_grouped_daily;
        ''')
        conn.commit()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1:
        if sys.argv[1] == 'load_db':
            if len(sys.argv) > 2:
                date = sys.argv[2]
                load_db(date)
            else:
                today = datetime.now(tz=pytz.timezone('America/New_York'))
                for i in range(5):
                    date = today.strftime('%Y-%m-%d')
                    load_db(date)
                    today = today - timedelta(days=1)