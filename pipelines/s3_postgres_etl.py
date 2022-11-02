import psycopg2
import gzip
import boto3
import io
import logging
import os
import sys
logger = logging.getLogger(__name__)
db_uri = os.environ.get('DB_URI', 'postgresql://postgres@localhost:5432/db')
s3 = boto3.resource('s3')
bucket = s3.Bucket('staging_alpha.logstash')

def get_s3_dates(prefix='logs/alpha/event_logs/'):
    def get_common_prefixes(p):
        return [
            o.get('Prefix') for o in boto3.client('s3').list_objects_v2(
                Bucket=bucket.name, Prefix=p, Delimiter='/').get('CommonPrefixes')
        ] or []
    results = []
    for year in get_common_prefixes(prefix):
        for month in get_common_prefixes(year):
            for date in get_common_prefixes(month):
                results.append('/'.join(date.rstrip('/').split('/')[-3:]))
    # exclude last date
    print("s3 connected")
    return results[:-1]
def load_file(cursor, bucket, key):
    logging.info(f'downloading {key}')
    buf = io.BytesIO()
    
    bucket.download_fileobj(key, buf)
    buf.seek(0)
    print("loadedfile")
    with gzip.GzipFile(fileobj=buf) as gf:
        fout = io.BytesIO()
        lines = gf.readlines()
        fout.writelines(line for line in lines if line.strip())
        fout.truncate()
        fout.seek(0)
        
        cursor.copy_expert( 
            "COPY logging_staging_broker2 FROM STDIN CSV QUOTE E'\x10' DELIMITER '\t'", fout)

def load_date(dt):
    prefix = f'logs/alpha/event_logs/{dt}'

    with psycopg2.connect(db_uri) as conn:
        cursor = conn.cursor()
        for obj in bucket.objects.filter(Prefix=prefix):
            try:
                load_file(cursor, bucket, obj.key)
            except psycopg2.DataError as e:
                logger.error(f'error loading {obj.key}: {str(e)}')
                conn.rollback()
                cursor = conn.cursor()
            else:
                conn.commit()

def load():
    dates = get_s3_dates()
    with psycopg2.connect(db_uri) as conn: #check code before running 
        cursor = conn.cursor()
        cursor.execute('''
            DROP TABLE IF EXISTS logging_staging_broker2;
            CREATE TABLE logging_staging_broker2( 
                ts timestamp,
                kind text,
                b jsonb
            );
        ''')
        cursor.execute('''
            SELECT to_char(max(ts::date), 'YYYY/mm/dd')
            FROM log_date_broker;
        ''')

        last_date = cursor.fetchone()[0]
        conn.commit()

    for date in [date for date in dates if date > last_date]:
        load_date(date)

    with psycopg2.connect(db_uri) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO prod_daily_httplog_broker
            SELECT
                b->>'path',
                b->>'owner_id',
                b->>'acc_id',
                b->>'elapsed',
                b->>'method',
                b->>'query',
                b->>'key_id',
                b->>'correspondent',
                nullif((b->'body')::jsonb, '""'),
                ts,
                b->>'env'
            FROM logging_staging_broker
            WHERE b->>'path' like '%brokerapi%'
            ;
        ''')

        cursor.execute('''
        
            INSERT INTO log_date_broker(ts)
            SELECT max(ts)
            FROM logging_staging_broker2; 
        
        ''')
        print("done")
        conn.commit()


# WHERE JSON_EXTRACT(b, '$.path') LIKE '%brokerapi%'

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1:
        if sys.argv[1] == 'load':
            load()