import psycopg2
import gzip
import boto3
import io
import logging
import os
import sys

logger = logging.getLogger(__name__)
db_uri = os.environ.get('DB_URI', 'postgresql://postgres@localhost:5432/db')

def load():
    with psycopg2.connect(db_uri) as conn:
        cursor = conn.cursor()
        cursor.execute('''
Insert into logs_categorised
    select * from(
    select
        ts as ts
        ,path, owner_id
        ,case
            when path ~ '^/gobroker/api/_polygon' then
              regexp_replace(body::text, '"{\\"api_key_id\\":\\"(.+)\\"}"', '\1')
            else key_id
        end as key_id
        ,CASE when path like '%/gobroker/api/v1%' then 'live_v1'
              when path like '%/gobroker/api/v2%' then 'live_v2'
              when path like '%/papertrader/api/v1%' then 'paper_v1'
              when path like '%/papertrader/api/v2%' then 'paper_v2'
              when path like '%/papertrader/api/_internal%' then 'paper_dash'
              when path like '%/apigateway/_tradingview%' then 'tradingview'
              when path like '%/papertrader/api/_tradingview/%' then 'tradingview'
              when path like '%/gobroker/api/_tradingview/%' then 'tradingview'
              when path like '%/gobroker/api/_ifttt%' then 'ifttt'
              when path like '%/gobroker/api/_zaam/%' then 'zaam'                           
              when path like '%/gobroker/api/_internal/%' then 'internal' 
              when path like '%/gomarkets%' then 'data'
              when path like '%stream%' then 'data' 
              when path like '%/gobroker/api/_polygon%' then 'polygon'
              when path like '%/_brokerapi/%' then 'brokerapi' 
              when path like '%_brokerdash/internal%' then 'brokerinternal'
              when path like '%_internalbrokerapi%' then 'brokerinternal'
              when path like '%oauth%' then 'oauth'
                else 'other' end
    from logstash_2021
    where ts > '2022-03-14'
)s
;
    ''')

        cursor.execute('''
insert into owner_access_kind_2022
    select date_trunc('day', ts) as dt,
            owner_id as owner_id ,
            accesskind as access_kind,
            count(*)
    from logs_categorised
    where ts > '2022-03-14'
    group by 1, 2, 3
   
); 
    ''')
        conn.commit()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1:
        if sys.argv[1] == 'load':
            load()