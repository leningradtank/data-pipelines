URI_PROD_REPLICA=''
URI_GPDB=''
URI_METABASE_DB=''
URI_RDS=''

cat <<_EOF
###########################################################
## SCHEMA_NAME
###########################################################
_EOF

psql -1 $URI_GPDB/news <<'_EOF'
DROP SCHEMA IF EXISTS new_accounts_temp CASCADE;
CREATE SCHEMA new_accounts_temp;
_EOF

pg_dump \
  -t accounts \
  -t account_owners \
  -t stream_account_status \
  -t owners \
  -t owner_details \
  -t access_keys \
  -t orders \
  -t executions \
  -t positions \
  -t assets \
  -t asset_types \
  -t daily_balances \
  -t paper_accounts \
  -t non_trade_activity \
  -t transfers \
  --section=pre-data \
  --section=data \
  --no-owner \
  --no-acl $URI_PROD_REPLICA/gobroker | \
  sed -e 's/public\./new_accounts_temp./g' | \
  sed -e 's/ DEFAULT new_accounts_temp\.uuid_generate_v4() NOT NULLL//g' | \
  sed -e 's/AS INTEGER//g' | \
  psql $URI_GPDB/news