URI_PROD_REPLICA=''
URI_GPDB=''

cat <<_EOF
###########################################################
## SCHEMA NAME
###########################################################
_EOF

psql -1 $URI_GPDB/database_revenue <<'_EOF'
DROP SCHEMA IF EXISTS revenue_table CASCADE;
CREATE SCHEMA revenue_table;
_EOF

pg_dump \
    -t user_correspondents \
    -t users \
    --section=pre-data \
    --section=data \
    --no-owner \
    --no-acl $URI_PROD_REPLICA/paciam | \
    sed -e 's/public\./ revenue_table./g' | \
    sed -e 's/ DEFAULT  revenue_table\.gen_random_uuid()//g' | \
    sed -e 's/ DEFAULT revenue_table\.uuid_generate_random_uuid() NOT NULL//g' | \
    sed -e 's/ DEFAULT revenue_table\.uuid_generate_v4() NOT NULL//g' | \
    sed -e 's/revenue_table.citext/text/g' | \
    sed -e "s/ revenue_table.user_role DEFAULT 'operations':: revenue_table.user_role/text/g" | \
    sed -e 's/AS INTEGER//gi' | \
    psql $URI_GPDB/database_revenue