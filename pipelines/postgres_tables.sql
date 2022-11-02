----! categorize access by path !----
drop table if exists logstash_categorise;
create table logstash_categorise with (
    appendonly = true,
    blocksize = 2097152,
    orientation = column,
    compresstype = zlib,
    fillfactor = 100
)as 
select * from(
    select
        timestamp as ts
        ,path, owner_id
        ,case
            when path ~ '^/gobroker/api/_polygon' then
              regexp_replace(body::text, '"{\\"api_key_id\\":\\"(.+)\\"}"', '\1')
            else key_id
        end as key_id
        ,case
            when path ~ '^/gobroker/api/v1' then 'live_v1'
            when path ~ '^/gobroker/api/v2' then 'live_v2'
            when path ~ '^/papertrader/api/v1' then 'paper_v1'
            when path ~ '^/papertrader/api/v2' then 'paper_v2'
            when path ~ '^/apigateway/_tradingview' then 'tradingview'
            when path ~ '^/gobroker/api/_internal/' then 'internal'
            when path ~ '^/gomarkets' then 'data'
            when path ~ '^/gobroker/api/_polygon' then 'polygon'
            else 'other'
        end as access_kind
    from logs_staging
    where timestamp >= '2019-01-01'
)s
where coalesce(owner_id, '') <> '' or coalesce(key_id, '') <> ''
distributed by (ts)
;


----* categorize access by path (incremental) *----
insert into logstash_categorise2
select * from(
    select
        ts
        ,path, owner_id
        ,case
            when path ~ '^/gobroker/api/_polygon' then
              regexp_replace(body::text, '"{\\"api_key_id\\":\\"(.+)\\"}"', '\1')
            else key_id
        end as key_id
        ,case
            when path ~ '^/gobroker/api/v1' then 'live_v1'
            when path ~ '^/gobroker/api/v2' then 'live_v2'
            when path ~ '^/papertrader/api/v1' then 'paper_v1'
            when path ~ '^/papertrader/api/v2' then 'paper_v2'
            when path ~ '^/apigateway/_tradingview' then 'tradingview'
            when path ~ '^/gobroker/api/_internal/' then 'internal'
            when path ~ '^/gomarkets' then 'data'
            when path ~ '^/gobroker/api/_polygon' then 'polygon'
            when path ~ '^/_brokerapi/' then 'brokerapi' 
            else 'other'
        end as access_kind
    from logs_staging2
    where ts > (select max(ts) from logstash_categorise2)
)s
where coalesce(owner_id, '') <> '' or coalesce(key_id, '') <> ''
;

----* map key to owner id *----
drop table if exists key_mapping;
create table key_mapping as
select distinct key_id, owner_id
from logstash_categorise
where nullif(key_id, '') is not null
;

--! summarize owner access by day !-----
create table owner_access_summary with (
    appendonly = true,
    blocksize = 2097152,
    orientation = column,
    compresstype = zlib,
    fillfactor = 100
) as
select owner_id, dt, access_kind, count(*)
from(
    select
        coalesce(h.owner_id, m.owner_id) as owner_id
        ,ts::date as dt
        ,access_kind
    from httplog2019 h
    left join key_mapping m on h.key_id = m.key_id
)s
where access_kind <> 'other'
group by owner_id, dt, access_kind
;


----! summarize owner access by day !-----
drop table if exists activity_type;
create table activity_type with(
    appendonly = true,
    blocksize = 2097152,
    orientation = column,
    compresstype = zlib,
    fillfactor = 100
) as 
select * from(
    select
    (ts at time zone 'America/New_York')::date as dt, owner_id, access_kind, count(*)
    from logstash_categorise
    group by dt, owner_id, access_kind

    union

    select x.dt, y.owner_id, access_kind, sum(count) as count
    from(
    select (ts at time zone 'America/New_York')::date as dt, key_id, access_kind, count(*)
    from logstash_categorise
    where access_kind in ('polygon', 'data') and nullif(key_id, '') is not null
    group by 1, 2, 3
    )x left join key_mapping y on x.key_id = y.key_id
    group by dt, owner_id, access_kind
)s
where owner_id is not null

distributed by (dt)
;

----* summarize owner access by day (incremental) *-----
insert into activity_type
select * from(
    select
    (ts at time zone 'America/New_York')::date as dt, owner_id, access_kind, count(*)
    from logstash_categorise2
    where ts > (select max(dt) from activity_type) + 1
    group by dt, owner_id, access_kind

    union

    select x.dt, y.owner_id, access_kind, sum(count) as count
    from(
        select (ts at time zone 'America/New_York')::date as dt, key_id, access_kind, count(*)
        from logstash_categorise2
        where access_kind in ('polygon', 'data') and nullif(key_id, '') is not null
        and ts > (select max(dt) from activity_type) + 1
        group by 1, 2, 3
    )x left join key_mapping y on x.key_id = y.key_id
    group by dt, owner_id, access_kind
)s
where owner_id is not null
;


----! online queries !-----
--------------------------
-- 30 days rolling activity
--------------------------
select dt_range as dt, count(distinct owner_id)
from(
select dt, owner_id, generate_series(dt, dt + 29, interval '1 day')::date as dt_range
from(
    select dt, owner_id
    from activity_type
    where access_kind in ('live_v1', 'live_v2', 'paper_v1', 'paper_v2', 'data')
    group by dt, owner_id
)s
)s
group by dt_range
having dt_range < current_date

-----------------------------
-- daily active per kind
-----------------------------
select dt
,count(distinct case when access_kind in ('live_v1', 'live_v2') then owner_id end) as live_api_users
,count(distinct case when access_kind in ('paper_v1', 'paper_v2') then owner_id end) as paper_api_users
,count(distinct case when access_kind = 'internal' then owner_id end) as dashboard_users
,count(distinct case when access_kind = 'polygon' then owner_id end) as polygon_users
,count(distinct case when access_kind = 'tradingview' then owner_id end) as tradinview_users
,count(distinct case when access_kind = 'other' then owner_id end) as other_users
--access_kind, count(distinct owner_id) 
from activity_type
where owner_id is not null
group by dt
order by dt



-----------------------------
-- weekly activity + balance
-----------------------------
select
    coalesce(s.dt, db.dt) dt
    ,count(s.owner_id) as activity_active
    ,count(db.owner_id) as db_active
    ,count(coalesce(s.owner_id, db.owner_id)) as total_active
from(
    select
        date_trunc('week', dt) dt,
        owner_id
    from activity_type
    where dt < date_trunc('week', current_timestamp)
    group by date_trunc('week', dt), owner_id
)s full join (
    select date_trunc('week', asof) dt, owner_id::text from gobroker.daily_balances db join gobroker.account_owners ao on db.account_id = ao.account_id
    where equity > 0 and asof >= '2019-01-01'
    group by dt, owner_id
)db on s.dt = db.dt and s.owner_id = db.owner_id
group by 1
order by dt 