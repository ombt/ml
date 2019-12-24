-- 
-- with dd_cte as (
-- select 
--     upper(trim(ae.serialnumber)) as sn,
--     min(date_trunc('day', ae.timestamp_iso)) as dt,
--     max(ae.timestamp_iso) as dt_max
-- from 
--     dx.dx_aps_error ae
-- where
--     '<START_DATE>' <= ae.transaction_date
-- and
--     ae.transaction_date < '<END_DATE>'
-- and
--     date_trunc('day', ae.timestamp_iso) < date_parse('<END_DATE>', '%Y-%m-%d')
-- group by
--     upper(trim(ae.serialnumber))
-- ),
-- ac_cte as (
-- select 
--     date_trunc('day', ac.timestamp_iso) as dt, 
--     upper(trim(ac.serialnumber)) as sn, 
--     ac.duration, 
--     ac.description, 
--     ac.id, 
--     max(ac.value) as max_value, 
--     min(ac.value) as min_value
-- from 
--     dx.dx_aps_counter ac,
--     (
--     select 
--         upper(trim(ae.serialnumber)) as sn,
--         min(date_trunc('day', ae.timestamp_iso)) as dt,
--         max(ae.timestamp_iso) as dt_max
--     from 
--         dx.dx_aps_error ae
--     where
--         '<START_DATE>' <= ae.transaction_date
--     and
--         ae.transaction_date < '<END_DATE>'
--     and
--         date_trunc('day', ae.timestamp_iso) < date_parse('<END_DATE>', '%Y-%m-%d')
--     group by
--         upper(trim(ae.serialnumber))
--     ) dd
-- where 
--     upper(trim(ac.serialnumber)) = dd.sn
-- and 
--     date_trunc('day', ac.timestamp_iso) between 
--         dd.dt 
--     and 
--         dd.dt_max + interval '1' day
-- and 
--     ac.id in ('normal','priority','tubes',
--            '1','2','3','4','5','6','7','8')  
-- and 
--     ac.duration in ('YTD')
-- and 
--     ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
-- group by 
--     date_trunc('day', ac.timestamp_iso), 
--     ac.serialnumber, 
--     ac.duration, 
--     ac.description, 
--     ac.id
-- ),
-- ebcr1_cte as (
-- select
--     max(ae.productline) as pl, 
--     ae.serialnumber as sn, 
--     date_trunc('day', ae.timestamp_iso) as dt, 
--     count(*) as pat_errcount, 
--     max(ae.timestamp_iso) as flag_date
-- from 
--     dx.dx_aps_error ae,
--     (
--     select 
--         upper(trim(ae.serialnumber)) as sn,
--         min(date_trunc('day', ae.timestamp_iso)) as dt,
--         max(ae.timestamp_iso) as dt_max
--     from 
--         dx.dx_aps_error ae
--     where
--         '<START_DATE>' <= ae.transaction_date
--     and
--         ae.transaction_date < '<END_DATE>'
--     and
--         date_trunc('day', ae.timestamp_iso) < date_parse('<END_DATE>', '%Y-%m-%d')
--     group by
--         upper(trim(ae.serialnumber))
--     ) dd
-- where 
--     ae.message like '%BCR%1%' 
-- and 
--     ae.serialnumber = dd.sn
-- and 
--     ae.timestamp_iso between 
--         (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
--     and 
--         dd.dt_max
-- group by 
--     ae.serialnumber,
--     date_trunc('day', ae.timestamp_iso)
-- )
-- select
--     '<PHM_PATTERNS_SK>' as phm_patterns_sk, 
--     '<V_RUN_DATE>' as run_date, 
--     y.sn as iom_sn, 
--     y.pl as pl, 
--     y.sn as sn, 
--     y.flag_date as timestamp, 
--     '<ALGORITHM_TYPE>' as algorithm_type, 
--     errors_per_day as testcount, 
--     y.pat_errcount as errorcount, 
--     per_error_count as errorpct, 
--     0 as timestamp_ms, 
--     now() as date_created
-- from (
--     select
--         eb.sn,
--         eb.pat_errcount,
--         (e.vcount_normal_max + 
--          e.vcount_priority_max -
--          e.vcount_normal_min -
--          e.vcount_priority_min) as errors_per_day,
--         case when (e.vcount_normal_max + 
--                    e.vcount_priority_max -
--                    e.vcount_normal_min -
--                    e.vcount_priority_min) > 0
--              then
--                  (eb.pat_errcount*100.0) /
--                  (e.vcount_normal_max + 
--                   e.vcount_priority_max -
--                   e.vcount_normal_min -
--                   e.vcount_priority_min)
--              else
--                  0
--              end as per_error_count
--     from 
--         ebcr1_cte eb,
--         (
--         select distinct
--             ebcr1_cte.sn,
--             a.min_max_value as vcount_normal_min,
--             b.max_max_value as vcount_normal_max,
--             c.min_max_value as vcount_priority_min,
--             d.max_max_value as vcount_priority_max 
--         from (
--             select 
--                 ebcr1_cte.sn,
--                 min(ac_cte.max_value) as min_max_value
--             from 
--                 ac_cte,
--                 ebcr1_cte
--             where 
--                 ac_cte.id = 'normal' 
--             and 
--                 ac_cte.duration = 'YTD'
--             and 
--                 ac_cte.description = 'InputTubeCounter' 
--             and 
--                 ac_cte.sn = ebcr1_cte.sn 
--             and 
--                 ebcr1_cte.dt <= ac_cte.dt
--             group by
--                 ebcr1_cte.sn
--             ) a,
--             (
--             select 
--                 ebcr1_cte.sn,
--                 max(ac_cte.max_value) as max_max_value
--             from 
--                 ac_cte,
--                 ebcr1_cte
--             where 
--                 ac_cte.id = 'normal' 
--             and 
--                 ac_cte.duration = 'YTD'
--             and 
--                 ac_cte.description = 'InputTubeCounter' 
--             and 
--                 ac_cte.sn = ebcr1_cte.sn 
--             and 
--                 ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
--             group by
--                 ebcr1_cte.sn
--             ) b,
--             (
--             select 
--                 ebcr1_cte.sn,
--                 min(ac_cte.max_value) as min_max_value
--             from 
--                 ac_cte,
--                 ebcr1_cte
--             where 
--                 ac_cte.id = 'priority' 
--             and 
--                 ac_cte.duration = 'YTD'
--             and 
--                 ac_cte.description = 'InputTubeCounter' 
--             and 
--                 ac_cte.sn = ebcr1_cte.sn 
--             and 
--                 ebcr1_cte.dt <= ac_cte.dt
--             group by
--                 ebcr1_cte.sn
--             ) c,
--             (
--             select 
--                 ebcr1_cte.sn,
--                 max(ac_cte.max_value) as max_max_value
--             from 
--                 ac_cte,
--                 ebcr1_cte
--             where 
--                 ac_cte.id = 'priority' 
--             and 
--                 ac_cte.duration = 'YTD'
--             and 
--                 ac_cte.description = 'InputTubeCounter' 
--             and 
--                 ac_cte.sn = ebcr1_cte.sn 
--             and 
--                 ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
--             group by
--                 ebcr1_cte.sn
--             ) d,
--             ebcr1_cte
--         where
--             a.sn = ebcr1_cte.sn
--         and
--             b.sn = ebcr1_cte.sn
--         and
--             c.sn = ebcr1_cte.sn
--         and
--             d.sn = ebcr1_cte.sn
--         ) e
--     where
--         eb.sn = e.sn
--     ) aps_data,
--     ebcr1_cte as y
-- where
--     aps_data.sn = y.sn
-- limit 100

with dd_cte as (
select
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as max_dt
from 
    dx.dx_aps_error ae
where
    '2019-07-01' <= ae.transaction_date
and
    ae.transaction_date < '2019-07-02'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-07-02', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by 1, 2
),
ac_cte as (
select 
    date_trunc('day', ac.timestamp_iso) as dt, 
    upper(trim(ac.serialnumber)) as iom_sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join (
    select
        upper(trim(ae.serialnumber)) as iom_sn,
        min(date_trunc('day', ae.timestamp_iso)) as dt,
        max(ae.timestamp_iso) as dt_max
    from 
        dx.dx_aps_error ae
    where
        '2019-07-01' <= ae.transaction_date
    and
        ae.transaction_date < '2019-07-02'
    and
        date_trunc('day', ae.timestamp_iso) < date_parse('2019-07-02', '%Y-%m-%d')
    group by
        upper(trim(ae.serialnumber))
    order by 1, 2
) x
on 
    upper(trim(ac.serialnumber)) = x.iom_sn 
and 
    x.dt <= date_trunc('day', ac.timestamp_iso)
and
    date_trunc('day', ac.timestamp_iso) < (x.dt_max + interval '1' day)
where 
   ac.id in ('normal','priority','tubes', '1','2','3','4','5','6','7','8')  
and 
   ac.duration in ('YTD')
and 
   ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso),
    upper(trim(ac.serialnumber)),
    ac.duration, 
    ac.description, 
    ac.id
)
select * from ac_cte

