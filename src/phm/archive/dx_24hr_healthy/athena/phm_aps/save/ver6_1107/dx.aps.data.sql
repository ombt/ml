with aps_devices_dates_cte as (
select 
    upper(trim(ae.serialnumber)) as sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-10-29' <= ae.transaction_date
and
    ae.transaction_date < '2019-10-30'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-10-30', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
),
phm_aps_data as (
select
    1065 as phm_algorithm_definitions_sk, 
    10057 as phm_patterns_sk, 
    date_parse('2019-10-30', '%Y-%m-%d') as run_date, 
    ae_dd.iom_sn as iom_sn, 
    ae_dd.pl as pl, 
    ae_dd.module_sn as sn, 
    ae_dd.ms_time as timestamp, 
    'COUNT' as algorithm_type, 
    0 as testcount, 
    ae_dd.errorcount as errorcount, 
    0 as errorpct, 
    0 as timestamp_ms, 
    date_parse('2019-10-30', '%Y-%m-%d') as date_created
from (
    select
        max(upper(trim(am.iom_sn))) as iom_sn,
        am.pl as pl,
        upper(trim(am.sn)) as module_sn,
        date_trunc('day', ae.timestamp_iso) as flag_date,
        count(*) as errorcount,
        max(ae.timestamp_iso) as ms_time
    from 
        dx.dx_aps_error ae
    inner join 
        aps_devices_dates_cte dd
    on
        upper(trim(ae.serialnumber)) = dd.sn
    inner join
        dx_phm.phm_aps_pl_sn_mapping am
    on
        upper(trim(am.iom_sn)) = dd.sn
    and
        am.message like '%' || 'Carrier Routing Error - Decapper Gate' || '%'
    where 
        ae.timestamp_iso  betwee
            (dd.dt - interval '2' day)
        and 
            dd.dt_max
    and
        ae.message like '%' || 'Carrier Routing Error - Decapper Gate' || '%' 
    group by
        am.pl,
        upper(trim(am.sn)),
        date_trunc('day', ae.timestamp_iso)
    order by
        max(upper(trim(am.iom_sn))),
        upper(trim(am.sn)),
        date_trunc('day', ae.timestamp_iso)
    ) ae_dd
)
select * from phm_aps_data
