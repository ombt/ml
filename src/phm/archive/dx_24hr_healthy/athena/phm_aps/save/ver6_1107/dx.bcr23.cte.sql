
with aps_devices_dates_cte as (
select 
    upper(trim(ae.serialnumber)) as sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-11-03' <= ae.transaction_date
and
    ae.transaction_date < '2019-11-05'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-11-05', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
),
aps_counters_temp_cte as (
select 
    date_trunc('day', ac.timestamp_iso) as timestamp, 
    upper(trim(ac.serialnumber)) as sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join 
    aps_devices_dates_cte dd
on
    upper(trim(ac.serialnumber)) = dd.sn
and 
    date_trunc('day', ac.timestamp_iso) between
        dd.dt 
    and 
        (dd.dt_max + interval '1' day)
and
    ac.transaction_date between
        date_format((dd.dt - interval '1' day), '%Y-%m-%d')
    and 
        date_format((dd.dt_max + interval '1' day), '%Y-%m-%d')
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
),
aps_errors_bcr23_cte as (
select
    max(upper(trim(am.iom_sn))) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    count(*) as pat_errcount,
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae
inner join 
    aps_devices_dates_cte dd
on
    upper(trim(ae.serialnumber)) = dd.sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    upper(trim(ae.serialnumber)) = upper(trim(am.iom_sn))
and
    am.message like '%' || 'Unreadable Sample ID (BCR 2)' || '%' 
where 
    ae.timestamp_iso between 
        (dd.dt - interval '2' day)
    and 
        dd.dt_max
and
    ae.message like '%' || 'Unreadable Sample ID (BCR 2)' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(upper(trim(am.iom_sn))),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
),
vcount_1_min_cte as (
select
    ac.sn,
    ac.timestamp,
    max(ac.max_value) as vcount_1_min
from
    aps_counters_temp_cte ac
inner join
    aps_errors_bcr23_cte bcr23
on
    ac.sn = bcr23.iom_sn
and
    ac.timestamp = bcr23.dt
where 
    ac.id = 'tubes' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'CentrifugeCounter' 
group by
    ac.sn,
    ac.timestamp
),
vcount_1_max_cte as (
select
    ac.sn,
    ac.timestamp,
    max(ac.max_value) as vcount_1_max
from
    aps_counters_temp_cte ac
inner join
    aps_errors_bcr23_cte bcr23
on
    ac.sn = bcr23.iom_sn
and
    ac.timestamp = bcr23.dt + interval '1' day
where 
    ac.id = 'tubes' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'CentrifugeCounter' 
group by
    ac.sn,
    ac.timestamp
),
vcount_1_min_max_cte as (
select 
    vmin.sn as vmin_sn,
    vmin.timestamp as vmin_timestamp,
    vmin.vcount_1_min,
    vmax.sn as vmax_sn,
    vmax.timestamp as vmax_timestamp,
    vmax.vcount_1_max
from 
    vcount_1_min_cte vmin
inner join
    vcount_1_max_cte vmax
on
    vmax.sn = vmin.sn
and
    ( vmin.timestamp + interval '1' day ) = vmax.timestamp
and
    vmax.timestamp = date_parse('2019-11-05', '%Y-%m-%d')
order by
    vmax.sn asc,
    vmax.vcount_1_max desc
),
aps_data_cte as (
select
    '<PHM_PATTERNS_SK>' as   phm_patterns_sk, 
    '<V_RUN_DATE>' as        run_date, 
    data2.iom_sn as          iom_sn, 
    data2.pl as              pl, 
    data2.sn as              sn, 
    data2.flag_date as       timestamp, 
    '<ALGORITHM_TYPE>' as    algorithm_type, 
    data2.errors_per_day as  testcount, 
    data2.pat_errcount as    errorcount, 
    data2.per_error_count as errorpct, 
    0 as                     timestamp_ms, 
      now() as               date_created
from (
    select
        data.iom_sn,
        data.sn,
        data.pl,
        data.flag_date,
        data.pat_errcount,
        (data.vcount_1_max - data.vcount_1_min) as errors_per_day,
        case when (data.vcount_1_max - data.vcount_1_min) > 0
             then
                 (cast (data.pat_errcount as double)*100.0)/
                 (cast ((data.vcount_1_max + data.vcount_1_min) as double))
             else
                 0
             end as per_error_count
    from (
        select
            bcr23.iom_sn,
            bcr23.sn,
            bcr23.pl,
            bcr23.pat_errcount,
            bcr23.flag_date,
            v.vcount_1_min,
            v.vcount_1_max
        from
            aps_errors_bcr23_cte bcr23,
            vcount_1_min_max_cte v
        where
            v.vmin_sn = bcr23.iom_sn
        and
            v.vmin_timestamp = bcr23.dt
        ) data
    ) data2
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s') as timestamp,
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    ad.timestamp_ms, 
    date_format(ad.date_created, '%Y%m%d%H%i%s') as date_created
from 
    aps_data_cte ad
where
    ad.testcount >= 0
and
    ad.errorcount >= 0
and
    ad.errorpct >= 0
order by 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s')
