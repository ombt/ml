
with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Automated Refrigerator: Error 27: Rack Transfer Failed (Barrier Sensor Check)' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Automated Refrigerator: Error 27: Rack Transfer Failed (Barrier Sensor Check)' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Automated Refrigerator: Error 28: Automated Refrigerator Barrier Failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Automated Refrigerator: Error 28: Automated Refrigerator Barrier Failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Automated Refrigerator: Error 63:' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Automated Refrigerator: Error 63:' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Aliquoter Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Aliquoter Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Centrifuge 2 Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Centrifuge 2 Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Centrifuge 2 Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Centrifuge 2 Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Centrifuge 2 Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Centrifuge 2 Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Centrifuge Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Centrifuge Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Centrifuge Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Centrifuge Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Centrifuge Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Centrifuge Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Decapper Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Decapper Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Decapper Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Decapper Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Desealer Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Desealer Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Desealer Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Desealer Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 1 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 1 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 2 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 2 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 3 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 3 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 4 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 4 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 5 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 5 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 6 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 6 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 7 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 7 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Instrument 8 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Instrument 8 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - IOM Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - IOM Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - IOM Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - IOM Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - IOM Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - IOM Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - IOM Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - IOM Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Resealer Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Resealer Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Resealer Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Resealer Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 4' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 5' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 6' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 7' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Return Gate 8' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Return Gate 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - RIM Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - RIM Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 4' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 5' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 6' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 7' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Sampling Gate 8' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Sampling Gate 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Storage Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Storage Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Storage Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Storage Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Storage Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Storage Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Did Not Pass - Storage Gate D' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Did Not Pass - Storage Gate D' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Aliquoter Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Aliquoter Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Centrifuge 2 Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Centrifuge 2 Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Centrifuge 2 Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Centrifuge 2 Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Centrifuge 2 Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Centrifuge 2 Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Centrifuge Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Centrifuge Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Centrifuge Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Centrifuge Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Centrifuge Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Centrifuge Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Decapper Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Decapper Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Decapper Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Decapper Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Desealer Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Desealer Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Desealer Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Desealer Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 1 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 1 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 2 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 2 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 3 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 3 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 4 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 4 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 5 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 5 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 6 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 6 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 7 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 7 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Instrument 8 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Instrument 8 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - IOM Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - IOM Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - IOM Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - IOM Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - IOM Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - IOM Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - IOM Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - IOM Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Resealer Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Resealer Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Resealer Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Resealer Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 4' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 5' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 6' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 7' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Return Gate 8' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Return Gate 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - RIM Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - RIM Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 4' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 5' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 6' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 7' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Sampling Gate 8' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Sampling Gate 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Storage Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Storage Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Storage Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Storage Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Storage Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Storage Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Carrier Routing Error - Storage Gate D' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Carrier Routing Error - Storage Gate D' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge 2 Temperature Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge 2 Temperature Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge 2: Error 28: Total Timeout Expired' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge 2: Error 28: Total Timeout Expired' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 21: Moving Fault - Axis 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 21: Moving Fault - Axis 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 22: Moving Fault - Axis 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 22: Moving Fault - Axis 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 23: Moving Fault - Axis 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 23: Moving Fault - Axis 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 34: Axis 1 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 34: Axis 1 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 35: Axis 2 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 35: Axis 2 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 36: Axis 3 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 36: Axis 3 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 41: Gripper Open Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 41: Gripper Open Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 42: Gripper Close Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 42: Gripper Close Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 48: Arm Limp' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 48: Arm Limp' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 50: Tube Lost' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 50: Tube Lost' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot 2: Error 51: Object found in Gripper' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot 2: Error 51: Object found in Gripper' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 21: Moving Fault - Axis 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 21: Moving Fault - Axis 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 22: Moving Fault - Axis 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 22: Moving Fault - Axis 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 23: Moving Fault - Axis 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 23: Moving Fault - Axis 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 34: Axis 1 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 34: Axis 1 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 35: Axis 2 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 35: Axis 2 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 36: Axis 3 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 36: Axis 3 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 41: Gripper Open Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 41: Gripper Open Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 42: Gripper Close Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 42: Gripper Close Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 48: Arm Limp' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 48: Arm Limp' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 50: Tube Lost' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 50: Tube Lost' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Robot: Error 51: Object found in Gripper' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Robot: Error 51: Object found in Gripper' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge Temperature Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge Temperature Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Centrifuge: Error 28: Total Timeout Expired' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Centrifuge: Error 28: Total Timeout Expired' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Decapper: Error 19: Grip/Release Cap Failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Decapper: Error 19: Grip/Release Cap Failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Decapper: Error 20: Cap Drop Failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Decapper: Error 20: Cap Drop Failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Decapper: Error 21: Head Up Failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Decapper: Error 21: Head Up Failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Decapper: Error 22: Head Down Failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Decapper: Error 22: Head Down Failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 21: Moving Fault - Axis 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 21: Moving Fault - Axis 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 22: Moving Fault - Axis 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 22: Moving Fault - Axis 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 23: Moving Fault - Axis 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 23: Moving Fault - Axis 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 34: Axis 1 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 34: Axis 1 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 35: Axis 2 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 35: Axis 2 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 36: Axis 3 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 36: Axis 3 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 41: Gripper Open Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 41: Gripper Open Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 42: Gripper Close Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 42: Gripper Close Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 48: Arm Limp' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 48: Arm Limp' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 50: Tube Lost' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 50: Tube Lost' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'I/O Robot: Error 51: Object found in Gripper' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'I/O Robot: Error 51: Object found in Gripper' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 113: Carrier not in list' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 113: Carrier not in list' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS1' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS1' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 301: Carrier Did Not Pass - Pass Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 301: Carrier Did Not Pass - Pass Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 302: Carrier Did Not Pass - Divert Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 302: Carrier Did Not Pass - Divert Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 311: Entry Gate Pass Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 311: Entry Gate Pass Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 312: BCR Pass Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 312: BCR Pass Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 317: Routine Input Gate Pass Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 317: Routine Input Gate Pass Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 318: Priority Input Gate Pass Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 318: Priority Input Gate Pass Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 325: Exit Divert Pass Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 325: Exit Divert Pass Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 326: Piston Pass Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 326: Piston Pass Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS1: Error 327: Exit To Track Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS1: Error 327: Exit To Track Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 113' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 113' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS2' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS2' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 301' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 301' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 302' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 302' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 311' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 311' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 312' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 312' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 317' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 317' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 318' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 318' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 325' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 325' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 326' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 326' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS2: Error 327' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS2: Error 327' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 113' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 113' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS3' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS3' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 301' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 301' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 302' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 302' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 311' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 311' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 312' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 312' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 317' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 317' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 318' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 318' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 325' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 325' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 326' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 326' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS3: Error 327' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS3: Error 327' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 113' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 113' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS4' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS4' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 301' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 301' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 302' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 302' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 311' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 311' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 312' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 312' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 317' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 317' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 318' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 318' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 325' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 325' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 326' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 326' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS4: Error 327' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS4: Error 327' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 113' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 113' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS5' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS5' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 301' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 301' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 302' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 302' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 311' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 311' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 312' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 312' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 317' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 317' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 318' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 318' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 325' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 325' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 326' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 326' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS5: Error 327' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS5: Error 327' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 113' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 113' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS6' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS6' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 301' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 301' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 302' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 302' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 311' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 311' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 312' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 312' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 317' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 317' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 318' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 318' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 325' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 325' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 326' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 326' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS6: Error 327' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS6: Error 327' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 113' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 113' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS7' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS7' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 301' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 301' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 302' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 302' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 311' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 311' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 312' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 312' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 317' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 317' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 318' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 318' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 325' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 325' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 326' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 326' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS7: Error 327' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS7: Error 327' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 101: Sample Presentation Error' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 101: Sample Presentation Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 102: Sample Queue Error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 102: Sample Queue Error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 103: Carrier Missing' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 103: Carrier Missing' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 113' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 113' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like 'LAS8' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = 'LAS8' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 301' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 301' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 302' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 302' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 311' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 311' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 312' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 312' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 317' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 317' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 318' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 318' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 325' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 325' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 326' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 326' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'LAS8: Error 327' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'LAS8: Error 327' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Aliquoter Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Aliquoter Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Centrifuge 2 Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Centrifuge 2 Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Centrifuge 2 Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Centrifuge 2 Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Centrifuge 2 Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Centrifuge 2 Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Centrifuge Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Centrifuge Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Centrifuge Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Centrifuge Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Centrifuge Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Centrifuge Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Decapper Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Decapper Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Decapper Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Decapper Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Desealer Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Desealer Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Desealer Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Desealer Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 1 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 1 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 2 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 2 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 3 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 3 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 4 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 4 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 5 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 5 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 6 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 6 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 7 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 7 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Instrument 8 Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Instrument 8 Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - IOM Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - IOM Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - IOM Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - IOM Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - IOM Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - IOM Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - IOM Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - IOM Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Resealer Divert Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Resealer Divert Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Resealer Gate' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Resealer Gate' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 4' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 5' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 6' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 7' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Return Gate 8' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Return Gate 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - RIM Divert' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - RIM Divert' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 4' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 5' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 6' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 7' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Sampling Gate 8' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Sampling Gate 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Storage Gate A' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Storage Gate A' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Storage Gate B' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Storage Gate B' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Storage Gate C' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Storage Gate C' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Overdue Carrier - Storage Gate D' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Overdue Carrier - Storage Gate D' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Resealer - Temperature Out of Range' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Resealer - Temperature Out of Range' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Resealer: Error 18: Heater error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Resealer: Error 18: Heater error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Resealer: Error 21: Arm up failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Resealer: Error 21: Arm up failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Resealer: Error 22: Arm down failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Resealer: Error 22: Arm down failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Resealer: Error 39: Analog signal error' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Resealer: Error 39: Analog signal error' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 1' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 2' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 3' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 4' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 5' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 6' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 7' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Presentation Error - Instrument 8' || '%'
where 
    (dd.dt - interval '3' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Presentation Error - Instrument 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 4' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 4' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 5' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 5' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 6' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 6' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 7' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 7' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Sample Queue Error - Instrument 8' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Sample Queue Error - Instrument 8' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage - Temperature Out of Range' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage - Temperature Out of Range' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 21: Moving Fault - Axis 1' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 21: Moving Fault - Axis 1' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 22: Moving Fault - Axis 2' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 22: Moving Fault - Axis 2' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 23: Moving Fault - Axis 3' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 23: Moving Fault - Axis 3' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 34: Axis 1 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 34: Axis 1 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 35: Axis 2 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 35: Axis 2 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 36: Axis 3 Homing Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 36: Axis 3 Homing Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 41: Gripper Open Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 41: Gripper Open Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 42: Gripper Close Fault' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 42: Gripper Close Fault' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 48: Arm Limp' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 48: Arm Limp' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 50: Tube Lost' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 50: Tube Lost' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage Sample Robot: Error 51: Object found in Gripper' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage Sample Robot: Error 51: Object found in Gripper' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage: Error 61: Waste Chute Obstructed' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage: Error 61: Waste Chute Obstructed' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Storage: Error 62: Tube Drop Failure' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Storage: Error 62: Tube Drop Failure' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
bcr1_cte as (
select
    max(ae.productline) as pl, 
    ae.serialnumber as iom_sn, 
    date_trunc('day', ae.timestamp_iso) as dt, 
    count(*) as pat_errcount, 
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
where 
    ae.message like '%BCR%1%' 
and 
    (dd.dt - interval '30' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
group by 
    ae.serialnumber,
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from bcr1_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
bcr23_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    count(*) as pat_errcount,
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Unreadable Sample ID (BCR 2)' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Unreadable Sample ID (BCR 2)' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from bcr23_cte


with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
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
bcr23_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    count(*) as pat_errcount,
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || 'Unreadable Sample ID (BCR 3)' || '%'
where 
    (dd.dt - interval '' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || 'Unreadable Sample ID (BCR 3)' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from bcr23_cte

