
with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-06-16' <= ae.transaction_date
and
    ae.transaction_date < '2019-06-17'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-06-17', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
),
ac_cte as (
select 
    date_trunc('day', ac.timestamp_iso) as timestamp, 
    upper(trim(ac.serialnumber)) as iom_sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
where 
    ac.id in ('normal','priority','tubes', '1','2','3','4','5','6','7','8')  
and 
    ac.duration in ('YTD')
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
select * from bcr1_cte

