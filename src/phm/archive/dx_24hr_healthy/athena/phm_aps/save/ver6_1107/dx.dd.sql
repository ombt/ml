
with aps_devices_dates_cte as (
select 
    upper(trim(ae.serialnumber)) as sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '2019-11-04' <= ae.transaction_date
and
    ae.transaction_date < '2019-11-05'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('2019-11-05', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
)
select * from aps_devices_dates_cte

