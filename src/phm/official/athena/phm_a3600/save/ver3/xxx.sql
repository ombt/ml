select 
    ae2.a3600_deviceid,
    upper(trim(ae2.a3600_iom_serial)) as a3600_iom_serial_uc,
    ae2.a3600_productline,
    upper(trim(ae2.a3600_serialnumber)) as a3600_serialnumber_uc,
    date_trunc('day', ae2.timestamp_iso) as flag_date,
    ae2.a3600_nodetype,
    ae2.errorcode,
    ae2.a3600_nodeid,
    ae2.a3600_layoutinstance,
    ac.tubestoday,
    max(ae2.timestamp_iso) as max_compl_date,
    count(ae2.errorcode) as error_count,
    (count(ae2.errorcode) * 100.0 / ac.tubestoday) as error_percentage
from 
    dx.dx_a3600_error ae2,
    dx.dx_a3600_counter ac,
    (
    select 
        ae.a3600_productline,
        ae.a3600_deviceid,
        upper(trim(ae.a3600_iom_serial)) as a3600_iom_serial_uc,
        upper(trim(ae.a3600_serialnumber)) as a3600_serialnumber,
        ae.a3600_nodetype,
        ae.errorcode,
        max (ae.timestamp_iso) as max_compl_date,
        date_trunc('day', min(ae.timestamp_iso)) as min_compl_date
    from 
        dx.dx_a3600_error ae
    where
        '2019-10-20' <= ae.transaction_date
    and
        ae.transaction_date < '2019-10-21'
    and 
        ae.errorcode = '0665'
    group by 
        ae.a3600_productline,
        ae.a3600_deviceid,
        ae.a3600_iom_serial,
        ae.a3600_serialnumber,
        ae.a3600_nodetype,
        ae.errorcode
    order by 
        upper(trim(ae.a3600_iom_serial)),
        ae.a3600_nodetype,
        ae.errorcode
    ) dd
where
    upper(trim(ae2.a3600_serialnumber)) = upper(trim(dd.a3600_serialnumber))
and
    upper(trim(ac.a3600_serialnumber)) = upper(trim(dd.a3600_serialnumber))
and
    ac.a3600_nodetype = ae2.a3600_nodetype
and
    ac.counter_date = date_trunc('day', ae2.timestamp_iso)
and
    ((ac.tubestoday is not null) and (ac.tubestoday > 0))
and
    ac.a3600_nodeid = ae2.a3600_nodeid
and
    ac.a3600_layoutinstance = ae2.a3600_layoutinstance
and
    ae2.timestamp_iso between 
        dd.min_compl_date - interval '2' day + interval '1' day
    and
        dd.max_compl_date
group by 
    ae2.a3600_deviceid,
    ae2.a3600_iom_serial,
    ae2.a3600_productline,
    ae2.a3600_serialnumber,
    date_trunc('day', ae2.timestamp_iso),
    ae2.a3600_nodetype,
    ae2.errorcode,
    ae2.a3600_nodeid,
    ae2.a3600_layoutinstance,
    ac.tubestoday
order by
    upper(trim(ae2.a3600_iom_serial)),
    ae2.a3600_productline,
    upper(trim(ae2.a3600_serialnumber)),
    date_trunc('day', ae2.timestamp_iso)
