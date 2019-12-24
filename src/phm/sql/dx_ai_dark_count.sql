select
    dxr.deviceid as deviceid,
    dxr.moduleserialnumber as moduleserialnumber,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx.dx_205_result dxr
where
    date_parse('03/08/2019 00:00:00', '%m/%d/%Y %T') <= dxr.datetimestamplocal
and 
    dxr.datetimestamplocal < date_parse('03/09/2019 00:00:00', '%m/%d/%Y %T') 
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= 1
and
    upper(dxr.moduleserialnumber) like 'AI%'
group by
    dxr.deviceid,
    dxr.moduleserialnumber
having
    count(dxr.testid) >= 10
and
    max(dxr.integrateddarkcount) >= 543
and
    stddev(dxr.integrateddarkcount) >= 110
order by
    dxr.deviceid,
    dxr.moduleserialnumber
