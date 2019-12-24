select
    icqr.deviceid as deviceid,
    icqr.modulesn as modulesn,
    count(icqr.testid) as num_testid,
    max(icqr.integrateddarkcount) as max_idc,
    stddev(icqr.integrateddarkcount) as sd_idc
from
    idaqowner.icq_results icqr
where
    to_timestamp('02/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS') <= icqr.logdate_local
and 
    icqr.logdate_local < to_timestamp('03/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
and
    icqr.integrateddarkcount is not null
and
    icqr.integrateddarkcount >= 1
and
    upper(icqr.modulesn) like 'AI%'
group by
    icqr.deviceid,
    icqr.modulesn
having
    count(icqr.testid) >= 10
and
    max(icqr.integrateddarkcount) >= 543
and
    stddev(icqr.integrateddarkcount) >= 110
order by
    icqr.deviceid,
    icqr.modulesn
