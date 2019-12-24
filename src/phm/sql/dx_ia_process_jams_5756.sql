select
    eval1.moduleserialnumber,
    eval1.num_retries,
    eval2.num_results
from (
    select
        m.moduleserialnumber,
        count(m.moduleserialnumber) as num_retries
    from
        dx.dx_205_messagehistory m
    where
        date_parse('02/01/2019 00:00:00', 
                   '%m/%d/%Y %T') <= m.datetimestamplocal
    and 
        m.datetimestamplocal < date_parse('02/02/2019 00:00:00', 
                                          '%m/%d/%Y %T') 
    and 
        m.aimcode = 5756
    and 
        m.aimsubcode = 'D298'
    group by
        m.moduleserialnumber
    ) eval1
inner join (
    select
        r.moduleserialnumber,
        count(r.correctedcount) as num_results
    from
        dx.dx_205_result r
    where
        date_parse('02/01/2019 00:00:00', 
                   '%m/%d/%Y %T') <= r.datetimestamplocal
    and 
        r.datetimestamplocal < date_parse('02/02/2019 00:00:00', 
                                          '%m/%d/%Y %T') 
    and 
        r.correctedcount is not null
    group by
        r.moduleserialnumber
    ) eval2
on 
    eval1.moduleserialnumber = eval2.moduleserialnumber
where
    eval2.num_results >= 10
and
    eval1.num_retries >= 8        
order by
    eval1.moduleserialnumber

