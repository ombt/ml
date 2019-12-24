select
    eval1.modulesn,
    eval1.num_retries,
    eval2.num_results
from (
    select
        m.modulesn,
        count(m.modulesn) as num_retries
    from
        idaqowner.icq_messagehistory m
    where
        to_timestamp('02/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS') <= m.logdate_local
    and 
        m.logdate_local < to_timestamp('02/02/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
    and 
        m.aimcode = '5756'
    and 
        m.aimsubcode = 'D298'
    group by
        m.modulesn
    ) eval1
inner join (
    select
        r.modulesn,
        count(r.correctedcount) as num_results
    from
        idaqowner.icq_results r
    where
        to_timestamp('02/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
    and 
        r.logdate_local < to_timestamp('02/02/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
    and 
        r.correctedcount is not null
    group by
        r.modulesn
    ) eval2
on 
    eval1.modulesn = eval2.modulesn
where
    eval2.num_results >= 10
and
    eval1.num_retries >= 8        
order by
    eval1.modulesn


