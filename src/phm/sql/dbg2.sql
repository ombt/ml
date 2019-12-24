select
    inner1.modulesn,
    inner1.num_retries,
    inner2.num_results
from (
    select
        m.modulesn,
        count(m.modulesn) as num_retries
    from
        idaqowner.icq_messagehistory m
    where
        m.logdate_local >= trunc(sysdate) - 1
    and 
        m.logdate_local < trunc(sysdate)
    and 
        m.aimcode = '5756'
    and 
        m.aimsubcode = 'D298'
    group by
        m.modulesn
    ) inner1
inner join (
    select
        r.modulesn,
        count(r.correctedcount) as num_results
    from
        idaqowner.icq_results r
    where
        r.logdate_local >= trunc(sysdate) - 1
    and 
        r.logdate_local < trunc(sysdate)
    and 
        r.correctedcount is not null
    group by
        r.modulesn
    ) inner2
on 
    inner1.modulesn = inner2.modulesn
where
    inner2.num_results >= 10
and
    inner1.num_retries >= 8        
order by
    inner1.modulesn
