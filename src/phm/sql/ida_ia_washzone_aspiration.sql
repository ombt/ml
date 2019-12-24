select
    inner.modulesn,
    inner.ratiodisptl,
    inner.numtotaldisp
from (
    select
        w.modulesn,
        count(w.emptycount) as numtotaldisp,
       (sum(case when (w.emptycount - w.emptytolerance) <= 500 
                 then 1 
                 else 0 
                 end)) / (count(w.emptycount)) 
            as ratiodisptl
    from
        idaqowner.icq_wam w
    where
        to_timestamp('02/01/2019 00:00:00', 
                     'MM/DD/YYYY HH24:MI:SS') <= w.logdate_local
    and 
        w.logdate_local < to_timestamp('02/02/2019 00:00:00', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        w.washzone = 1
    and 
        w.position = 1
    group by
        w.modulesn
    ) inner
where
    inner.ratiodisptl >= 0.1
and 
    inner.numtotaldisp >= 60 
