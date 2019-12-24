select
    eval.moduleserialnumber,
    eval.ratiodisptl,
    eval.numtotaldisp
from (
    select
        w.moduleserialnumber,
        count(w.emptycount) as numtotaldisp,
       (cast (sum(case when (w.emptycount - w.emptytolerance) <= 500 
                 then 1 
                 else 0 
                 end) as double)) / (cast (count(w.emptycount) as double))
            as ratiodisptl
    from
        dx.dx_205_wamdata w
    where
        date_parse('02/01/2019 00:00:00', 
                   '%m/%d/%Y %T') <= w.datetimestamplocal
    and 
        w.datetimestamplocal < date_parse('02/02/2019 00:00:00', 
                                          '%m/%d/%Y %T') 
    and 
        w.washzone = 1
    and 
        w.position = 1
    group by
        w.moduleserialnumber
    ) eval
where
    eval.ratiodisptl >= 0.1
and 
    eval.numtotaldisp >= 60 
