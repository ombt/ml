select
    evals.moduleserialnumber,
    evals.mean_pwmvalue
from (
    select
        i.moduleserialnumber,
        i.itvmechanismname,
        avg(i.pwmvalue) as mean_pwmvalue
    from 
        dx.dx_205_itvdata i
    where
        date_parse('02/01/2019 00:00:00', 
                   '%m/%d/%Y %T') <= i.datetimestamplocal
    and 
        i.datetimestamplocal < date_parse('02/02/2019 00:00:00', 
                                          '%m/%d/%Y %T') 
    and 
        i.actualspeed != 600
    and 
        i.requestedspeed = 1502
    and 
        i.itvmechanismname = 'Reagent1Itv1Mechanism'
    group by
        i.moduleserialnumber,
        i.itvmechanismname
    ) evals
where
    evals.mean_pwmvalue >= 725
order by
    upper(evals.moduleserialnumber)

