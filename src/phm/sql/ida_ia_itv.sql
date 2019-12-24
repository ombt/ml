select
    evals.modulesn,
    evals.mean_pwmvalue
from (
    select
        i.modulesn,
        i.itvmechanismname,
        avg(i.pwmvalue) as mean_pwmvalue
    from 
        idaqowner.icq_itvdata i
    where
        to_timestamp('02/01/2019 00:00:00', 
                     'MM/DD/YYYY HH24:MI:SS') <= i.logdate_local
    and 
        i.logdate_local < to_timestamp('03/01/2019 00:00:00', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        i.actualspeed != 600
    and 
        i.requestedspeed = 1502
    and 
        i.itvmechanismname = 'Reagent1Itv1Mechanism'
    group by
        i.modulesn,
        i.itvmechanismname
    ) evals
where
    evals.mean_pwmvalue >= 725
order by
    evals.modulesn

