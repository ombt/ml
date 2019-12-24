select
    flagged.modulesn,
    flagged.days_flagged
from (
    select
        evals.modulesn,
        count(*) as days_flagged
    from (
        select
            raws.modulesn,
            raws.logdate,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                vpd.modulesn, 
                trunc(vpd.logdate_local) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                idaqowner.icq_vacuumpressuredata vpd 
            where  
                to_timestamp('02/01/2019 00:00:00', 
                             'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
            and 
                vpd.logdate_local < to_timestamp('02/08/2019 00:00:00', 
                                                 'MM/DD/YYYY HH24:MI:SS')
            and  
                vpd.vacuumstatename = 'ConcludeLeakTest'
            and
                vpd.adcvalueleaktest is not null
            and
                vpd.adcvalue is not null
            and
                vpd.adcvalue <> 0
            ) raws
        group by
            raws.modulesn,
            raws.logdate
        ) evals
    where
        evals.meanpercentdiff >= 7
    group by
        evals.modulesn
    ) flagged
where
    flagged.days_flagged >= 2
order by
    flagged.modulesn

