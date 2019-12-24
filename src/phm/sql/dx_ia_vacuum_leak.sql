select
    flagged.moduleserialnumber,
    flagged.days_flagged
from (
    select
        evals.moduleserialnumber,
        count(*) as days_flagged
    from (
        select
            raws.moduleserialnumber,
            raws.logdate,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                vpd.moduleserialnumber, 
                date_trunc('day', vpd.datetimestamplocal) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                dx.dx_205_vacuumpressuredata vpd 
            where  
                date_parse('02/01/2019 00:00:00', 
                           '%m/%d/%Y %T') <= vpd.datetimestamplocal
            and 
                vpd.datetimestamplocal < date_parse('02/08/2019 00:00:00',
                                                    '%m/%d/%Y %T') 
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
            raws.moduleserialnumber,
            raws.logdate
        ) evals
    where
        evals.meanpercentdiff >= 7
    group by
        evals.moduleserialnumber
    ) flagged
where
    flagged.days_flagged >= 2
order by
    flagged.moduleserialnumber

