select
    evals.modulesn,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.modulesn,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.modulesn,
            vpd.adcvalue
        from
            idaqowner.icq_vacuumpressuredata vpd
        where
            to_timestamp('02/01/2019 00:00:00', 
                         'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
        and 
            vpd.logdate_local < to_timestamp('02/02/2019 00:00:00', 
                                             'MM/DD/YYYY HH24:MI:SS')
        and 
            vpd.vacuumstatename = 'VerifyVacuum'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.modulesn
    ) evals
where
    evals.min_adcvalue > 2160
and 
    evals.num_evals >= 30
order by
    evals.modulesn
