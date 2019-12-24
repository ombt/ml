select
    evals.moduleserialnumber,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.moduleserialnumber,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.moduleserialnumber,
            vpd.adcvalue
        from
            dx.dx_205_vacuumpressuredata vpd 
        where  
            date_parse('02/01/2019 00:00:00', 
                       '%m/%d/%Y %T') <= vpd.datetimestamplocal
        and 
            vpd.datetimestamplocal < date_parse('02/02/2019 00:00:00',
                                                '%m/%d/%Y %T') 
        and 
            vpd.vacuumstatename = 'VerifyVacuum'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.moduleserialnumber
    ) evals
where
    evals.min_adcvalue > 2160
and 
    evals.num_evals >= 30
order by
    evals.moduleserialnumber
