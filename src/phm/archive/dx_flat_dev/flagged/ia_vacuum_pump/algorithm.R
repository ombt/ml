#
# Alinity IA Vacuum Pump
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
select
    evals.moduleserialnumber as modulesn,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.moduleserialnumber,
        max(rawdata.datetimestamplocal) as flag_date,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.moduleserialnumber,
            vpd.datetimestamplocal,
            vpd.adcvalue
        from
            dx.dx_205_alinity_i_vacuumpressuredata vpd 
        where  
            '<START_DATE>' <= vpd.transaction_date
        and 
            vpd.transaction_date < '<END_DATE>'
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.moduleserialnumber
    ) evals
where
    evals.min_adcvalue > <VACUUMPUMP_MINADC>
and 
    evals.num_evals >= <VACUUMPUMP_NUMEVALS>
order by
    evals.moduleserialnumber"
#
# number of days to check
#
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- "205"

