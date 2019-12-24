#
# Alinity IA Vacuum Leak
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
select
    flagged.moduleserialnumber as modulesn,
    date_format(flagged.flag_date,'%Y%m%d%H%i%s') as flag_date,
    flagged.days_flagged
from (
    select
        evals.moduleserialnumber,
        max(evals.flag_date) as flag_date,
        count(*) as days_flagged
    from (
        select
            raws.moduleserialnumber,
            raws.logdate,
            max(raws.datetimestamplocal) as flag_date,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                vpd.moduleserialnumber, 
                vpd.datetimestamplocal, 
                date_trunc('day', vpd.datetimestamplocal) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                dx.dx_205_alinity_i_vacuumpressuredata vpd 
            where  
                '<START_DATE>' <= vpd.transaction_date
            and 
                vpd.transaction_date < '<END_DATE>'
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
        evals.meanpercentdiff >= <VACUUMLEAK_MEANDIFF>
    group by
        evals.moduleserialnumber
    ) flagged
where
    flagged.days_flagged >= <VACUUMLEAK_FLAGDAYS>
order by
    flagged.moduleserialnumber"
#
# number of days to check
#
number_of_days <- 7
#
# product line code for output file
#
product_line_code <- "205"

