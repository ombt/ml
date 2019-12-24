#
# Alinity IA Vacuum Leak
#
#####################################################################
#
# required libraries
#
library(getopt)
library(DBI)
library(RJDBC)
library(dplyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source libs
#
rlibpath <- Sys.getenv("PHM_ROOT")
if (nchar(rlibpath) == 0) {
    stop("PHM_ROOT not defined")
}
source(file.path(rlibpath,"rlib","common_utils.R"))
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
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
                dx.dx_205_vacuumpressuredata vpd 
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
not_flagged_query_template <- "
select
    flagged.moduleserialnumber as modulesn,
    date_format(flagged.flag_date,'%Y%m%d') as flag_date,
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
                dx.dx_205_vacuumpressuredata vpd 
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
        not ( evals.meanpercentdiff >= <VACUUMLEAK_MEANDIFF> )
    group by
        evals.moduleserialnumber
    ) flagged
order by
    flagged.moduleserialnumber"
#
#####################################################################
#
# start algorithm
#
main("dx", 7, flagged_query_template, not_flagged_query_template)
#
q(status=0)
