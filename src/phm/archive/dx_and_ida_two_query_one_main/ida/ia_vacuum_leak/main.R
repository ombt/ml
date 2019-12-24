#
# Alinity IA Vacumm Leak
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
    flagged.modulesn,
    to_char(flagged.flag_date, 'YYYYMMDDHH24MISS') as flag_date,
    flagged.days_flagged
from (
    select
        evals.modulesn,
        max(evals.flag_date) as flag_date,
        count(*) as days_flagged
    from (
        select
            raws.modulesn,
            raws.logdate,
            max(raws.logdate_local) as flag_date,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                vpd.modulesn, 
                vpd.logdate_local,
                trunc(vpd.logdate_local) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                idaqowner.icq_vacuumpressuredata vpd 
            where  
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
            and 
                vpd.logdate_local < to_timestamp('<END_DATE>', 
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
        evals.meanpercentdiff >= <VACUUMLEAK_MEANDIFF>
    group by
        evals.modulesn
    ) flagged
where
    flagged.days_flagged >= <VACUUMLEAK_FLAGDAYS>
order by
    flagged.modulesn"
#
not_flagged_query_template <- "
select
    flagged.modulesn,
    to_char(flagged.flag_date, 'YYYYMMDD') as flag_date,
    flagged.days_flagged
from (
    select
        evals.modulesn,
        max(evals.flag_date) as flag_date,
        count(*) as days_flagged
    from (
        select
            raws.modulesn,
            raws.logdate,
            max(raws.logdate_local) as flag_date,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                vpd.modulesn, 
                vpd.logdate_local,
                trunc(vpd.logdate_local) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                idaqowner.icq_vacuumpressuredata vpd 
            where  
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
            and 
                vpd.logdate_local < to_timestamp('<END_DATE>', 
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
        not ( evals.meanpercentdiff >= <VACUUMLEAK_MEANDIFF> )
    group by
        evals.modulesn
    ) flagged
order by
    flagged.modulesn"
#
#####################################################################
#
# start algorithm
#
main("ida", 7, flagged_query_template, not_flagged_query_template)
#
q(status=0)

