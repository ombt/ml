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
common_utils_path <- file.path(".", "common_utils.R")
if ( ! file.exists(common_utils_path)) {
    if (nchar(Sys.getenv("DEV_ROOT")) == 0) {
        stop("No 'common_utils.R' found")
    }
    common_utils_path <- file.path(Sys.getenv("DEV_ROOT"),
                                   "rlib",
                                   "common_utils.R")
    if ( ! file.exists(common_utils_path)) {
        stop("No DEV_ROOT 'common_utils.R' found")
    }
}
source(common_utils_path)
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
main(7, flagged_query_template, TRUE, "205", "ida")
# main(7, not_flagged_query_template, FALSE, "205", "ida")
#
q(status=0)

