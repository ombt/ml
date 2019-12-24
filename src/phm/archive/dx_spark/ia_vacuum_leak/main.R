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
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# set working directory
#
args <- commandArgs()
scripts <- args[grepl("--file=", args)]
script_paths <- sub("^.*--file=(.*)$", "\\1", scripts)
work_dir <- dirname(script_paths[1])
#
print(sprintf("Working directory: %s", work_dir))
setwd(work_dir)
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
main(7, flagged_query_template, TRUE, "205", "spark")
# main(7, not_flagged_query_template, FALSE, "205", "spark")
#
q(status=0)
