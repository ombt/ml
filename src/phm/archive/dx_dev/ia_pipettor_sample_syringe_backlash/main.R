#
# Alinity IA Pipettor Sample Syringe Backlash
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
    final.moduleserialnumber as modulesn,
    date_format(final.flag_date,'%Y%m%d%H%i%s') as flag_date,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner1.moduleserialnumber,
        max(inner1.datetimestamplocal) as flag_date,
        count(inner1.moduleserialnumber) as num_tests,
        avg(cast (inner1.backlash as double)) as avg_backlash
    from (
        select
            ia.moduleserialnumber,
            ia.datetimestamplocal,
            regexp_extract(ia.activity,'^.*PosDiff: *(\\d+).*$',1) as backlash
        from 
            dx.dx_205_alinity_i_instrumentactivity ia
        where
            '<START_DATE>' <= ia.transaction_date
        and 
            ia.transaction_date < '<END_DATE>'
        and 
            ia.activity like '<I_POSDIFF_THRESHOLD_ACTIVITY>'
        ) inner1
    group by
        inner1.moduleserialnumber
    ) final
where
    final.avg_backlash > <I_POSDIFF_THRESHOLD_MAX>
and 
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>
order by
    final.moduleserialnumber"
#
not_flagged_query_template <- "
select 
    final.moduleserialnumber as modulesn,
    date_format(final.flag_date,'%Y%m%d') as flag_date,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner1.moduleserialnumber,
        max(inner1.datetimestamplocal) as flag_date,
        count(inner1.moduleserialnumber) as num_tests,
        avg(cast (inner1.backlash as double)) as avg_backlash
    from (
        select
            ia.moduleserialnumber,
            ia.datetimestamplocal,
            regexp_extract(ia.activity,'^.*PosDiff: *(\\d+).*$',1) as backlash
        from 
            dx.dx_205_alinity_i_instrumentactivity ia
        where
            '<START_DATE>' <= ia.transaction_date
        and 
            ia.transaction_date < '<END_DATE>'
        and 
            ia.activity like '<I_POSDIFF_THRESHOLD_ACTIVITY>'
        ) inner1
    group by
        inner1.moduleserialnumber
    ) final
where not (
    final.avg_backlash > <I_POSDIFF_THRESHOLD_MAX>
and 
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>
)
order by
    final.moduleserialnumber"
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205")
# main(1, not_flagged_query_template, FALSE, "205")
#
q(status=0)
