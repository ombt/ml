#
# Alinity IA Washzone PX Aspiration
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
    eval.moduleserialnumber as modulesn,
    date_format(eval.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval.ratiodisptl,
    eval.numtotaldisp
from (
    select
        w.moduleserialnumber,
        max(w.datetimestamplocal) as flag_date,
        count(w.emptycount) as numtotaldisp,
       (cast (sum(case when (w.emptycount - w.emptytolerance) <= <I_WZASP_THRESHOLD_NUMDISPTCTT>
                 then 1 
                 else 0 
                 end) as double)) / (cast (count(w.emptycount) as double))
            as ratiodisptl
    from
        dx.dx_205_alinity_i_wamdata w
    where
        '<START_DATE>' <= w.transaction_date
    and 
        w.transaction_date < '<END_DATE>'
    and 
        w.washzone = <I_WZASP_THRESHOLD_WZ>
    and 
        w.position = <I_WZASP_THRESHOLD_POS>
    group by
        w.moduleserialnumber
    ) eval
where
    eval.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>
and 
    eval.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>"
#
not_flagged_query_template <- "
select
    eval.moduleserialnumber as modulesn,
    date_format(eval.flag_date,'%Y%m%d') as flag_date,
    eval.ratiodisptl,
    eval.numtotaldisp
from (
    select
        w.moduleserialnumber,
        max(w.datetimestamplocal) as flag_date,
        count(w.emptycount) as numtotaldisp,
       (cast (sum(case when (w.emptycount - w.emptytolerance) <= <I_WZASP_THRESHOLD_NUMDISPTCTT>
                 then 1 
                 else 0 
                 end) as double)) / (cast (count(w.emptycount) as double))
            as ratiodisptl
    from
        dx.dx_205_alinity_i_wamdata w
    where
        '<START_DATE>' <= w.transaction_date
    and 
        w.transaction_date < '<END_DATE>'
    and 
        w.washzone = <I_WZASP_THRESHOLD_WZ>
    and 
        w.position = <I_WZASP_THRESHOLD_POS>
    group by
        w.moduleserialnumber
    ) eval
where not (
    eval.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>
and 
    eval.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>
)"
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205")
# main(1, not_flagged_query_template, FALSE, "205")
#
q(status=0)
