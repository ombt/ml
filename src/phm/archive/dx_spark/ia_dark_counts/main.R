#
# Alinity IA Optics Dark Count
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
    dxr.moduleserialnumber as modulesn,
    date_format(max(dxr.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx.dx_205_result dxr
where
    '<START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<END_DATE>'
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(dxr.moduleserialnumber) like 'AI%'
group by
    dxr.moduleserialnumber
having
    count(dxr.testid) >= <TESTID>
and
    max(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_MAX>
and
    stddev(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_SD>
order by
    dxr.moduleserialnumber"
#
not_flagged_query_template <- "
select
    dxr.moduleserialnumber as modulesn,
    date_format(max(dxr.datetimestamplocal),'%Y%m%d') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx.dx_205_result dxr
where
    '<START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<END_DATE>'
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(dxr.moduleserialnumber) like 'AI%'
group by
    dxr.moduleserialnumber
having not (
    count(dxr.testid) >= <TESTID>
and
    max(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_MAX>
and
    stddev(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_SD>
)
order by
    dxr.moduleserialnumber"
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205", "spark")
# main(1, not_flagged_query_template, FALSE, "205", "spark")
#
q(status=0)
