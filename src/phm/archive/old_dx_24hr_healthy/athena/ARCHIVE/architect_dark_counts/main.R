#
# Architect Dark Count Average and Standard Deviation Exceeded
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
# required libraries
#
library(checkpoint)
#
CHECKPOINT_LOCATION <- Sys.getenv("CHECKPOINT_LOCATION")
if (nchar(CHECKPOINT_LOCATION) > 0) {
    checkpoint("2019-07-01", 
               checkpointLocation=CHECKPOINT_LOCATION)
} else {
    print("CHECKPOINT_LOCATION is not defined. Skipping.")
}
#
library(getopt)
library(DBI)
library(RJDBC)
library(odbc)
library(dplyr)
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source libs
#
common_utils_path <- file.path(".", "adhoc_common_utils.R")
if ( ! file.exists(common_utils_path)) {
    stop("No 'adhoc_common_utils.R' found")
}
source(common_utils_path)
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select 
    rawdata.modulesn,
    rawdata.pl,
    max(rawdata.max_completion_date) as flag_date
from (
    select
        upper(trim(r.architect_moduleserial)) as modulesn,
        r.architect_productline as pl,
        date_trunc('day', r.completiondatetime_iso) as test_completion_date, 
        max(r.completiondatetime_iso) as max_completion_date,
        stddev(r.darkcount) as std_dev_dark_count, 
        avg (r.darkcount) as average_dark_count
    from
        dx.dx_architect_results r
    where
        r.darkcount is not null 
    and
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    group by 
        upper(trim(r.architect_moduleserial)),
        r.architect_productline,
        date_trunc('day', r.completiondatetime_iso)
    ) rawdata
where
    rawdata.std_dev_dark_count >= <MAX_SD>
and
    rawdata.average_dark_count >= <MAX_AVG>
group by
    rawdata.modulesn,
    rawdata.pl
having
    count(rawdata.modulesn) >= <MAX_DAYS>"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.architect_moduleserial))) as modulesn
from
    dx.dx_architect_results dxr
where
    dxr.architect_moduleserial is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
chart_data_query_template <- NA
#
#####################################################################
#
# start algorithm
#
main(7, 
     flagged_query_template, 
     modulesn_query_template, 
     chart_data_query_template,
     NA)
#
q(status=0)
