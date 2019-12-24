#
# Architect Dark Count Average Exceeded
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
    max(rawdata.max_completion_date) as flag_date,
    max(rawdata.average_dark_count) as max_average_dark_count
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
    rawdata.average_dark_count >= <DARKCOUNT_MAX_AVG>
group by
    rawdata.modulesn,
    rawdata.pl
having
    count(rawdata.modulesn) >= <DARKCOUNT_MAX_DAYS>"
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
#
chart_data_query_template <- "
select 
    rawdata.modulesn,
    rawdata.pl,
    max(rawdata.max_completion_date) as flag_date,
    max(rawdata.average_dark_count) as chart_data_value
from (
    select
        upper(trim(r.architect_moduleserial)) as modulesn,
        r.architect_productline as pl,
        date_trunc('day', r.completiondatetime_iso) as test_completion_date, 
        max(r.completiondatetime_iso) as max_completion_date,
        stddev(r.darkcount) as std_dev_dark_count, 
        avg(r.darkcount) as average_dark_count
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
group by
    rawdata.modulesn,
    rawdata.pl
having
    max(rawdata.average_dark_count) is not null
and
    count(rawdata.modulesn) >= 1"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    options,
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$MAX_AVERAGE_DARK_COUNT
    #
    for (irec in 1:nrow(flagged_results)) {
        modulesn <- flagged_results[irec, "MODULESN"]
        #
        if (grepl("^I1SR", modulesn)) {
             flagged_results[irec, "IHN_LEVEL3_DESC"] <- "I1S DARK COUNT > 250"
        } else if (grepl("^ISR", modulesn)) {
             flagged_results[irec, "IHN_LEVEL3_DESC"] <- "I2S DARK COUNT > 250"
        } else if (grepl("^I20", modulesn)) {
             flagged_results[irec, "IHN_LEVEL3_DESC"] <- "I2S DARK COUNT > 250"
        }
    }
    #
    return(flagged_results)
}
#
#####################################################################
#
# start algorithm
#
main(3, 
     flagged_query_template, 
     modulesn_query_template, 
     chart_data_query_template,
     NA)
#
q(status=0)
