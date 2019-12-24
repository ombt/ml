#
# Alinity IA Washzone PX Aspiration
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
common_utils_path <- file.path(".", "old_common_utils.R")
if ( ! file.exists(common_utils_path)) {
    stop("No 'old_common_utils.R' found")
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
modulesn_query_template <- "
select
    distinct(dxr.moduleserialnumber) as modulesn
from
    dx.dx_205_alinity_i_result dxr
where
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
reliability_query_template <- NA
#
spark_load_data <- function(db_conn,
                            param_sets, 
                            options,
                            test_period)
{
#     library(DBI)
#     #
#     results_tbl <- "dx_205_alinity_i_result"
#     results_uri_template <- "s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/205-alinity-i/Result/transaction_date=<START_DATE>"
#     results_uri <- query_subs(results_uri_template, test_period, "VALUE")
#     #
#     read_in <- spark_read_parquet(db_conn, 
#                                   results_tbl, 
#                                   results_uri)
}
#
#####################################################################
#
# start algorithm
#
main(1, 
     flagged_query_template, 
     modulesn_query_template, 
     reliability_query_template, 
     "205",
     "spark")
#
q(status=0)
