#
# Alinity IA Optics Dark Count
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
    upper(trim(dxr.moduleserialnumber)) as modulesn,
    dxr.productline as pl,
    date_format(max(dxr.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx.dx_205_alinity_i_result dxr
where
    '<START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<END_DATE>'
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(trim(dxr.moduleserialnumber)) like 'AI%'
group by
    upper(trim(dxr.moduleserialnumber)),
    dxr.productline
having
    count(dxr.testid) >= <TESTID>
and
    max(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_MAX>
and
    stddev(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_SD>"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn,
    dxr.productline as pl
from
    dx.dx_205_alinity_i_result dxr
where
    dxr.moduleserialnumber is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- TRUE
#
chart_data_query_template <- "
select
    upper(trim(dxr.moduleserialnumber)) as modulesn,
    dxr.productline as pl,
    date_format(max(dxr.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as chart_data_value,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx.dx_205_alinity_i_result dxr
where
    '<START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<END_DATE>'
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(trim(dxr.moduleserialnumber)) like 'AI%'
group by
    upper(trim(dxr.moduleserialnumber)),
    dxr.productline
having
    count(dxr.testid) >= 1"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$MAX_IDC
    #
    return(flagged_results)
}
#
generate_suppression <- function(params, 
                                 rel_db_con, 
                                 options,
                                 test_period)
{
    query <- "
SELECT
  DISTINCT(UPPER(CALCULATEDSN)) AS MODULESN
FROM
  TICKETHEADER TH
INNER JOIN TICKETWORKDONE TWD
  ON TH.TICKET_SQ = TWD.TICKET_SQ
WHERE 
  TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '205'
        AND (TWD.WORKDONE_CODE LIKE 'FA3%' 
             OR TWD.WORKDONE_CODE LIKE 'FAG%'
             OR TWD.WORKDONE_CODE LIKE 'FB3%'
             OR TWD.WORKDONE_CODE LIKE 'FBG%')
"
    #
    return(exec_query(params, rel_db_con, query, options, test_period))
}
#
#####################################################################
#
# start algorithm
#
main(1, 
     flagged_query_template, 
     modulesn_query_template, 
     chart_data_query_template,
     NA)
#
q(status=0)
