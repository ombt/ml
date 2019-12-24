#
# Alinity IA Pipettor Sample Syringe Backlash
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
            upper(trim(ia.moduleserialnumber)) as moduleserialnumber,
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
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
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
    final.moduleserialnumber as modulesn,
    date_format(final.flag_date,'%Y%m%d%H%i%s') as flag_date,
    final.num_tests,
    final.avg_backlash as chart_data_value
from (
    select
        inner1.moduleserialnumber,
        max(inner1.datetimestamplocal) as flag_date,
        count(inner1.moduleserialnumber) as num_tests,
        avg(cast (inner1.backlash as double)) as avg_backlash
    from (
        select
            upper(trim(ia.moduleserialnumber)) as moduleserialnumber,
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
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>
order by
    final.moduleserialnumber"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$AVG_BACKLASH
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
INNER JOIN TICKETPRODUCT TP
  ON TH.TICKET_SQ = TP.TICKET_SQ
WHERE 
  TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '205'
      AND TP.ACTION_TAKEN IN ('N110', 'N120')
        AND TP.LIST_NUM LIKE '7-77650%'
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
     "205")
#
q(status=0)
