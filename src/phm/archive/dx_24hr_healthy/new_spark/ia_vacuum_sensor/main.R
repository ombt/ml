#
# Alinity IA Vacuum Sensor
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
    upper(trim(v.moduleserialnumber)) as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_alinity_i_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    upper(trim(v.moduleserialnumber))
having (
    count(v.adcvalue) >= 3
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)
order by
    upper(trim(v.moduleserialnumber))"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
from
    dx.dx_205_alinity_i_result dxr
where
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
chart_data_query_template <- "
select
    upper(trim(v.moduleserialnumber)) as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    avg(v.adcvalue) as chart_data_value,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_alinity_i_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    upper(trim(v.moduleserialnumber))
having 
    count(v.adcvalue) >= 3
order by
    upper(trim(v.moduleserialnumber))"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$MEAN_ADC
    #
    return(flagged_results)
}
#
use_suppression <- TRUE
#
generate_suppression <- function(params, 
                                 rel_db_con, 
                                 options,
                                 test_period)
{
#     query <- "
# select
#     distinct(work.sn) as modulesn
# from (
# SELECT
#   DISTINCT(UPPER(CALCULATEDSN)) AS SN
# FROM
#   TICKETHEADER TH
# INNER JOIN TICKETWORKDONE TWD
#   ON TH.TICKET_SQ = TWD.TICKET_SQ
# WHERE 
#   TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
#     AND TH.BESTPL = '205'
#         AND TWD.WORKDONE_CODE LIKE 'CW%' 
# UNION ALL
# SELECT
#   DISTINCT(UPPER(CALCULATEDSN)) AS SN
# FROM
#   TICKETHEADER TH
# INNER JOIN TICKETPRODUCT TP
#   ON TH.TICKET_SQ = TP.TICKET_SQ
# WHERE 
#   TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
#     AND TH.BESTPL = '205'
#       AND TP.ACTION_TAKEN IN ('N110', 'N120')
#         AND TP.LIST_NUM LIKE ' A-30104916%'  
# ) work
# "
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
        AND TP.LIST_NUM LIKE ' A-30104916%'  
"
    #
    return(exec_query(params, rel_db_con, query, options, test_period))
}
#
spark_load_data <- function(db_conn,
                            param_sets, 
                            options,
                            test_period)
{
# dx.dx_205_alinity_i_result dxr
# dx.dx_205_alinity_i_vacuumpressuredata v
    library(DBI)
    #
    results_tbl <- "dx_205_alinity_i_result"
    results_uri_template <- "s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/205-alinity-i/Result/transaction_date=<START_DATE>"
    results_uri <- query_subs(results_uri_template, test_period, "VALUE")
    #
    read_in <- spark_read_parquet(db_conn, 
                                  results_tbl, 
                                  results_uri)
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
     "205",
     "spark")
#
q(status=0)
