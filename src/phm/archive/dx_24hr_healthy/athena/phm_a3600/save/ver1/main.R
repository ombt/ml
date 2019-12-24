#
# A3600 algorithm
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
    checkpoint("2019-07-01", checkpointLocation=CHECKPOINT_LOCATION)
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
flagged_query_template <- " "
#
modulesn_query_template <- " "
#
use_suppression <- FALSE
#
chart_data_query_template <- NA
#
empty_df <- data.frame()
#
#####################################################################
#
# post flagged-query R processing
#
post_flagged_processing <- function(results,
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period) 
{
    #
    names(results) <- toupper(names(results))
    #
    return(results)
}
#
#####################################################################
#
get_data_query <- function(start_date,
                           end_date,
                           module,
                           error_code_value
                           v_nodetype1,
                           v_errorcode1,
                           v_samp_id_chk1,
                           v_samp_id_chk2,
                           v_start_date,
                           v_end_date)
{
    query <- "
with phm_ods_a3600_errors as (
select 
    a36err.transaction_date,
    a36err.file_path,
    a36err.errorcode,
    a36err.a3600_nodeid,
    a36err.a3600_nodetype,
    a36err.nodeid,
    a36err.nodetype,
    a36err.a3600_layoutinstance,
    a36err.TIMESTAMP,
    a36err.timestamp_iso,
    a36err.sampleid,
    a36err.moreinfo,
    a36err.\"off-line\",
    a36err.a3600_countrycode,
    a36err.a3600_customernumber,
    a36err.a3600_deviceid,
    a36err.a3600_fileversion,
    a36err.a3600_iom_productline,
    a36err.a3600_iom_serial,
    a36err.a3600_productline,
    a36err.a3600_serialnumber,
    a36err.date_,
    a36err.derived_created_dt,
    a36err.duplicate,
    a36err.hash_,
    a36err.hr_,
    a36err.laboratory,
    a36err.list_nbr,
    a36err.list_sale_sz,
    a36err.output_created_dt,
    a36err.parsed_created_dt,
    a36err.pkey,
    a36err.software_version,
    a36err.system_id,
    a36err.tresataid__customer,
    a36err.tresataid__customer_a,
    null as dummy
from
    dx.dx_a3600_error a36err
where 
    '<START_DATE>' <= a36err.transaction_date
and
    a36err.transaction_date < '<END_DATE>'
order by 
    a36err.transaction_date
)
select 
    dd.a3600_deviceid,
    dd.a3600_serialnumber,
    dd.a3600_productline,
    dd.a3600_iom_serial,
    date_trunc('day', min(ae2.timestamp_iso)) as flag_date,
    dd.a3600_nodetype,
    dd..errorcode,
    ae2.nodeid,
    ae2.instanceid,
    ae2.a3600_layoutinstance,
    ac.tubes_today,
    max(ae2.timestamp_iso) as max_compl_date,
    count(ae2.errorcode) as error_count,
    trunc((count(ae2.errorcode)*100/ac.tubes_today), 2) as error_percentage
from 
    phm_ods_a3600_errors ae2,
    dx.dx_a3600_counter ac,
    (
    select 
        ae1.a3600_productline,
        ae1.a3600_deviceid,
        ae1.a3600_serialnumber,
        ae1.a3600_iom_serial,
        ae1.a3600_nodetype,
        ae1.errorcode,
        max(ae1.timestamp_iso) as max_compl_date,
        date_trunc('day', min(ae1.timestamp_iso)) as min_compl_date
    from 
        phm_ods_a3600_errors ae1
    where
        date_parse('<START_DATE>', '%Y-%m-%d') <= ae1.timestamp_iso
    and 
        ae1.timestamp_iso < date_parse('<END_DATE>', '%Y-%m-%d')
    and 
        ae1.errorcode = '<ERROR_CODE_VALUE>'
    and 
        ((('<MODULE>' != '%') and 
          (ae1.a3600_nodetype = '<MODULE>')) or 
         (('<MODULE>' = '%') and 
          (ae1.a3600_nodetype like '<MODULE>')))
    group by 
        ae1.a3600_productline,
        ae1.a3600_deviceid,
        ae1.a3600_serialnumber,
        ae1.a3600_iom_serial,
        ae1.a3600_nodetype,
        ae1.errorcode
    order by 
        ae1.a3600_serialnumber,
        ae1.a3600_nodetype,
        ae1.errorcode
    ) devices_and_dates dd
where
    ac.nodetype = ae2.nodetype
and 
    ac.counter_date = trunc(ae2.timestamp_iso)
and 
    ac.nodeid = ae2.nodeid
and 
    ac.instanceid = ae2.instanceid
and 
    ac.tubes_today <> 0
and 
    ((('<V_NODETYPE1>' != '%') and (ae2.nodetype = '<V_NODETYPE1>')) or
     (('<V_NODETYPE1>' = '%') and (ae2.nodetype like '<V_NODETYPE1>')))
and 
    ae2.errorcode = '<V_ERRORCODE1>'
and 
    coalesce(ae2.sampleid, '<V_SAMP_ID_CHK1>') like coalesce('<V_SAMP_ID_CHK2>', ae2.sampleid)
and 
    dd.min_compl_date <= (ae2.timestamp_is - <V_DATA_DAYS> + 1)
and 
    ae2.timestamp_iso < dd.max_compl_date
group by 
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc(ae2/timestamp_iso),
    ae2.nodetype,
    ae2.errorcode,
    ae2.nodeid,
    ae2.instanceid,
    ac.tubes_today
"
    #
    query <- gsub('<START_DATE>', 
                  start_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<END_DATE>', 
                  end_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<MODULE>', 
                  module, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<ERROR_CODE_VALUE>', 
                  error_code_value, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_NODETYPE1>', 
                  v_nodetype1,
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_ERRORCODE1>', 
                  v_errorcode1,
                  query, 
                  fixed=TRUE)
    query <- gsub('<ERROR_CODE_VALUE>', 
                  v_samp_id_chk1,
                  query, 
                  fixed=TRUE)
    query <- gsub('<ERROR_CODE_VALUE>', 
                  v_samp_id_chk2,
                  query, 
                  fixed=TRUE)
    query <- gsub('<ERROR_CODE_VALUE>', 
                  v_start_date,
                  query, 
                  fixed=TRUE)
    query <- gsub('<ERROR_CODE_VALUE>', 
                  v_end_date,
                  query, 
                  fixed=TRUE)
    #
    return(query)
}
#
#####################################################################
#
# run algorithm for a set of parameters
#
run_algorithm <- function(params, 
                          db_conn, 
                          flagged_query_template, 
                          modulesn_query_template,
                          chart_data_query_template,
                          options, 
                          test_period)
{
    #
    # set patterns for any errors
    #
    errors$phm_patterns_sk(unique(params[ , "PHM_PATTERNS_SK_DUP"])[1])
    #
    # easy to access parameters if we assign row names
    #
    rownames(params) <- params[,"PARAMETER_NAME"]
    #
    print(params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"])
    #
    # get all the parameter values
    #
    start_date      <- test_period["START_DATE", "VALUE"]
    end_date        <- test_period["END_DATE", "VALUE"]
    phm_patterns_sk <- unique(params[ , "PHM_PATTERNS_SK_DUP"])[1]
    #
    module                <- params["MODULE", "PARAMETER_VALUE"]
    error_code_value      <- params["ERROR_CODE_VALUE", "PARAMETER_VALUE"]
    ihn_level3_desc       <- params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]
    algorithm_type        <- params["ALGORITHM_TYPE", "PARAMETER_VALUE"]
    error_count           <- params["ERROR_COUNT", "PARAMETER_VALUE"]
    threshold_description <- params["THRESHOLD_DESCRIPTION", "PARAMETER_VALUE"]
    thresholds_days       <- params["THRESHOLDS_DAYS", "PARAMETER_VALUE"]
    threshold_data_days   <- params["THRESHOLD_DATA_DAYS", "PARAMETER_VALUE"]
    #
    # map to the variables used in the orginal IDA algorithm.
    #
    module_type           <- module
    pattern_description   <- error_code_value
    threshold_alert       <- ihn_level3_desc
    threshold_number      <- error_count
    threshold_number_desc <- threshold_description
    threshold_number_unit <- thresholds_days
    threshold_data_days   <- threshold_data_days
    #
    if (((pattern_description == "0405") && (module_type == "IOM")) ||
        ((pattern_description == "0605") && (module_type == "CM"))) {
        v_samp_id_chk1 <- "NULL"
        v_samp_id_chk2 <- "amp;U__"
    } else if ((pattern_description == "5015") && (module_type == "ISR")) {
        v_samp_id_chk1 <- "NULL"
        v_samp_id_chk2 <- "%"
    } else {
        v_samp_id_chk1 <- " "
        v_samp_id_chk2 <- "%"
    }
    write_data(params, "params.csv")
    write_data(params, "all_data.csv", TRUE)
    #
    data_query <- get_data_query(start_date = start_date,
                                 end_date = end_date,
                                 module = module,
                                 error_code_value = error_code_value)
    #
    data_results <- exec_query(params,
                               db_conn, 
                               data_query,
                               options, 
                               test_period)
    #
    if (errors$occurred()) {
        return(empty_results())
    }
    else if (nrow(data_results) <= 0) {
        return(empty_results())
    }
    write_data(data_results, "data.csv")
    write_data(data_results, "all_data.csv", TRUE)
    #
    return(empty_results())
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
     NA,
    "dx")
#
q(status=0)
