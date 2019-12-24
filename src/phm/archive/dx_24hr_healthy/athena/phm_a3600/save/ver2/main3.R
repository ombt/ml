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
#     -- a36err.a3600_countrycode,
#     -- a36err.a3600_customernumber,
#     -- a36err.date_,
#     -- a36err.derived_created_dt,
#     -- a36err.hash_,
#     -- a36err.hr_,
#     -- a36err.laboratory,
#     -- a36err.list_nbr,
#     -- a36err.list_sale_sz,
#     -- a36err.nodeid,
#     -- a36err.nodetype,
#     -- a36err.output_created_dt,
#     -- a36err.parsed_created_dt,
#     -- a36err.pkey,
#     -- a36err.software_version,
#     -- a36err.TIMESTAMP,
#     -- a36err.tresataid__customer,
#     -- a36err.tresataid__customer_a
#
get_error_count_data_query <- function(start_date,
                                       end_date,
                                       v_errorcode1, 
                                       v_nodetype1,
                                       v_samp_id_chk1, 
                                       v_samp_id_chk2, 
                                       v_data_days)
{
    query <- "
select 
    ae2.a3600_deviceid,
    ae2.a3600_iom_serial,
    ae2.a3600_iom_productline,
    ae2.a3600_serialnumber,
    date_trunc('day', ae2.timestamp_iso) as flag_date,
    ae2.a3600_nodetype,
    ae2.errorcode,
    ae2.a3600_nodeid,
    ae2.a3600_layoutinstance,
    ac.tubestoday,
    max(ae2.timestamp_iso) as max_compl_date,
    count(ae2.errorcode) as error_count,
    (count(ae2.errorcode) * 100.0 / ac.tubestoday) as error_percentage
from 
    dx.dx_a3600_error ae2,
    dx.dx_a3600_counter ac,
    (
    select 
        ae.a3600_iom_productline,
        ae.a3600_deviceid,
        ae.a3600_iom_serial,
        ae.a3600_serialnumber,
        ae.a3600_nodetype,
        ae.errorcode,
        max (ae.timestamp_iso) as max_compl_date,
        date_trunc('day', min(ae.timestamp_iso)) as min_compl_date
    from 
        dx.dx_a3600_error ae
    where
        '<START_DATE>' <= ae.transaction_date
    and
        ae.transaction_date < '<END_DATE>'
    and 
        ae.errorcode = '<V_ERRORCODE1>'
    and 
        (('<V_NODETYPE1>' != '%' and ae.a3600_nodetype = '<V_NODETYPE1>') or
         ('<V_NODETYPE1>' = '%' and ae.a3600_nodetype like '<V_NODETYPE1>'))
    group by 
        ae.a3600_iom_productline,
        ae.a3600_deviceid,
        ae.a3600_iom_serial,
        ae.a3600_serialnumber,
        ae.a3600_nodetype,
        ae.errorcode
    order by 
        ae.a3600_iom_serial,
        ae.a3600_nodetype,
        ae.errorcode
    ) dd
where
    upper(trim(ae2.a3600_serialnumber)) = upper(trim(dd.a3600_serialnumber))
and
    upper(trim(ac.a3600_serialnumber)) = upper(trim(dd.a3600_serialnumber))
and
    ac.a3600_nodetype = ae2.a3600_nodetype
and
    ac.counter_date = date_trunc('day', ae2.timestamp_iso)
and
    ac.a3600_nodeid = ae2.a3600_nodeid
and
    ac.a3600_layoutinstance = ae2.a3600_layoutinstance
and
    (('<V_NODETYPE1>' != '%' and ae2.nodetype = '<V_NODETYPE1>')
or
    ('<V_NODETYPE1>' = '%' and ae2.nodetype like '<V_NODETYPE1>'))
and
    ae2.errorcode = '<V_ERRORCODE1>'
and
    coalesce(ae2.sampleid, '<V_SAMP_ID_CHK1>') like coalesce('<V_SAMP_ID_CHK2>', ae2.sampleid)
and
    ae2.timestamp_iso between 
        dd.min_compl_date - interval '<V_DATA_DAYS>' day + interval '1' day
    and
        dd.max_compl_date
group by 
    ae2.a3600_deviceid,
    ae2.a3600_iom_serial,
    ae2.a3600_iom_productline,
    ae2.a3600_serialnumber,
    date_trunc('day', ae2.timestamp_iso),
    ae2.a3600_nodetype,
    ae2.errorcode,
    ae2.a3600_nodeid,
    ae2.a3600_layoutinstance,
    ac.tubestoday
order by
    ae2.a3600_iom_serial,
    ae2.a3600_iom_productline,
    ae2.a3600_serialnumber,
    date_trunc('day', ae2.timestamp_iso)
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
    query <- gsub('<V_ERRORCODE1>', 
                  v_errorcode1, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_NODETYPE1>', 
                  v_nodetype1, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_SAMP_ID_CHK1>',
                  v_samp_id_chk1, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_SAMP_ID_CHK2>',
                  v_samp_id_chk2, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_DATA_DAYS>',
                  v_data_days, 
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
    #
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
    if (algorithm_type == "ERROR_COUNT") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        data_query <- get_error_count_data_query(start_date     = start_date,
                                                 end_date       = end_date,
                                                 v_nodetype1    = module_type,
                                                 v_errorcode1   = pattern_description,
                                                 v_samp_id_chk1 = v_samp_id_chk1, 
                                                 v_samp_id_chk2 = v_samp_id_chk2, 
                                                 v_data_days    = threshold_data_days)
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
            print(sprintf("No records found. Skipping %s", 
                          params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]))
            return(empty_results())
        }
        names(data_results) <- toupper(names(data_results))
        write_data(data_results, "data.csv")
        write_data(data_results, "all_data.csv", TRUE)
        #
        curr_day          <- NA
        prev_day          <- NA
        curr_day_tstamp   <- NA
        prev_day_tstamp   <- NA
        consecutive_days  <- TRUE
        curr_day_errcount <- 0
        flagging_days     <- 0
        flag              <- 'no'
        ihn_value         <- NA
        v_insert_count    <- 0
        total_error_count <- 0
        #
        current_sn = " "
        #
        for (idr in 1:nrow(data_results)) {
            dr_record <- data_results[idr, ]
            #
            write_data(dr_record, "error_count_data.csv")
            write_data(dr_record, "all_data.csv", TRUE)
            #
            if (current_sn != dr_record[1,"A3600_SERIALNUMBER"])
            {
                curr_day          <- NA
                prev_day          <- NA
                curr_day_tstamp   <- NA
                prev_day_tstamp   <- NA
                consecutive_days  <- TRUE
                curr_day_errcount <- 0
                flagging_days     <- 0
                flag              <- 'no'
                ihn_value         <- NA
                v_insert_count    <- 0
                total_error_count <- 0
            }
            current_sn = dr_record[1,"A3600_SERIALNUMBER"]
            #
            flag               <- 'no'
            ihn_value          <- NA
            v_flagged_pl       <- NA
            v_flagged_exp_code <- NA
            #
            curr_day <- dr_record[1,"FLAG_DATE"]
            curr_day_tstamp <- as.POSIXct(curr_day, format="%Y-%m-%d %H:%M:%OS")
            curr_day_errcount <- dr_record[1,"ERROR_COUNT"]
            #
            if (( ! is.na(prev_day)) && (curr_day_tstamp != (prev_day_tstamp + 1))) {
                consecutive_days <- FALSE
            }
            if (consecutive_days && (curr_day_errcount >= threshold_number)) {
                flagging_days <- flagging_days + 1
                prev_day <- curr_day
                prev_day_tstamp <- curr_day_tstamp
            }
            if (curr_day_errcount < threshold_number) {
                flagging_days <- 0
                prev_day <- NA
                prev_day_tstamp <- NA
                consecutive_days <- TRUE
            }
            if ((flagging_days >= threshold_number_unit) &&
                (curr_day_errcount >= z.threshold_number)) {
                flag <- 'yes'
                ihn_value <- threshold_alert
            }
            if (flag == "yes") {
                 print(sprintf("SN: %s FLAGGED", dr_record[1,"A3600_SERIALNUMBER"]))
            } else {
	         print(sprintf("SN: %s NOT FLAGGED ... YET", dr_record[1,"A3600_SERIALNUMBER"]))
            }
        }
    } else if (algorithm_type == "PERCENTAGE") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
    } else if (algorithm_type == "SD_HIGH_VOLUME") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
    } else if (algorithm_type == "SD_LOW_VOLUME") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
    } else {
        print(sprintf("INFO: Skipping UNKNOWN Algorithm Type: %s", algorithm_type))
    }
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
