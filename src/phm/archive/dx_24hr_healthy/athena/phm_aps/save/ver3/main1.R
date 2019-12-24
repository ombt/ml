#
# APS algorithm
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
print(sprintf("INFO: Working directory: %s", work_dir))
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
    print("INFO: CHECKPOINT_LOCATION is not defined. Skipping.")
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
get_devices_dates_query <- function(start_date, end_date)
{
    query <- "
select 
    upper(trim(ae.serialnumber)) as sn,
    min(date_trunc('day', ae.timestamp_iso)) as dt,
    max(ae.timestamp_iso) as dt_max
from 
    dx.dx_aps_error ae
where
    '<START_DATE>' <= ae.transaction_date
and
    ae.transaction_date < '<END_DATE>'
and
    date_trunc('day', ae.timestamp_iso) < date_parse('<END_DATE>', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by
    upper(trim(ae.serialnumber)),
    min(date_trunc('day', ae.timestamp_iso))
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
    #
    return(query)
}
#
get_aps_counters_query <- function(sn, dt, dt_max)
{
    query <- "
select 
    date_trunc('day', ac.timestamp_iso) as dt, 
    upper(trim(ac.serialnumber)) as sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value
from 
    dx.dx_aps_counter ac
where 
    upper(trim(ac.serialnumber)) = '<SN>'
and 
    date_trunc('day', ac.timestamp_iso) between 
        date_parse('<DT>', '%Y-%m-%d %T')
    and 
        date_parse('<DT_MAX>', '%Y-%m-%d %T') + interval '1' day
and 
    ac.id in ('normal','priority','tubes',
           '1','2','3','4','5','6','7','8')  
and 
    ac.duration in ('YTD')
and 
    ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso), 
    ac.serialnumber, 
    ac.duration, 
    ac.description, 
    ac.id
"
    #
    query <- gsub('<SN>', 
                  sn, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<DT>', 
                  gsub("\\.[0-9][0-9]*","",dt),
                  query, 
                  fixed=TRUE)
    query <- gsub('<DT_MAX>', 
                  gsub("\\.[0-9][0-9]*","",dt_max),
                  query, 
                  fixed=TRUE)
    #
    return(query)
}
#
get_aps_counters_query_2 <- function(start_date, end_date)
{
    query <- "
select 
    date_trunc('day', ac.timestamp_iso) as dt, 
    upper(trim(ac.serialnumber)) as sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value
from 
    dx.dx_aps_counter ac,
    (
    select 
        upper(trim(ae.serialnumber)) as sn,
        min(date_trunc('day', ae.timestamp_iso)) as dt,
        max(ae.timestamp_iso) as dt_max
    from 
        dx.dx_aps_error ae
    where
        '<START_DATE>' <= ae.transaction_date
    and
        ae.transaction_date < '<END_DATE>'
    and
        date_trunc('day', ae.timestamp_iso) < date_parse('<END_DATE>', '%Y-%m-%d')
    group by
        upper(trim(ae.serialnumber))
    order by
        upper(trim(ae.serialnumber)),
        min(date_trunc('day', ae.timestamp_iso))
    ) dd
where 
    upper(trim(ac.serialnumber)) = dd.sn
and 
    date_trunc('day', ac.timestamp_iso) between 
        dd.dt 
    and 
        dd.dt_max
and 
    ac.id in ('normal','priority','tubes',
           '1','2','3','4','5','6','7','8')  
and 
    ac.duration in ('YTD')
and 
    ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso), 
    ac.serialnumber, 
    ac.duration, 
    ac.description, 
    ac.id
"
    #
    query <- gsub('<SN>', 
                  sn, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<DT>', 
                  gsub("\\.[0-9][0-9]*","",dt),
                  query, 
                  fixed=TRUE)
    query <- gsub('<DT_MAX>', 
                  gsub("\\.[0-9][0-9]*","",dt_max),
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
dd_results <- data.frame()
#
all_apsc_results <- data.frame()
first_all_apsc_results <- TRUE
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
    start_date <- test_period["START_DATE", "VALUE"]
    end_date   <- test_period["END_DATE", "VALUE"]
    #
    phm_patterns_sk <- unique(params[ , "PHM_PATTERNS_SK_DUP"])[1]
    #
    threshold_type        <- params["THRESHOLD_TYPE", "PARAMETER_VALUE"]
    ihn_level3_desc       <- params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]
    algorithm_type        <- params["ALGORITHM_TYPE", "PARAMETER_VALUE"]
    error_count           <- params["ERROR_COUNT", "PARAMETER_VALUE"]
    error_code_reg_expr   <- params["ERROR_CODE_REG_EXPR", "PARAMETER_VALUE"]
    threshold_description <- params["THRESHOLD_DESCRIPTION", "PARAMETER_VALUE"]
    thresholds_days       <- params["THRESHOLDS_DAYS", "PARAMETER_VALUE"]
    threshold_data_days   <- params["THRESHOLD_DATA_DAYS", "PARAMETER_VALUE"]
    #
    write_data(params, "debug_params.csv")
    write_data(params, "debug_all_data.csv", TRUE)
    #
    # run this query only once since the data never change. store the results
    # in a global. that is why <<- was used for assgnment.
    #
    if (nrow(dd_results) == 0) {
        dd_query <- get_devices_dates_query(start_date, end_date)
        #
        dd_results <<- exec_query(params,
                                  db_conn, 
                                  dd_query,
                                  options, 
                                  test_period)
        #
        if (errors$occurred()) {
            return(empty_results())
        } else if (nrow(dd_results) <= 0) {
            stop(sprintf("INFO: No Devices Dates records found: (%s,%s)",
                         start_date, 
                         end_date))
        }
        #
        names(dd_results) <<- toupper(names(dd_results))
        write_data(dd_results, "debug_devices_dates.csv")
        write_data(dd_results, "debug_all_data.csv", TRUE)
        #
        print(sprintf("INFO: %d Devices Dates records found: (%s,%s)",
                      nrow(dd_results),
                      start_date, 
                      end_date))
        #
        # get the counter data for each SN.
        #
        for (iddr in 1:nrow(dd_results)) {
            dd_record <- dd_results[iddr, ]
            if (nrow(dd_record) <= 0) {
                next
            }
            #
            dd_sn     <- dd_record[1,"SN"]
            dd_dt     <- dd_record[1,"DT"]
            dd_dt_max <- dd_record[1,"DT_MAX"]
            #
            apsc_query <- get_aps_counters_query(dd_sn, 
                                                 dd_dt, 
                                                 dd_dt_max)
            #
            apsc_results <<- exec_query(params,
                                        db_conn, 
                                        apsc_query,
                                        options, 
                                        test_period)
            #
            if (errors$occurred()) {
                return(empty_results())
            } else if (nrow(dd_results) <= 0) {
                print(sprintf("INFO: No counter data records found for: (%s,%s,%s)",
                              dd_sn, 
                              dd_dt, 
                              dd_dt_max))
                next
            }
            #
            names(apsc_results) <<- toupper(names(apsc_results))
            write_data(apsc_results, "debug_aps_counter_data.csv")
            write_data(apsc_results, "debug_all_data.csv", TRUE)
            #
            if (first_all_apsc_results) {
                all_apsc_results <- rbind(all_apsc_results,
                                          apsc_results,
                                          stringsAsFactors=FALSE)
            } else {
                #
                # these are globals, hence the '<<-'
                #
                first_all_apsc_results <<- FALSE
                all_apsc_results <<- apsc_results
            }
        }
        #
        write_data(all_apsc_results, "debug_all_aps_counter_data.csv")
        print(sprintf("INFO: %d total APS Counter records found",
                      nrow(all_apsc_results)))
    }
    #
    final_results <- empty_results()
    #
    #
    return(final_results)
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
