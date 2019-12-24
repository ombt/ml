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
# primary data queries
#
get_data_query <- function(start_date,
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
    upper(trim(ae2.a3600_iom_serial)) as a3600_iom_serial_uc,
    ae2.a3600_iom_productline,
    upper(trim(ae2.a3600_serialnumber)) as a3600_serialnumber_uc,
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
        upper(trim(ae.a3600_iom_serial)) as a3600_iom_serial_uc,
        upper(trim(ae.a3600_serialnumber)) as a3600_serialnumber,
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
        upper(trim(ae.a3600_iom_serial)),
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
    ((ac.tubestoday is not null) and (ac.tubestoday > 0))
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
    coalesce(ae2.sampleid, <V_SAMP_ID_CHK1>) like coalesce(<V_SAMP_ID_CHK2>, ae2.sampleid)
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
    upper(trim(ae2.a3600_iom_serial)),
    ae2.a3600_iom_productline,
    upper(trim(ae2.a3600_serialnumber)),
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
get_error_count_data_query <- function(start_date,
                                       end_date,
                                       v_errorcode1, 
                                       v_nodetype1,
                                       v_samp_id_chk1, 
                                       v_samp_id_chk2, 
                                       v_data_days)
{
    return(get_data_query(start_date,
                          end_date,
                          v_errorcode1, 
                          v_nodetype1,
                          v_samp_id_chk1, 
                          v_samp_id_chk2, 
                          v_data_days))
}
#
get_percentage_data_query <- function(start_date,
                                      end_date,
                                      v_errorcode1, 
                                      v_nodetype1,
                                      v_samp_id_chk1, 
                                      v_samp_id_chk2, 
                                      v_data_days)
{
    return(get_data_query(start_date,
                          end_date,
                          v_errorcode1, 
                          v_nodetype1,
                          v_samp_id_chk1, 
                          v_samp_id_chk2, 
                          v_data_days))
}
#
get_sd_high_volume_data_query <- function(start_date,
                                         end_date,
                                         v_errorcode1, 
                                         v_nodetype1,
                                         v_samp_id_chk1, 
                                         v_samp_id_chk2, 
                                         v_data_days)
{
    return(get_data_query(start_date,
                          end_date,
                          v_errorcode1, 
                          v_nodetype1,
                          v_samp_id_chk1, 
                          v_samp_id_chk2, 
                          v_data_days))
}
#
get_sd_low_volume_data_query <- function(start_date,
                                         end_date,
                                         v_errorcode1, 
                                         v_nodetype1,
                                         v_samp_id_chk1, 
                                         v_samp_id_chk2, 
                                         v_data_days)
{
    return(get_data_query(start_date,
                          end_date,
                          v_errorcode1, 
                          v_nodetype1,
                          v_samp_id_chk1, 
                          v_samp_id_chk2, 
                          v_data_days))
}
#
#####################################################################
#
# primary data processing routines
#
process_error_count_data <- function(data_results,
                                     phm_patterns_sk,
                                     threshold_alert,
                                     threshold_number,
                                     threshold_number_desc,
                                     threshold_number_unit,
                                     threshold_data_days)
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
    #
    current_sn = " "
    flagged_results <- empty_results()
    #
    for (idr in 1:nrow(data_results)) {
        dr_record <- data_results[idr, ]
        #
        write_data(dr_record, "debug_error_count_data.csv")
        write_data(dr_record, "debug_all_data.csv", TRUE)
        #
        if (nchar(dr_record[1,"A3600_SERIALNUMBER_UC"]) <= 0) {
            next
        }
        if (current_sn != dr_record[1,"A3600_SERIALNUMBER_UC"]) {
            print(sprintf("INFO: Processing SN: %s", dr_record[1,"A3600_SERIALNUMBER_UC"]))
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
        current_sn = dr_record[1,"A3600_SERIALNUMBER_UC"]
        current_pl = dr_record[1,"A3600_IOM_PRODUCTLINE"]
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
        if (( ! is.na(prev_day)) && (curr_day_tstamp != (prev_day_tstamp + 24*60*60))) {
            consecutive_days <- FALSE
        }
        #
        if (consecutive_days && (curr_day_errcount >= threshold_number)) {
            flagging_days <- flagging_days + 1
            prev_day <- curr_day
            prev_day_tstamp <- curr_day_tstamp
        }
        #
        if (curr_day_errcount < threshold_number) {
            flagging_days <- 0
            prev_day <- NA
            prev_day_tstamp <- NA
            consecutive_days <- TRUE
        }
        #
        if ((flagging_days >= threshold_number_unit) &&
            (curr_day_errcount >= threshold_number)) {
            flag <- 'yes'
            ihn_value <- threshold_alert
            #
            flagged_record <- list(PHN_PATTERNS_SK=phm_patterns_sk,
                                   PL=current_pl,
                                   MODULESN=current_sn,
                                   FLAG_DATE=curr_day,
                                   CHART_DATA_VALUE=1,
                                   FLAG_YN="Y",
                                   IHN_LEVEL3_DESC=ihn_value)
            flagged_results <- rbind(flagged_results,
                                     flagged_record,
                                     stringsAsFactors=FALSE)
            #
            print(sprintf("INFO: SN: %s FLAGGED", dr_record[1,"A3600_SERIALNUMBER_UC"]))
        }
    }
    #
    return(flagged_results)
} 
#
process_percentage_data <- function(data_results,
                                    phm_patterns_sk,
                                    threshold_alert,
                                    threshold_number,
                                    threshold_number_desc,
                                    threshold_number_unit,
                                    threshold_data_days)
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
    #
    current_sn = " "
    flagged_results <- empty_results()
    #
    # "PHN_PATTERNS_SK",
    # "PL",
    # "MODULESN",
    # "FLAG_DATE",
    # "CHART_DATA_VALUE",
    # "FLAG_YN",
    # "IHN_LEVEL3_DESC")
    #
    for (idr in 1:nrow(data_results)) {
        dr_record <- data_results[idr, ]
        #
        write_data(dr_record, "debug_percentage_data.csv")
        write_data(dr_record, "debug_all_data.csv", TRUE)
        #
        if (nchar(dr_record[1,"A3600_SERIALNUMBER_UC"]) <= 0) {
            next
        }
        if (current_sn != dr_record[1,"A3600_SERIALNUMBER_UC"]) {
            print(sprintf("INFO: Processing SN: %s", dr_record[1,"A3600_SERIALNUMBER_UC"]))
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
        }
        current_sn = dr_record[1,"A3600_SERIALNUMBER_UC"]
        current_pl = dr_record[1,"A3600_IOM_PRODUCTLINE"]
        #
        flag               <- 'no'
        ihn_value          <- NA
        v_flagged_pl       <- NA
        v_flagged_exp_code <- NA
        #
        curr_day <- dr_record[1,"FLAG_DATE"]
        curr_day_tstamp <- as.POSIXct(curr_day, format="%Y-%m-%d %H:%M:%OS")
        curr_day_errcount <- dr_record[1,"ERROR_PERCENTAGE"]
        #
        if (( ! is.na(prev_day)) && (curr_day_tstamp != (prev_day_tstamp + 24*60*60))) {
            consecutive_days <- FALSE
        }
        #
        if (consecutive_days && (curr_day_errcount >= threshold_number)) {
            flagging_days <- flagging_days + 1
        }
        prev_day <- curr_day
        prev_day_tstamp <- curr_day_tstamp
        #
        if (curr_day_errcount < threshold_number) {
            flagging_days <- 0
            prev_day <- NA
            prev_day_tstamp <- NA
            consecutive_days <- TRUE
        }
        #
        if ((flagging_days >= threshold_number_unit) &&
            (curr_day_errcount >= threshold_number)) {
            flag <- 'yes'
            ihn_value <- threshold_alert
            #
            flagged_record <- list(PHN_PATTERNS_SK=phm_patterns_sk,
                                   PL=current_pl,
                                   MODULESN=current_sn,
                                   FLAG_DATE=curr_day,
                                   CHART_DATA_VALUE=1,
                                   FLAG_YN="Y",
                                   IHN_LEVEL3_DESC=ihn_value)
            flagged_results <- rbind(flagged_results,
                                     flagged_record,
                                     stringsAsFactors=FALSE)
            #
            print(sprintf("INFO: SN: %s FLAGGED", dr_record[1,"A3600_SERIALNUMBER_UC"]))
        }
    }
    #
    return(flagged_results)
} 
#
get_v_req_start_date <- function(db_conn,
                                 options,
                                 start_date,
                                 end_date,
                                 current_sn,
                                 current_nodeid)
{
    query <- "
select
    date_trunc('day', min(ae.timestamp_iso)) as v_req_start_date
from 
    dx.dx_a3600_error ae
where
     ae.a3600_serialnumber = '<MODULE_SN>'
and
     ae.a3600_nodeid = '<NODE_ID>'
and
    '<START_DATE>' <= ae.transaction_date
and
    ae.transaction_date < '<END_DATE>'
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
    query <- gsub('<NODE_ID>', 
                  current_nodeid, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<MODULE_SN>', 
                  current_sn, 
                  query, 
                  fixed=TRUE)
    #
    results <- exec_query(empty_df,
                          db_conn, 
                          query,
                          options, 
                          empty_df)
    #
    if (errors$occurred()) {
        return("")
    } else if (nrow(results) <= 0) {
        print(sprintf("INFO: No records found for V_REQ_START_DATE, %s", current_dn))
        return("")
    }
    names(results) <- toupper(names(results))
    write_data(results, "debug_v_req_start_date.csv")
    write_data(results, "debug_all_data.csv", TRUE)
    #
    return(results[1,"V_REQ_START_DATE"])
}
#
get_low_filter_count <- function(current_sn_data_results, 
                                 current_sn,
                                 threshold_number_unit,
                                 flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    #
    if (rows_to_keep > as.integer(threshold_number_unit)) {
        rows_to_keep <- as.integer(threshold_number_unit)
    }
    results <- results[1:rows_to_keep,]
    #
    filter_count <- abs(mean(results$TUBESTODAY)-sd(results$TUBESTODAY))
    #
    print(sprintf("INFO: get_low_filter_count: sn=%s, fc=%f,%d",
                  current_sn,
                  filter_count,
                  as.integer(filter_count)))
    #
    return(as.integer(filter_count))
}
#
get_low_v_date_30th <- function(current_sn_data_results, 
                                current_sn,
                                threshold_number_unit,
                                flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return("1970-01-01 00:00:00")
    }
    #
    if (rows_to_keep > as.integer(threshold_number_unit)) {
        rows_to_keep <- as.integer(threshold_number_unit)
    }
    results <- results[1:rows_to_keep,]
    #
    v_date_30th <- min(results[,"FLAG_DATE"])
    #
    print(sprintf("INFO: get_low_v_date_30th: sn=%s, v_date_30th=%s",
                  current_sn,
                  v_date_30th))
    #
    return(v_date_30th)
}
#
get_low_today_test_count <- function(current_sn_data_results, 
                                     current_sn,
                                     flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") == flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    today_test_count <- as.integer(results[1,"TUBESTODAY"])
    #
    print(sprintf("INFO: get_low_today_test_count: sn=%s, today_test_count=%d",
                  current_sn,
                  today_test_count))
    #
    return(today_test_count)
}
#
get_low_today_errorpct <- function(current_sn_data_results, 
                                   current_sn,
                                   flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") == flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    today_errorpct <- as.integer(results[1,"ERROR_PERCENTAGE"])
    #
    print(sprintf("INFO: get_low_today_errorpct: sn=%s, today_errorpct=%f",
                  current_sn,
                  today_errorpct))
    #
    return(today_errorpct)
}
#
get_low_v_threshold_limit <- function(current_sn_data_results, 
                                      current_sn,
                                      threshold_number,
                                      flag_date_tstamp,
                                      v_date_30th,
                                      filter_count)
{
    v_date_30th_tstamp <- as.POSIXct(v_date_30th,
                                     format="%Y-%m-%d %H:%M:%OS")
    #
    results <- 
        current_sn_data_results[
            (v_date_30th_tstamp <= as.POSIXct(current_sn_data_results$FLAG_DATE,
                                              format="%Y-%m-%d %H:%M:%OS")) &
            (as.POSIXct(current_sn_data_results$FLAG_DATE,
                        format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp), ]
    #
    number_of_rows <- nrow(results)
    if (number_of_rows <= 1) {
        return(0)
    }
    results <- results[1:number_of_rows,]
    results <- results[results$TUBESTODAY < filter_count,]
    if (nrow(results) <= 1) {
        return(0)
    }
    #
    v_threshold_limit <- as.double(threshold_number)*sd(results$ERROR_PERCENTAGE)+mean(results$ERROR_PERCENTAGE)
    #
    print(sprintf("INFO: get_low_v_threshold_limit: sn=%s, nrow=%d, v_threshold_limit=%f,%d",
                  current_sn,
                  nrow(results),
                  v_threshold_limit,
                  as.integer(v_threshold_limit)))
    #
    return(v_threshold_limit)
}
#
get_low_conseq_count <- function(current_sn_data_results, 
                                 current_sn,
                                 threshold_number,
                                 threshold_number_unit,
                                 v_threshold_limit,
                                 flag_date_tstamp,
                                 filter_count)
{
    results <- 
        current_sn_data_results[(as.POSIXct(current_sn_data_results$FLAG_DATE,
                                            format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp), ]
    if (nrow(results) <= 0) {
        return(0)
    }
    #
    results <- results[results$TUBESTODAY < filter_count,]
    if (nrow(results) <= 0) {
        return(0)
    }
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    #
    if (rows_to_keep > as.integer(threshold_number_unit)) {
        rows_to_keep <- as.integer(threshold_number_unit)
    }
    results <- results[1:rows_to_keep,]
    if (nrow(results) <= 0) {
        return(0)
    }
    #
    results <- results[results$ERROR_PRECENTAGE >= v_threshold_limit,]
    #
    print(sprintf("INFO: get_low_conseq_count: sn=%s, conseq_counts=nrow=%d",
                  current_sn,
                  nrow(results)))
    #
    return(nrow(results))
}
#
process_sd_low_volume_data <- function(db_conn,
                                       options,
                                       start_date,
                                       end_date,
                                       data_results,
                                       phm_patterns_sk,
                                       threshold_alert,
                                       threshold_number,
                                       threshold_number_desc,
                                       threshold_number_unit,
                                       threshold_data_days)
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
    #
    current_sn = " "
    flagged_results <- empty_results()
    #
    for (idr in 1:nrow(data_results)) {
        dr_record <- data_results[idr, ]
        #
        write_data(dr_record, "debug_sd_low_volume_data.csv")
        write_data(dr_record, "debug_all_data.csv", TRUE)
        #
        if (nchar(dr_record[1,"A3600_SERIALNUMBER_UC"]) <= 0) {
            next
        } else if (current_sn == dr_record[1,"A3600_SERIALNUMBER_UC"]) {
            #
            # skip if we have the same serial number.
            #
            next;
        }
        #
        print(sprintf("INFO: Processing SN: %s", dr_record[1,"A3600_SERIALNUMBER_UC"]))
        #
        flag              <- 'no'
        ihn_value         <- NA
        today_test_count  <- 0
        filter_count      <- 0
        today_errorpct    <- 0
        prev_day_errorpct <- 0
        v_threshold_limit <- 100000
        conseq_count      <- 0
        #
        current_sn <- dr_record[1,"A3600_SERIALNUMBER_UC"]
        current_sn_data_results <- 
            data_results[data_results$A3600_SERIALNUMBER_UC==current_sn,]
        current_pl <- dr_record[1,"A3600_IOM_PRODUCTLINE"]
        write_data(current_sn_data_results, "debug_current_sn_data_results.csv", TRUE)
        #
        current_nodeid <- dr_record[1,"A3600_NODEID"]
        #
        v_req_start_date <- get_v_req_start_date(db_conn,
                                                 options,
                                                 start_date,
                                                 end_date,
                                                 current_sn,
                                                 current_nodeid)
        if (nchar(v_req_start_date) <= 0) {
            print(sprintf("INFO: Zero length V_REQ_START_DATE. Skipping %s.", current_sn))
            next
        } else {
            print(sprintf("INFO: %s V_REQ_START_DATE = <%s>", current_sn, v_req_start_date))
        }
        v_req_start_date_tstamp <- as.POSIXct(v_req_start_date, 
                                              format="%Y-%m-%d %H:%M:%OS")
        #
        # sort by flag date.
        #
        sorted_current_sn_data_results <- 
            current_sn_data_results[order(current_sn_data_results$FLAG_DATE),]
        write_data(sorted_current_sn_data_results, 
                  "debug_sorted_current_sn_data_results.csv", TRUE)
        #
        sorted_desc_current_sn_data_results <- 
            current_sn_data_results[order(current_sn_data_results$FLAG_DATE,
                                          decreasing=TRUE),]
        write_data(sorted_desc_current_sn_data_results, 
                  "debug_sorted_desc_current_sn_data_results.csv", TRUE)
        #
        # process sorted records
        #
        for (isorted in 1:nrow(sorted_current_sn_data_results)) {
            sorted_record <- sorted_current_sn_data_results[isorted,]
            #
            flag_date <- sorted_record[1,"FLAG_DATE"]
            flag_date_tstamp <- as.POSIXct(flag_date, 
                                           format="%Y-%m-%d %H:%M:%OS")
            if ( ! (flag_date_tstamp >= v_req_start_date_tstamp)) {
                next
            }
            #
            filter_count <- 
                get_low_filter_count(sorted_desc_current_sn_data_results, 
                                     current_sn,
                                     threshold_number_unit,
                                     flag_date_tstamp)
            if ( ! (filter_count > 0)) {
                next
            }
            #
            v_date_30th <- 
                get_low_v_date_30th(sorted_desc_current_sn_data_results, 
                                    current_sn,
                                    threshold_number_unit,
                                    flag_date_tstamp)
            today_test_count <- 
                get_low_today_test_count(sorted_desc_current_sn_data_results, 
                                        current_sn,
                                        flag_date_tstamp)
            today_errorpct <- 
                get_low_today_errorpct(sorted_desc_current_sn_data_results, 
                                       current_sn,
                                       flag_date_tstamp)
            #
            if ( ! (today_test_count >= filter_count)) {
                next
            }
            #
            v_threshold_limit <- 
                get_low_v_threshold_limit(current_sn_data_results, 
                                          current_sn,
                                          threshold_number,
                                          flag_date_tstamp,
                                          v_date_30th,
                                          filter_count)
            if ( ! (v_threshold_limit > 0)) {
                next
            } else if ( ! (today_errorpct >= v_threshold_limit)) {
                next
            }
            #
            conseg_count <- 
                get_low_conseq_count(sorted_desc_current_sn_data_results, 
                                     current_sn,
                                     threshold_number,
                                     threshold_number_unit,
                                     v_threshold_limit,
                                     flag_date_tstamp,
                                     filter_count)
            if (conseg_count >= threshold_number_unit) {
                inh_value <- threshold_alert
                #
                flagged_record <- 
                    list(PHN_PATTERNS_SK=phm_patterns_sk,
                         PL=current_pl,
                         MODULESN=current_sn,
                         FLAG_DATE=flag_date,
                         CHART_DATA_VALUE=1,
                         FLAG_YN="Y",
                         IHN_LEVEL3_DESC=ihn_value)
                flagged_results <- rbind(flagged_results,
                                         flagged_record,
                                         stringsAsFactors=FALSE)
                #
                print(sprintf("INFO: SN: %s FLAGGED", current_sn))
            }
        } 
    }
    #
    return(flagged_results)
}
#
get_high_filter_count <- function(current_sn_data_results, 
                                  current_sn,
                                  threshold_number_unit,
                                  flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    #
    if (rows_to_keep > as.integer(threshold_number_unit)) {
        rows_to_keep <- as.integer(threshold_number_unit)
    }
    results <- results[1:rows_to_keep,]
    #
    filter_count <- abs(mean(results$TUBESTODAY)-sd(results$TUBESTODAY))
    #
    print(sprintf("INFO: get_high_filter_count: sn=%s, fc=%f,%d",
                  current_sn,
                  filter_count,
                  as.integer(filter_count)))
    #
    return(as.integer(filter_count))
}
#
get_high_v_date_30th <- function(current_sn_data_results, 
                                 current_sn,
                                 threshold_number_unit,
                                 flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return("1970-01-01 00:00:00")
    }
    #
    if (rows_to_keep > as.integer(threshold_number_unit)) {
        rows_to_keep <- as.integer(threshold_number_unit)
    }
    results <- results[1:rows_to_keep,]
    #
    v_date_30th <- min(results[,"FLAG_DATE"])
    #
    print(sprintf("INFO: get_high_v_date_30th: sn=%s, v_date_30th=%s",
                  current_sn,
                  v_date_30th))
    #
    return(v_date_30th)
}
#
get_high_today_test_count <- function(current_sn_data_results, 
                                      current_sn,
                                      flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") == flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    today_test_count <- as.integer(results[1,"TUBESTODAY"])
    #
    print(sprintf("INFO: get_high_today_test_count: sn=%s, today_test_count=%d",
                  current_sn,
                  today_test_count))
    #
    return(today_test_count)
}
#
get_high_today_errorpct <- function(current_sn_data_results, 
                                    current_sn,
                                    flag_date_tstamp)
{
    results <- 
        current_sn_data_results[as.POSIXct(current_sn_data_results$FLAG_DATE,
                                           format="%Y-%m-%d %H:%M:%OS") == flag_date_tstamp, ]
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    today_errorpct <- as.integer(results[1,"ERROR_PERCENTAGE"])
    #
    print(sprintf("INFO: get_high_today_errorpct: sn=%s, today_errorpct=%f",
                  current_sn,
                  today_errorpct))
    #
    return(today_errorpct)
}
#
get_high_v_threshold_limit <- function(current_sn_data_results, 
                                       current_sn,
                                       threshold_number,
                                       flag_date_tstamp,
                                       v_date_30th,
                                       filter_count)
{
    v_date_30th_tstamp <- as.POSIXct(v_date_30th,
                                     format="%Y-%m-%d %H:%M:%OS")
    #
    results <- 
        current_sn_data_results[
            (v_date_30th_tstamp <= as.POSIXct(current_sn_data_results$FLAG_DATE,
                                              format="%Y-%m-%d %H:%M:%OS")) &
            (as.POSIXct(current_sn_data_results$FLAG_DATE,
                        format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp), ]
    #
    number_of_rows <- nrow(results)
    if (number_of_rows <= 1) {
        return(0)
    }
    results <- results[1:number_of_rows,]
    results <- results[results$TUBESTODAY > filter_count,]
    if (nrow(results) <= 1) {
        return(0)
    }
    #
    v_threshold_limit <- as.double(threshold_number)*sd(results$ERROR_PERCENTAGE)+mean(results$ERROR_PERCENTAGE)
    #
    print(sprintf("INFO: get_high_v_threshold_limit: sn=%s, nrow=%d, v_threshold_limit=%f,%d",
                  current_sn,
                  nrow(results),
                  v_threshold_limit,
                  as.integer(v_threshold_limit)))
    #
    return(v_threshold_limit)
}
#
get_high_conseq_count <- function(current_sn_data_results, 
                                  current_sn,
                                  threshold_number,
                                  threshold_number_unit,
                                  v_threshold_limit,
                                  flag_date_tstamp,
                                  filter_count)
{
    results <- 
        current_sn_data_results[(as.POSIXct(current_sn_data_results$FLAG_DATE,
                                            format="%Y-%m-%d %H:%M:%OS") <= flag_date_tstamp), ]
    if (nrow(results) <= 0) {
        return(0)
    }
    #
    results <- results[results$TUBESTODAY > filter_count,]
    if (nrow(results) <= 0) {
        return(0)
    }
    #
    rows_to_keep <- nrow(results)
    if (rows_to_keep <= 0) {
        return(0)
    }
    #
    if (rows_to_keep > as.integer(threshold_number_unit)) {
        rows_to_keep <- as.integer(threshold_number_unit)
    }
    results <- results[1:rows_to_keep,]
    if (nrow(results) <= 0) {
        return(0)
    }
    #
    results <- results[results$ERROR_PRECENTAGE >= v_threshold_limit,]
    #
    print(sprintf("INFO: get_high_conseq_count: sn=%s, conseq_counts=nrow=%d",
                  current_sn,
                  nrow(results)))
    #
    return(nrow(results))
}
#
process_sd_high_volume_data <- function(db_conn,
                                        options,
                                        start_date,
                                        end_date,
                                        data_results,
                                        phm_patterns_sk,
                                        threshold_alert,
                                        threshold_number,
                                        threshold_number_desc,
                                        threshold_number_unit,
                                        threshold_data_days)
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
    #
    current_sn = " "
    flagged_results <- empty_results()
    #
    for (idr in 1:nrow(data_results)) {
        dr_record <- data_results[idr, ]
        #
        write_data(dr_record, "debug_sd_high_volume_data.csv")
        write_data(dr_record, "debug_all_data.csv", TRUE)
        #
        if (nchar(dr_record[1,"A3600_SERIALNUMBER_UC"]) <= 0) {
            next
        } else if (current_sn == dr_record[1,"A3600_SERIALNUMBER_UC"]) {
            #
            # skip if we have the same serial number.
            #
            next;
        }
        #
        print(sprintf("INFO: Processing SN: %s", dr_record[1,"A3600_SERIALNUMBER_UC"]))
        #
        flag              <- 'no'
        ihn_value         <- NA
        today_test_count  <- 0
        filter_count      <- 0
        today_errorpct    <- 0
        prev_day_errorpct <- 0
        v_threshold_limit <- 100000
        conseq_count      <- 0
        #
        current_sn <- dr_record[1,"A3600_SERIALNUMBER_UC"]
        current_sn_data_results <- 
            data_results[data_results$A3600_SERIALNUMBER_UC==current_sn,]
        current_pl <- dr_record[1,"A3600_IOM_PRODUCTLINE"]
        write_data(current_sn_data_results, "debug_current_sn_data_results.csv", TRUE)
        #
        current_nodeid <- dr_record[1,"A3600_NODEID"]
        #
        v_req_start_date <- get_v_req_start_date(db_conn,
                                                 options,
                                                 start_date,
                                                 end_date,
                                                 current_sn,
                                                 current_nodeid)
        if (nchar(v_req_start_date) <= 0) {
            print(sprintf("INFO: Zero length V_REQ_START_DATE. Skipping %s.", current_sn))
            next
        } else {
            print(sprintf("INFO: %s V_REQ_START_DATE = <%s>", current_sn, v_req_start_date))
        }
        v_req_start_date_tstamp <- as.POSIXct(v_req_start_date, 
                                              format="%Y-%m-%d %H:%M:%OS")
        #
        # sort by flag date.
        #
        sorted_current_sn_data_results <- 
            current_sn_data_results[order(current_sn_data_results$FLAG_DATE),]
        write_data(sorted_current_sn_data_results, 
                  "debug_sorted_current_sn_data_results.csv", TRUE)
        #
        sorted_desc_current_sn_data_results <- 
            current_sn_data_results[order(current_sn_data_results$FLAG_DATE,
                                          decreasing=TRUE),]
        write_data(sorted_desc_current_sn_data_results, 
                  "debug_sorted_desc_current_sn_data_results.csv", TRUE)
        #
        # process sorted records
        #
        for (isorted in 1:nrow(sorted_current_sn_data_results)) {
            sorted_record <- sorted_current_sn_data_results[isorted,]
            #
            flag_date <- sorted_record[1,"FLAG_DATE"]
            flag_date_tstamp <- as.POSIXct(flag_date, 
                                           format="%Y-%m-%d %H:%M:%OS")
            if ( ! (flag_date_tstamp >= v_req_start_date_tstamp)) {
                next
            }
            #
            filter_count <- 
                get_high_filter_count(sorted_desc_current_sn_data_results, 
                                      current_sn,
                                      threshold_number_unit,
                                      flag_date_tstamp)
            if ( ! (filter_count > 0)) {
                next
            }
            #
            v_date_30th <- 
                get_high_v_date_30th(sorted_desc_current_sn_data_results, 
                                     current_sn,
                                     threshold_number_unit,
                                     flag_date_tstamp)
            today_test_count <- 
                get_high_today_test_count(sorted_desc_current_sn_data_results, 
                                          current_sn,
                                          flag_date_tstamp)
            today_errorpct <- 
                get_high_today_errorpct(sorted_desc_current_sn_data_results, 
                                        current_sn,
                                        flag_date_tstamp)
            #
            if ( ! (today_test_count >= filter_count)) {
                next
            }
            #
            v_threshold_limit <- 
                get_high_v_threshold_limit(current_sn_data_results, 
                                           current_sn,
                                           threshold_number,
                                           flag_date_tstamp,
                                           v_date_30th,
                                           filter_count)
            if ( ! (v_threshold_limit > 0)) {
                next
            } else if ( ! (today_errorpct >= v_threshold_limit)) {
                next
            }
            #
            conseg_count <- 
                get_high_conseq_count(sorted_desc_current_sn_data_results, 
                                      current_sn,
                                      threshold_number,
                                      threshold_number_unit,
                                      v_threshold_limit,
                                      flag_date_tstamp,
                                      filter_count)
            if (conseg_count >= threshold_number_unit) {
                inh_value <- threshold_alert
                #
                flagged_record <- list(PHN_PATTERNS_SK=phm_patterns_sk,
                                       PL=current_pl,
                                       MODULESN=current_sn,
                                       FLAG_DATE=flag_date,
                                       CHART_DATA_VALUE=1,
                                       FLAG_YN="Y",
                                       IHN_LEVEL3_DESC=ihn_value)
                flagged_results <- rbind(flagged_results,
                                         flagged_record,
                                         stringsAsFactors=FALSE)
                #
                print(sprintf("INFO: SN: %s FLAGGED", current_sn))
            }
        } 
    }
    #
    return(flagged_results)
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
    print(params["INFO: IHN_LEVEL3_DESC", "PARAMETER_VALUE"])
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
        v_samp_id_chk2 <- "'&U__'"
    } else if ((pattern_description == "5015") && (module_type == "ISR")) {
        v_samp_id_chk1 <- "NULL"
        v_samp_id_chk2 <- "'%'"
    } else {
        v_samp_id_chk1 <- "' '"
        v_samp_id_chk2 <- "'%'"
    }
    write_data(params, "debug_params.csv")
    write_data(params, "debug_all_data.csv", TRUE)
    #
    final_results <- empty_results()
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
            print(sprintf("INFO: No records found. Skipping %s", 
                          params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]))
            return(empty_results())
        }
        names(data_results) <- toupper(names(data_results))
        write_data(data_results, "debug_data.csv")
        write_data(data_results, "debug_all_data.csv", TRUE)
        #
        final_results <- process_error_count_data(data_results,
                                                  phm_patterns_sk,
                                                  threshold_alert,
                                                  threshold_number,
                                                  threshold_number_desc,
                                                  threshold_number_unit,
                                                  threshold_data_days)
    } else if (algorithm_type == "PERCENTAGE") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        data_query <- get_percentage_data_query(start_date     = start_date,
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
            print(sprintf("INFO: No records found. Skipping %s", 
                          params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]))
            return(empty_results())
        }
        names(data_results) <- toupper(names(data_results))
        write_data(data_results, "debug_data.csv")
        write_data(data_results, "debug_all_data.csv", TRUE)
        #
        final_results <- process_percentage_data(data_results,
                                                 phm_patterns_sk,
                                                 threshold_alert,
                                                 threshold_number,
                                                 threshold_number_desc,
                                                 threshold_number_unit,
                                                 threshold_data_days)
    } else if (algorithm_type == "SD_HIGH_VOLUME") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        data_query <- get_sd_high_volume_data_query(start_date     = start_date,
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
        } else if (nrow(data_results) <= 0) {
            print(sprintf("INFO: No records found. Skipping %s", 
                          params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]))
            return(empty_results())
        }
        names(data_results) <- toupper(names(data_results))
        write_data(data_results, "debug_data.csv")
        write_data(data_results, "debug_all_data.csv", TRUE)
        #
        final_results <- process_sd_high_volume_data(db_conn, 
                                                     options,
                                                     start_date,
                                                     end_date,
                                                     data_results,
                                                     phm_patterns_sk,
                                                     threshold_alert,
                                                     threshold_number,
                                                     threshold_number_desc,
                                                     threshold_number_unit,
                                                     threshold_data_days)
    } else if (algorithm_type == "SD_LOW_VOLUME") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        data_query <- get_sd_low_volume_data_query(start_date     = start_date,
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
        } else if (nrow(data_results) <= 0) {
            print(sprintf("INFO: No records found. Skipping %s", 
                          params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]))
            return(empty_results())
        }
        names(data_results) <- toupper(names(data_results))
        write_data(data_results, "debug_data.csv")
        write_data(data_results, "debug_all_data.csv", TRUE)
        #
        final_results <- process_sd_low_volume_data(db_conn, 
                                                    options,
                                                    start_date,
                                                    end_date,
                                                    data_results,
                                                    phm_patterns_sk,
                                                    threshold_alert,
                                                    threshold_number,
                                                    threshold_number_desc,
                                                    threshold_number_unit,
                                                    threshold_data_days)
    } else {
        print(sprintf("INFO: Skipping UNKNOWN Algorithm Type: %s", algorithm_type))
    }
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
