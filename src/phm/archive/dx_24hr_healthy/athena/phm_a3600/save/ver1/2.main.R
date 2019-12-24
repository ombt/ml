#
# Architect CC Cuvette Combined
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
v_alg_num <- 
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
device_and_dates_query_template <- "
    select 
        asi.productlineref,
        asi.deviceid,
        asi.systemsn,
        aln.sn,
        ae.nodetype,
        ae.errorcode,
        max (ae.completiondate) max_compl_date,
        trunc (min (ae.completiondate)) min_compl_date
    from 
        svc_phm_ods.phm_ods_a3600_errors ae,
        a3600_layout_nodes_pl_sn aln,
        idaowner.a3600systeminformation asi
    where     
            to_timestamp('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= run_date
        and 
            run_date < to_timestamp('<END_DATE>', 
                                    'MM/DD/YYYY HH24:MI:SS')
    and 
        ae.errorcode = '<ERROR_CODE_VALUE>'
    and 
        ae.layout_nodes_id = aln.layout_nodes_id
    and 
        aln.systeminfoid = asi.systeminfoid
    and 
        aln.sn is not null
    and 
        aln.canid = ae.nodeid
    and 
        asi.current_row = 'Y'
    and 
        ((('<MODULE>' != '%') and 
          (ae.nodetype = '<MODULE>')) or 
         (('<MODULE>' = '%') and 
          (ae.nodetype like '<MODULE>')))
    group by 
        asi.productlineref,
        asi.deviceid,
        asi.systemsn,
        aln.sn,
        ae.nodetype,
        ae.errorcode
    order by 
        asi.systemsn, 
        ae.nodetype, 
        ae.errorcode
"
#
get_threshold_counts_query <- function(test_period,
                                       v_sn1,
                                       v_nodetype1,
                                       v_errorcode1,
                                       v_start_date,
                                       v_end_date,
                                       v_data_days,
                                       v_samp_id_chk1,
                                       v_samp_id_chk2)
{
    query <- "
select 
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc(completiondate) as flag_date,
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today,
    max(ae.completiondate) as max_compl_date,
    count(ae.errorcode) as error_count,
    trunc((count(ae.errorcode)*100/ac.tubes_today), 2)
    error_percentage
from 
    svc_phm_ods.phm_ods_a3600_errors ae,
    a3600_layout_nodes_pl_sn n,
    idaowner.a3600systeminformation asi,
    idaowner.a3600_counters ac
where
    ae.layout_nodes_id = n.layout_nodes_id
and 
    n.canid = ae.nodeid
and 
    n.nodetype = ae.nodetype
and 
    lower(n.sn) = lower('<V_SN1>')
and 
    asi.systeminfoid = n.systeminfoid
and 
    ac.layout_nodes_id = n.layout_nodes_id
and 
    ac.nodetype = ae.nodetype
and 
    ac.counter_date = trunc(ae.completiondate)
and 
    ac.nodeid = ae.nodeid
and 
    ac.instanceid = ae.instanceid
and 
    n.systeminfoid = asi.systeminfoid
and 
    asi.current_row = 'Y'
and 
    ac.tubes_today <> 0
and 
    ((('<V_NODETYPE1>' != '%') and (ae.nodetype = '<V_NODETYPE1>')) or
     (('<V_NODETYPE1>' = '%') and (ae.nodetype like '<V_NODETYPE1>')))
and 
    ae.errorcode = '<V_ERRORCODE1>'
and 
    nvl(ae.sampleid, '<V_SAMP_ID_CHK1>') like nvl('<V_SAMP_ID_CHK2>', ae.sampleid)
and 
    ae.completiondate between 
        to_timestamp('<V_START_DATE>', 'YYYY-MM-DD HH24:MI:SS.FF') - <V_DATA_DAYS> + 1
    and
        to_timestamp('<V_END_DATE>', 'YYYY-MM-DD HH24:MI:SS.FF')
group by 
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc(completiondate),
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today
"
    #
    query <- gsub('<V_SN1>', 
                  v_sn1, 
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
    query <- gsub('<V_START_DATE>', 
                  v_start_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_END_DATE>', 
                  v_end_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_DATA_DAYS>', 
                  v_data_days, 
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
    #
    return(query)
}
#
#####################################################################
#
get_error_count_y_query <- function(test_period,
                                    phm_patterns_sk)
{ 
    query <- "
select distinct 
    e.module_sn,
    e.device_id,
    i.customer_name as customername,
    i.customer_number as customernumber,
    i.city,
    i.country_code as countrycode,
    pc.country as countryname,
    pc.arearegion as area,
    pc.arearegion,
    e.pl
from (
    select 
        max (sn) as sn,
        max (pl) as pl,
        max (customer_num) as customer_number,
        max (customer) as customer_name,
        max (city) as city,
        max (country_code) as country_code
    from 
        instrumentlisting
    group by sn) i,
    svc_phm_ods.phm_a3600_temp_error e,
    phm_country pc
where
    upper (i.sn) = e.module_sn
and 
    e.run_date = to_timestamp('<V_RUN_DATE>', 'YYYY-MM-DD HH24:MI:SS.FF')
and 
    i.pl = e.pl
and 
    pc.country_code = i.country_code
and 
    e.phm_thresholds_sk = '<PHM_THRESHOLDS_SK>'
"
    #
    query <- gsub('<V_RUN_DATE>', 
                  test_period["START_DATE", "VALUE"], 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PHM_THRESHOLDS_SK>',
                  phm_patterns_sk,
                  query, 
                  fixed=TRUE)
    return(query)
}
#
process_error_count <- function(test_period,
                                v_alg_num,
                                options,
                                dd_record, 
                                tc_results,
                                module_type,
                                phm_patterns_sk,
                                pattern_description,
                                threshold_alert,
                                threshold_number,
                                threshold_number_desc,
                                threshold_number_unit,
                                threshold_data_days)
{ 
    y_query <- get_error_count_y_query(test_period,
                                       phm_patterns_sk)
    y_results <- exec_query(empty_df,
                            db_conn, 
                            y_query,
                            options, 
                            empty_df)
    #
    if (errors$occurred()) {
        return(data.frame())
    }
    else if (nrow(y_results) <= 0) {
        return(data.frame())
    }
    write_data(y_results, "error_count_y_results.csv")
    write_data(y_results, "all_data.csv", TRUE)
    #
    return(data.frame())
}
#
#####################################################################
#
get_percentage_y_query <- function(test_period,
                                   phm_patterns_sk)
{ 
    query <- "
select distinct 
    e.module_sn,
    e.device_id,
    i.customer_name as customername,
    i.customer_number as customernumber,
    i.city,
    i.country_code as countrycode,
    pc.country as countryname,
    pc.arearegion as area,
    pc.arearegion,
    e.pl
from (
    select 
        max (sn) as sn,
        max (pl) as pl,
        max (customer_num) as customer_number,
        max (customer) as customer_name,
        max (city) as city,
        max (country_code) as country_code
    from 
        instrumentlisting
    group by sn) i,
    svc_phm_ods.phm_a3600_temp_error e,
    phm_country pc
where
    upper (i.sn) = e.module_sn
and 
    e.run_date = to_timestamp('<V_RUN_DATE>', 'YYYY-MM-DD HH24:MI:SS.FF')
and 
    i.pl = e.pl
and 
    pc.country_code = i.country_code
and 
    e.phm_thresholds_sk = '<PHM_THRESHOLDS_SK>'
"
    #
    query <- gsub('<V_RUN_DATE>', 
                  test_period["START_DATE", "VALUE"], 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PHM_THRESHOLDS_SK>',
                  phm_patterns_sk,
                  query, 
                  fixed=TRUE)
    return(query)
}
#
process_percentage <- function(test_period,
                               v_alg_num,
                               options,
                               dd_record, 
                               tc_results,
                               module_type,
                               phm_patterns_sk,
                               pattern_description,
                               threshold_alert,
                               threshold_number,
                               threshold_number_desc,
                               threshold_number_unit,
                               threshold_data_days)
{
    y_query <- get_percentage_y_query(test_period,
                                       phm_patterns_sk)
    y_results <- exec_query(empty_df,
                            db_conn, 
                            y_query,
                            options, 
                            empty_df)
    #
    if (errors$occurred()) {
        return(data.frame())
    }
    else if (nrow(y_results) <= 0) {
        return(data.frame())
    }
    write_data(y_results, "percentage_y_results.csv")
    write_data(y_results, "all_data.csv", TRUE)
    #
    return(data.frame())
}
#
#####################################################################
#
get_sd_high_volume_y_query <- function(test_period,
                                       v_alg_num)
erns_sk)
{ 
    query <- "
select distinct 
    e.module_sn,
    e.device_id,
    i.customer_name as customername,
    i.customer_number as customernumber,
    i.city,
    i.country_code as countrycode,
    pc.country as countryname,
    pc.arearegion as area,
    pc.arearegion,
    e.pl
from (
    select 
        max (sn) as sn,
        max (pl) as pl,
        max (customer_num) as customer_number,
        max (customer) as customer_name,
        max (city) as city,
        max (country_code) as country_code
    from 
        instrumentlisting
    group by sn) i,
    svc_phm_ods.phm_a3600_temp_error e,
    phm_country pc
where
    upper (i.sn) = e.module_sn
and 
    e.run_date = to_timestamp('<V_RUN_DATE>', 'YYYY-MM-DD HH24:MI:SS.FF')
and 
    i.pl = e.pl
and 
    pc.country_code = i.country_code
and 
    e.phm_algorithm_definitions_sk = <V_ALG_NUM>
"
    #
    query <- gsub('<V_RUN_DATE>', 
                  test_period["START_DATE", "VALUE"], 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_ALG_NUM>',
                  v_alg_num,
                  query, 
                  fixed=TRUE)
    return(query)
}
#
process_sd_high_volume <- function(test_period,
                                   v_alg_num,
                                   options,
                                   dd_record, 
                                   tc_results,
                                   module_type,
                                   phm_patterns_sk,
                                   pattern_description,
                                   threshold_alert,
                                   threshold_number,
                                   threshold_number_desc,
                                   threshold_number_unit,
                                   threshold_data_days)
{
    y_query <- get_sd_high_volume_y_query(test_period,
                                          v_alg_num)
    y_results <- exec_query(empty_df,
                            db_conn, 
                            y_query,
                            options, 
                            empty_df)
    #
    if (errors$occurred()) {
        return(data.frame())
    }
    else if (nrow(y_results) <= 0) {
        return(data.frame())
    }
    write_data(y_results, "sd_high_volume_y_results.csv")
    write_data(y_results, "all_data.csv", TRUE)
    #
    return(data.frame())
}
#
#####################################################################
#
get_sd_low_volume_y_query <- function(test_period,
                                      phm_patterns_sk)
{ 
    query <- "
select distinct 
    e.module_sn,
    e.device_id,
    i.customer_name as customername,
    i.customer_number as customernumber,
    i.city,
    i.country_code as countrycode,
    pc.country as countryname,
    pc.arearegion as area,
    pc.arearegion,
    e.pl
from (
    select 
        max (sn) as sn,
        max (pl) as pl,
        max (customer_num) as customer_number,
        max (customer) as customer_name,
        max (city) as city,
        max (country_code) as country_code
    from 
        instrumentlisting
    group by sn) i,
    svc_phm_ods.phm_a3600_temp_error e,
    phm_country pc
where
    upper (i.sn) = e.module_sn
and 
    e.run_date = to_timestamp('<V_RUN_DATE>', 'YYYY-MM-DD HH24:MI:SS.FF')
and 
    i.pl = e.pl
and 
    pc.country_code = i.country_code
and 
    e.phm_thresholds_sk = '<PHM_THRESHOLDS_SK>'
"
    #
    query <- gsub('<V_RUN_DATE>', 
                  test_period["START_DATE", "VALUE"], 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PHM_THRESHOLDS_SK>',
                  phm_patterns_sk,
                  query, 
                  fixed=TRUE)
    return(query)
}
#
process_sd_low_volume <- function(test_period,
                                  v_alg_num,
                                  options,
                                  dd_record, 
                                  tc_results,
                                  module_type,
                                  phm_patterns_sk,
                                  pattern_description,
                                  threshold_alert,
                                  threshold_number,
                                  threshold_number_desc,
                                  threshold_number_unit,
                                  threshold_data_days)
{
    y_query <- get_sd_low_volume_y_query(test_period,
                                         phm_patterns_sk)
    y_results <- exec_query(empty_df,
                            db_conn, 
                            y_query,
                            options, 
                            empty_df)
    #
    if (errors$occurred()) {
        return(data.frame())
    }
    else if (nrow(y_results) <= 0) {
        return(data.frame())
    }
    write_data(y_results, "sd_low_volume_y_results.csv")
    write_data(y_results, "all_data.csv", TRUE)
    3
    return(data.frame())
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
    phm_patterns_sk       <- unique(params[ , "PHM_PATTERNS_SK_DUP"])[1]
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
    # device and dates query
    #
    dd_results <- exec_query(params, 
                             db_conn, 
                             device_and_dates_query_template,
                             options, 
                             test_period)
    #
    if (errors$occurred()) {
        return(empty_results())
    }
    else if (nrow(dd_results) <= 0) {
        return(empty_results())
    }
    write_data(dd_results, "device_and_dates.csv")
    write_data(dd_results, "all_data.csv", TRUE)
    #
    # cycle over all the devices and dates
    #
    for (iddr in 1:nrow(dd_results)) {
        #
        # get the DD record 
        #
        dd_record <- dd_results[iddr, ]
        write_data(dd_record, "device_and_dates_record.csv")
        write_data(dd_record, "all_data.csv", TRUE)
        #
        # generate threshold counts query
        #
        tc_query <- get_threshold_counts_query(test_period,
                                               v_sn1 = dd_record[1,"SN"],
                                               v_nodetype1 = module_type,
                                               v_errorcode1 = pattern_description,
                                               v_start_date = dd_record[1,"MIN_COMPL_DATE"],
                                               v_end_date = dd_record[1,"MAX_COMPL_DATE"],
                                               v_data_days = threshold_number_unit,
                                               v_samp_id_chk1 = v_samp_id_chk1,
                                               v_samp_id_chk2 = v_samp_id_chk2)
        #
        tc_results <- exec_query(empty_df,
                                 db_conn, 
                                 tc_query,
                                 options, 
                                 empty_df)
        #
        if (errors$occurred()) {
            next
        }
        else if (nrow(tc_results) <= 0) {
            next
        }
        write_data(tc_results, "threshold_counts.csv")
        write_data(tc_results, "all_data.csv", TRUE)
        #
        # check the type of algorithm
        #
        if (algorithm_type == "ERROR_COUNT") {
            print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
            results <- process_error_count(test_period,
                                           v_alg_num,
                                           options, 
                                           dd_record, 
                                           tc_results,
                                           phm_patterns_sk,
                                           module_type,
                                           pattern_description,
                                           threshold_alert,
                                           threshold_number,
                                           threshold_number_desc,
                                           threshold_number_unit,
                                           threshold_data_days)
        } else if (algorithm_type == "PERCENTAGE") {
            print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
            results <- process_percentage(test_period,
                                          v_alg_num,
                                          options, 
                                          dd_record, 
                                          tc_results,
                                          phm_patterns_sk,
                                          module_type,
                                          pattern_description,
                                          threshold_alert,
                                          threshold_number,
                                          threshold_number_desc,
                                          threshold_number_unit,
                                          threshold_data_days)
        } else if (algorithm_type == "SD_HIGH_VOLUME") {
            print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
            results <- process_sd_high_volume(test_period,
                                              v_alg_num,
                                              options, 
                                              dd_record, 
                                              tc_results,
                                              module_type,
                                              phm_patterns_sk,
                                              pattern_description,
                                              threshold_alert,
                                              threshold_number,
                                              threshold_number_desc,
                                              threshold_number_unit,
                                              threshold_data_days)
        } else if (algorithm_type == "SD_LOW_VOLUME") {
            print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
            results <- process_sd_low_volume(test_period,
                                             v_alg_num,
                                             options, 
                                             dd_record, 
                                             tc_results,
                                             module_type,
                                             phm_patterns_sk,
                                             pattern_description,
                                             threshold_alert,
                                             threshold_number,
                                             threshold_number_desc,
                                             threshold_number_unit,
                                             threshold_data_days)
        } else {
            print(sprintf("INFO: Skipping UNKNOWN Algorithm Type: %s", algorithm_type))
        }
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
    "ida")
#
q(status=0)
