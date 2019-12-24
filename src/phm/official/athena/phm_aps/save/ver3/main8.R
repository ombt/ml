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
get_bcr1_data_query <- function(start_date, 
                                end_date,
                                threshold_data_days,
                                phm_patterns_sk, 
                                run_date, 
                                algorithm_type)
{
    query <- "
with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
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
),
ac_cte as (
select 
    date_trunc('day', ac.timestamp_iso) as timestamp, 
    upper(trim(ac.serialnumber)) as iom_sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    date_trunc('day', ac.timestamp_iso) between
        dd.dt 
    and 
        (dd.dt_max + interval '1' day)
where 
    ac.id in ('normal','priority','tubes', '1','2','3','4','5','6','7','8')  
and 
    ac.duration in ('YTD')
and 
    ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso), 
    upper(trim(ac.serialnumber)),
    ac.duration, 
    ac.description, 
    ac.id
),
bcr1_cte as (
select
    max(ae.productline) as pl, 
    upper(trim(ae.serialnumber)) as iom_sn,
    date_trunc('day', ae.timestamp_iso) as dt, 
    count(*) as pat_errcount, 
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
where 
    ae.message like '%BCR%1%' 
and 
    ae.timestamp_iso between 
        (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
    and 
        dd.dt_max
group by 
    upper(trim(ae.serialnumber)),
    date_trunc('day', ae.timestamp_iso)
order by 
    upper(trim(ae.serialnumber)),
    date_trunc('day', ae.timestamp_iso)
),
vcount_normal_min_cte as (
select
    data.iom_sn,
    data.max_value as vcount_normal_min
from (
    select
        ac.iom_sn,
        ac.max_value
    from
        ac_cte ac,
        (
        select distinct
            bcr1_cte.iom_sn,
            bcr1_cte.dt
        from
            bcr1_cte
        ) bcr1
    where 
        ac.id = 'normal' 
    and 
        ac.duration = 'YTD'
    and 
        ac.description = 'InputTubeCounter' 
    and
        ac.iom_sn = bcr1.iom_sn
    and
        ac.timestamp <= bcr1.dt
    order by
        ac.timestamp desc
    ) data
),
vcount_normal_max_cte as (
select
    data.iom_sn,
    data.max_value as vcount_normal_max
from (
    select
        ac.iom_sn,
        ac.max_value
    from
        ac_cte ac,
        (
        select distinct
            bcr1_cte.iom_sn,
            bcr1_cte.dt
        from
            bcr1_cte
        ) bcr1
    where 
        ac.id = 'normal' 
    and 
        ac.duration = 'YTD'
    and 
        ac.description = 'InputTubeCounter' 
    and
        ac.iom_sn = bcr1.iom_sn
    and
        ac.timestamp = (bcr1.dt + interval '1' day)
    ) data
),
vcount_priority_min_cte as (
select
    data.iom_sn,
    data.max_value as vcount_priority_min
from (
    select
        ac.iom_sn,
        ac.max_value
    from
        ac_cte ac,
        (
        select distinct
            bcr1_cte.iom_sn,
            bcr1_cte.dt
        from
            bcr1_cte
        ) bcr1
    where 
        ac.id = 'priority' 
    and 
        ac.duration = 'YTD'
    and 
        ac.description = 'InputTubeCounter' 
    and
        ac.iom_sn = bcr1.iom_sn
    and
        ac.timestamp <= bcr1.dt
    order by
        ac.timestamp desc
    ) data
),
vcount_priority_max_cte as (
select
    data.iom_sn,
    data.max_value as vcount_priority_max
from (
    select
        ac.iom_sn,
        ac.max_value
    from
        ac_cte ac,
        (
        select distinct
            bcr1_cte.iom_sn,
            bcr1_cte.dt
        from
            bcr1_cte
        ) bcr1
    where 
        ac.id = 'priority' 
    and 
        ac.duration = 'YTD'
    and 
        ac.description = 'InputTubeCounter' 
    and
        ac.iom_sn = bcr1.iom_sn
    and
        ac.timestamp = (bcr1.dt + interval '1' day)
    ) data
),
aps_data as (
select
    bcr1.iom_sn,
    a.vcount_normal_min,
    b.vcount_normal_max,
    c.vcount_priority_min,
    d.vcount_priority_max
from
    bcr1_cte bcr1,
    vcount_normal_min_cte a,
    vcount_normal_max_cte b,
    vcount_priority_min_cte c,
    vcount_priority_max_cte d
where
    a.iom_sn = bcr1.iom_sn
and
    b.iom_sn = bcr1.iom_sn
and
    c.iom_sn = bcr1.iom_sn
and
    d.iom_sn = bcr1.iom_sn
)
select * from aps_data
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
    query <- gsub('<THRESHOLD_DATA_DAYS>', 
                  threshold_data_days, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PHM_PATTERNS_SK>',
                  phm_patterns_sk, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_RUN_DATE>',
                  run_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<ALGORITHM_TYPE>',
                  algorithm_type, 
                  query, 
                  fixed=TRUE)
    #
    return(query)
}
#
#####################################################################
#
get_bcr23_data_query <- function(start_date, 
                                 end_date,
                                 threshold_data_days,
                                 phm_patterns_sk, 
                                 run_date, 
                                 algorithm_type,
                                 pattern_text)
{
    query <- "
with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
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
),
ac_cte as (
select 
    date_trunc('day', ac.timestamp_iso) as dt, 
    upper(trim(ac.serialnumber)) as iom_sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
where 
    ac.id in ('normal','priority','tubes', '1','2','3','4','5','6','7','8')  
and 
    ac.duration in ('YTD')
and 
    ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso), 
    upper(trim(ac.serialnumber)),
    ac.duration, 
    ac.description, 
    ac.id
),
bcr23_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    count(*) as pat_errcount,
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || '<PATTERN_TEXT>' || '%'
where 
    (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || '<PATTERN_TEXT>' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from bcr23_cte
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
    query <- gsub('<THRESHOLD_DATA_DAYS>', 
                  threshold_data_days, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PHM_PATTERNS_SK>',
                  phm_patterns_sk, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_RUN_DATE>',
                  run_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<ALGORITHM_TYPE>',
                  algorithm_type, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PATTERN_TEXT>',
                  pattern_text, 
                  query, 
                  fixed=TRUE)
    #
    return(query)
}
#
#####################################################################
#
get_count_data_query <- function(start_date, 
                                 end_date,
                                 threshold_data_days,
                                 phm_patterns_sk, 
                                 run_date, 
                                 algorithm_type,
                                 pattern_text)
{
    query <- "
with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
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
),
ac_cte as (
select 
    date_trunc('day', ac.timestamp_iso) as dt, 
    upper(trim(ac.serialnumber)) as iom_sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join 
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
where 
    ac.id in ('normal','priority','tubes', '1','2','3','4','5','6','7','8')  
and 
    ac.duration in ('YTD')
and 
    ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso), 
    upper(trim(ac.serialnumber)),
    ac.duration, 
    ac.description, 
    ac.id
),
count_cte as (
select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as flag_date,
    count(*) as errorcount,
    max(ae.timestamp_iso) as ms_time
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    am.message like '%' || '<PATTERN_TEXT>' || '%'
where 
    (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message like '%' || '<PATTERN_TEXT>' || '%' 
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
)
select count(*) from count_cte
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
    query <- gsub('<THRESHOLD_DATA_DAYS>', 
                  threshold_data_days, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PHM_PATTERNS_SK>',
                  phm_patterns_sk, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_RUN_DATE>',
                  run_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<ALGORITHM_TYPE>',
                  algorithm_type, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PATTERN_TEXT>',
                  pattern_text, 
                  query, 
                  fixed=TRUE)
    #
    return(query)
}
#
#####################################################################
#
get_las205_data_query <- function(start_date, 
                                  end_date,
                                  threshold_data_days,
                                  phm_patterns_sk, 
                                  run_date, 
                                  algorithm_type,
                                  las_pattern_text)
{
    query <- "
with dd_cte as (
select 
    upper(trim(ae.serialnumber)) as iom_sn,
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
),
ac_cte as (
select 
    date_trunc('day', ac.timestamp_iso) as dt, 
    upper(trim(ac.serialnumber)) as iom_sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join
    dd_cte dd
on
    upper(trim(ac.serialnumber)) = dd.iom_sn
and 
    dd.dt <= date_trunc('day', ac.timestamp_iso) 
and 
    date_trunc('day', ac.timestamp_iso) < (dd.dt_max + interval '1' day)
where 
    ac.id in ('normal','priority','tubes', '1','2','3','4','5','6','7','8')  
and 
    ac.duration in ('YTD')
and 
    ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso), 
    upper(trim(ac.serialnumber)),
    ac.duration, 
    ac.description, 
    ac.id
),
las205_cte as (
select
    max(am.iom_sn) as iom_sn,
    max(am.pl) as pl,
    max(upper(trim(am.sn))) as sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as max_timestamp,
    count(*) as las_error_count
from 
    dx.dx_aps_error ae
inner join 
    dd_cte dd
on
    ae.serialnumber = dd.iom_sn
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    am.iom_sn = dd.iom_sn
and
    am.message like '<LAS_PATTERN_TEXT>' || ': Error 205: Unreadable Barcode' || '%' 
inner join
    dx.dx_aps_error ae2
on
    ae2.serialnumber = dd.iom_sn
and
    ae2.message like 'Interface Module Unreadable Barcode -%' 
and
    (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day) <= ae2.timestamp_iso 
and 
    ae2.timestamp_iso < dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
and
    ae.message = '<LAS_PATTERN_TEXT>' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
)
select count(*) from las205_cte
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
    query <- gsub('<THRESHOLD_DATA_DAYS>', 
                  threshold_data_days, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<PHM_PATTERNS_SK>',
                  phm_patterns_sk, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<V_RUN_DATE>',
                  run_date, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<ALGORITHM_TYPE>',
                  algorithm_type, 
                  query, 
                  fixed=TRUE)
    query <- gsub('<LAS_PATTERN_TEXT>',
                  las_pattern_text, 
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
    final_results <- empty_results()
    #
    if (algorithm_type == "BCR_1") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        bcr1_query <- get_bcr1_data_query(start_date, 
                                          end_date,
                                          threshold_data_days,
                                          phm_patterns_sk,
                                          end_date, 
                                          algorithm_type)
        bcr1_results <- exec_query(params,
                                   db_conn, 
                                   bcr1_query,
                                   options, 
                                   test_period)
        #
        if (errors$occurred()) {
            print(sprintf("INFO: Error reading %s data", algorithm_type)) 
            return(empty_results())
        } else if (nrow(bcr1_results) <= 0) {
            print(sprintf("INFO: No %s records found", algorithm_type))
            return(empty_results())
        }
        #
        names(bcr1_results) <- toupper(names(bcr1_results))
        write_data(bcr1_results, "debug_bcr1_data.csv")
        write_data(bcr1_results, "debug_all_data.csv", TRUE)
        #
        print(sprintf("INFO: %d %s records found", nrow(bcr1_results), algorithm_type))
    } else if (algorithm_type == "BCR_2/3") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        bcr23_query <- get_bcr23_data_query(start_date, 
                                            end_date,
                                            threshold_data_days,
                                            phm_patterns_sk,
                                            end_date, 
                                            algorithm_type,
                                            error_code_reg_expr)
        bcr23_results <- exec_query(params,
                                    db_conn, 
                                    bcr23_query,
                                    options, 
                                    test_period)
        #
        if (errors$occurred()) {
            print(sprintf("INFO: Error reading %s data", algorithm_type)) 
            return(empty_results())
        } else if (nrow(bcr23_results) <= 0) {
            print(sprintf("INFO: No %s records found", algorithm_type))
            return(empty_results())
        }
        #
        names(bcr23_results) <- toupper(names(bcr23_results))
        write_data(bcr23_results, "debug_bcr23_data.csv")
        write_data(bcr23_results, "debug_all_data.csv", TRUE)
        #
        print(sprintf("INFO: %d %s records found", nrow(bcr23_results), algorithm_type))
    } else if (algorithm_type == "COUNT") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        count_query <- get_count_data_query(start_date, 
                                            end_date,
                                            threshold_data_days,
                                            phm_patterns_sk,
                                            end_date, 
                                            algorithm_type,
                                            error_code_reg_expr)
        count_results <- exec_query(params,
                                    db_conn, 
                                    count_query,
                                    options, 
                                    test_period)
        #
        if (errors$occurred()) {
            print(sprintf("INFO: Error reading %s data", algorithm_type)) 
            return(empty_results())
        } else if (nrow(count_results) <= 0) {
            print(sprintf("INFO: No %s records found", algorithm_type))
            return(empty_results())
        }
        #
        names(count_results) <- toupper(names(count_results))
        write_data(count_results, "debug_count_data.csv")
        write_data(count_results, "debug_all_data.csv", TRUE)
        #
        print(sprintf("INFO: %d %s records found", nrow(count_results), algorithm_type))
    } else if (algorithm_type == "LAS_205") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        las205_query <- get_las205_data_query(start_date, 
                                              end_date,
                                              threshold_data_days,
                                              phm_patterns_sk,
                                              end_date, 
                                              algorithm_type,
                                              error_code_reg_expr)
        las205_results <- exec_query(params,
                                     db_conn, 
                                     las205_query,
                                     options, 
                                     test_period)
        #
        if (errors$occurred()) {
            print(sprintf("INFO: Error reading %s data", algorithm_type)) 
            return(empty_results())
        } else if (nrow(las205_results) <= 0) {
            print(sprintf("INFO: No %s records found", algorithm_type))
            return(empty_results())
        }
        #
        names(las205_results) <- toupper(names(las205_results))
        write_data(las205_results, "debug_las205_data.csv")
        write_data(las205_results, "debug_all_data.csv", TRUE)
        #
        print(sprintf("INFO: %d %s records found", nrow(las205_results), algorithm_type))
    } else {
        print(sprintf("INFO: Skipping unknown Algorithm Type: %s", algorithm_type))
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

    date_parse('<START_DATE>', '%m/%d/%Y %T') <= v.datetimestamplocal
and 
    v.datetimestamplocal < date_parse('<END_DATE>', '%m/%d/%Y %T') 
and
