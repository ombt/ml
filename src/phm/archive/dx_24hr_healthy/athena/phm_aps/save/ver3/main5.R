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
),
ac_cte as (
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
    ) dd
where 
    upper(trim(ac.serialnumber)) = dd.sn
and 
    date_trunc('day', ac.timestamp_iso) between 
        dd.dt 
    and 
        dd.dt_max + interval '1' day
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
),
ebcr1_cte as (
select
    max(ae.productline) as pl, 
    ae.serialnumber as sn, 
    date_trunc('day', ae.timestamp_iso) as dt, 
    count(*) as pat_errcount, 
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae,
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
    ) dd
where 
    ae.message like '%BCR%1%' 
and 
    ae.serialnumber = dd.sn
and 
    ae.timestamp_iso between 
        (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
    and 
        dd.dt_max
group by 
    ae.serialnumber,
    date_trunc('day', ae.timestamp_iso)
)
select
    '<PHM_PATTERNS_SK>' as phm_patterns_sk, 
    '<V_RUN_DATE>' as run_date, 
    y.sn as iom_sn, 
    y.pl as pl, 
    y.sn as sn, 
    y.flag_date as timestamp, 
    '<ALGORITHM_TYPE>' as algorithm_type, 
    errors_per_day as testcount, 
    y.pat_errcount as errorcount, 
    per_error_count as errorpct, 
    0 as timestamp_ms, 
    now() as date_created
from (
    select
        eb.sn,
        eb.pat_errcount,
        (e.vcount_normal_max + 
         e.vcount_priority_max -
         e.vcount_normal_min -
         e.vcount_priority_min) as errors_per_day,
        case when (e.vcount_normal_max + 
                   e.vcount_priority_max -
                   e.vcount_normal_min -
                   e.vcount_priority_min) > 0
             then
                 (eb.pat_errcount*100.0) /
                 (e.vcount_normal_max + 
                  e.vcount_priority_max -
                  e.vcount_normal_min -
                  e.vcount_priority_min)
             else
                 0
             end as per_error_count
    from 
        ebcr1_cte eb,
        (
        select distinct
            ebcr1_cte.sn,
            a.min_max_value as vcount_normal_min,
            b.max_max_value as vcount_normal_max,
            c.min_max_value as vcount_priority_min,
            d.max_max_value as vcount_priority_max 
        from (
            select 
                ebcr1_cte.sn,
                min(ac_cte.max_value) as min_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'normal' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.sn
            ) a,
            (
            select 
                ebcr1_cte.sn,
                max(ac_cte.max_value) as max_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'normal' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.sn
            ) b,
            (
            select 
                ebcr1_cte.sn,
                min(ac_cte.max_value) as min_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'priority' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.sn
            ) c,
            (
            select 
                ebcr1_cte.sn,
                max(ac_cte.max_value) as max_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'priority' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.sn
            ) d,
            ebcr1_cte
        where
            a.sn = ebcr1_cte.sn
        and
            b.sn = ebcr1_cte.sn
        and
            c.sn = ebcr1_cte.sn
        and
            d.sn = ebcr1_cte.sn
        ) e
    where
        eb.sn = e.sn
    ) aps_data,
    ebcr1_cte as y
where
    aps_data.sn = y.sn
limit 100
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
get_bcr23_data_query <- function(start_date, 
                                 end_date,
                                 threshold_data_days,
                                 phm_patterns_sk, 
                                 run_date, 
                                 algorithm_type)
{
    query <- "
with dd_cte as (
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
),
ac_cte as (
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
    ) dd
where 
    upper(trim(ac.serialnumber)) = dd.sn
and 
    date_trunc('day', ac.timestamp_iso) between 
        dd.dt 
    and 
        dd.dt_max + interval '1' day
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
),
ebcr23_cte as (
select
    ae.serialnumber as iom_sn, 
    max(ae.productline) as pl, 
    ae.serialnumber as sn, 
    date_trunc('day', ae.timestamp_iso) as dt, 
    count(*) as pat_errcount, 
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae,
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
    ) dd
where 
    ae.message like '%<PATTERN_TEXT>%'
and 
    ae.serialnumber = dd.sn
and 
    ae.timestamp_iso between 
        (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
    and 
        dd.dt_max
group by 
    max(ae.productline),
    ae.serialnumber,
    date_trunc('day', ae.timestamp_iso)
)
select
    '<PHM_PATTERNS_SK>' as phm_patterns_sk, 
    '<V_RUN_DATE>' as run_date, 
    y.sn as iom_sn, 
    y.pl as pl, 
    y.sn as sn, 
    y.flag_date as timestamp, 
    '<ALGORITHM_TYPE>' as algorithm_type, 
    errors_per_day as testcount, 
    y.pat_errcount as errorcount, 
    per_error_count as errorpct, 
    0 as timestamp_ms, 
    now() as date_created
from (
    select
        eb.sn,
        eb.pat_errcount,
        (e.vcount_normal_max + 
         e.vcount_priority_max -
         e.vcount_normal_min -
         e.vcount_priority_min) as errors_per_day,
        case when (e.vcount_normal_max + 
                   e.vcount_priority_max -
                   e.vcount_normal_min -
                   e.vcount_priority_min) > 0
             then
                 (eb.pat_errcount*100.0) /
                 (e.vcount_normal_max + 
                  e.vcount_priority_max -
                  e.vcount_normal_min -
                  e.vcount_priority_min)
             else
                 0
             end as per_error_count
    from 
        ebcr1_cte eb,
        (
        select distinct
            ebcr1_cte.sn,
            a.min_max_value as vcount_normal_min,
            b.max_max_value as vcount_normal_max,
            c.min_max_value as vcount_priority_min,
            d.max_max_value as vcount_priority_max 
        from (
            select 
                ebcr1_cte.sn,
                min(ac_cte.max_value) as min_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'normal' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.sn
            ) a,
            (
            select 
                ebcr1_cte.sn,
                max(ac_cte.max_value) as max_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'normal' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.sn
            ) b,
            (
            select 
                ebcr1_cte.sn,
                min(ac_cte.max_value) as min_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'priority' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.sn
            ) c,
            (
            select 
                ebcr1_cte.sn,
                max(ac_cte.max_value) as max_max_value
            from 
                ac_cte,
                ebcr1_cte
            where 
                ac_cte.id = 'priority' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'InputTubeCounter' 
            and 
                ac_cte.sn = ebcr1_cte.sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.sn
            ) d,
            ebcr1_cte
        where
            a.sn = ebcr1_cte.sn
        and
            b.sn = ebcr1_cte.sn
        and
            c.sn = ebcr1_cte.sn
        and
            d.sn = ebcr1_cte.sn
        ) e
    where
        eb.sn = e.sn
    ) aps_data,
    ebcr1_cte as y
where
    aps_data.sn = y.sn
limit 100
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
            print(sprintf("INFO: Error reading BCR_1 data")) 
            return(empty_results())
        } else if (nrow(bcr1_results) <= 0) {
            print(sprintf("INFO: No BCR_1 records found"))
            next
        }
        #
        names(bcr1_results) <- toupper(names(bcr1_results))
        write_data(bcr1_results, "debug_bcr1_data.csv")
        write_data(bcr1_results, "debug_all_data.csv", TRUE)
        #
        print(sprintf("INFO: %d BCR1 records found", nrow(bcr1_results)))
    } else if (algorithm_type == "BCR_2/3") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        bcr23_query <- get_bcr23_data_query(start_date, 
                                            end_date,
                                            threshold_data_days,
                                            phm_patterns_sk,
                                            end_date, 
                                            algorithm_type)
        bcr23_results <- exec_query(params,
                                    db_conn, 
                                    bcr23_query,
                                    options, 
                                    test_period)
        #
        if (errors$occurred()) {
            print(sprintf("INFO: Error reading BCR_2/3 data")) 
            return(empty_results())
        } else if (nrow(bcr23_results) <= 0) {
            print(sprintf("INFO: No BCR_2/3 records found"))
            next
        }
        #
        names(bcr23_results) <- toupper(names(bcr23_results))
        write_data(bcr23_results, "debug_bcr23_data.csv")
        write_data(bcr23_results, "debug_all_data.csv", TRUE)
        #
        print(sprintf("INFO: %d BCR_2/3 records found", nrow(bcr23_results)))
    } else if (algorithm_type == "COUNT") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
    } else if (algorithm_type == "LAS_205") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
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
