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
    ac.iom_sn,
    ac.timestamp,
    min(ac.max_value) as vcount_normal_min
from
    ac_cte ac
inner join
    bcr1_cte bcr1
on
    ac.iom_sn = bcr1.iom_sn
where 
    ac.id = 'normal' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'InputTubeCounter' 
and
    ac.iom_sn = bcr1.iom_sn
and
    ac.timestamp = bcr1.dt
group by
    ac.iom_sn,
    ac.timestamp
),
vcount_normal_max_cte as (
select
    ac.iom_sn,
    ac.timestamp,
    max(ac.max_value) as vcount_normal_max
from
    ac_cte ac
inner join
    bcr1_cte bcr1
on
    ac.iom_sn = bcr1.iom_sn
where 
    ac.id = 'normal' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'InputTubeCounter' 
and
    ac.iom_sn = bcr1.iom_sn
and
    ac.timestamp = bcr1.dt
group by
    ac.iom_sn,
    ac.timestamp
),
vcount_priority_min_cte as (
select
    ac.iom_sn,
    ac.timestamp,
    min(ac.max_value) as vcount_priority_min
from
    ac_cte ac
inner join
    bcr1_cte bcr1
on
    ac.iom_sn = bcr1.iom_sn
where 
    ac.id = 'priority' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'InputTubeCounter' 
and
    ac.iom_sn = bcr1.iom_sn
and
    ac.timestamp = bcr1.dt
group by
    ac.iom_sn,
    ac.timestamp
),
vcount_priority_max_cte as (
select
    ac.iom_sn,
    ac.timestamp,
    max(ac.max_value) as vcount_priority_max
from
    ac_cte ac
inner join
    bcr1_cte bcr1
on
    ac.iom_sn = bcr1.iom_sn
where 
    ac.id = 'priority' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'InputTubeCounter' 
and
    ac.iom_sn = bcr1.iom_sn
and
    ac.timestamp = bcr1.dt
group by
    ac.iom_sn,
    ac.timestamp
),
aps_data_cte as (
select
    '<PHM_PATTERNS_SK>' as   phm_patterns_sk, 
    '<V_RUN_DATE>' as        run_date, 
    data2.iom_sn as          iom_sn, 
    data2.pl as              pl, 
    data2.iom_sn as          sn, 
    data2.flag_date as       timestamp, 
    '<ALGORITHM_TYPE>' as    algorithm_type, 
    data2.errors_per_day as  testcount, 
    data2.pat_errcount as    errorcount, 
    data2.per_error_count as errorpct, 
    0 as                     timestamp_ms, 
      now() as               date_created
from (
    select
        data.iom_sn,
        data.pl,
        data.flag_date,
        data.pat_errcount,
        (data.vcount_normal_max +
         data.vcount_priority_max -
         data.vcount_normal_min -
         data.vcount_priority_min) as errors_per_day,
        case when (data.vcount_normal_max +
                   data.vcount_priority_max -
                   data.vcount_normal_min -
                   data.vcount_priority_min) > 0
             then
                 (cast (data.pat_errcount as double)*100.0)/
                 (cast ((data.vcount_normal_max +
                         data.vcount_priority_max -
                         data.vcount_normal_min -
                         data.vcount_priority_min) as double))
             else
                 0
             end as per_error_count
    from (
        select
            bcr1.iom_sn,
            bcr1.pl,
            bcr1.pat_errcount,
            bcr1.flag_date,
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
        and
            a.timestamp = bcr1.dt
        and
            b.timestamp = bcr1.dt
        and
            c.timestamp = bcr1.dt
        and
            d.timestamp = bcr1.dt
        ) data
    ) data2
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s') as timestamp,
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    ad.timestamp_ms, 
    date_format(ad.date_created, '%Y%m%d%H%i%s') as date_created
from 
    aps_data_cte ad
order by 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s')
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
process_bcr1_data <- function(bcr1_results,
                              start_date,
                              end_date,
                              phm_patterns_sk,
                              threshold_type,
                              ihn_level3_desc,
                              algorithm_type,
                              error_count,
                              error_code_reg_expr,
                              threshold_description,
                              thresholds_days,
                              threshold_data_days)
{
    final_results <- empty_results();
    #
    return(final_results);
}
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
    upper(trim(am.sn)) as sn,
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
    ae.timestamp_iso between 
        (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
    and 
        dd.dt_max
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
),
vcount_1_min_cte as (
select
    ac.iom_sn,
    ac.timestamp,
    min(ac.max_value) as vcount_1_min
from
    ac_cte ac
inner join
    bcr23_cte bcr23
on
    ac.iom_sn = bcr23.iom_sn
where 
    ac.id = 'tubes' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'CentrifugeCounter' 
and
    ac.iom_sn = bcr23.iom_sn
and
    ac.timestamp = bcr23.dt
group by
    ac.iom_sn,
    ac.timestamp
),
vcount_1_max_cte as (
select
    ac.iom_sn,
    ac.timestamp,
    max(ac.max_value) as vcount_1_max
from
    ac_cte ac
inner join
    bcr23_cte bcr23
on
    ac.iom_sn = bcr23.iom_sn
where 
    ac.id = 'tubes' 
and 
    ac.duration = 'YTD'
and 
    ac.description = 'CentrifugeCounter' 
and
    ac.iom_sn = bcr23.iom_sn
and
    ac.timestamp = bcr23.dt
group by
    ac.iom_sn,
    ac.timestamp
),
aps_data_cte as (
select
    '<PHM_PATTERNS_SK>' as   phm_patterns_sk, 
    '<V_RUN_DATE>' as        run_date, 
    data2.iom_sn as          iom_sn, 
    data2.pl as              pl, 
    data2.iom_sn as          sn, 
    data2.flag_date as       timestamp, 
    '<ALGORITHM_TYPE>' as    algorithm_type, 
    data2.errors_per_day as  testcount, 
    data2.pat_errcount as    errorcount, 
    data2.per_error_count as errorpct, 
    0 as                     timestamp_ms, 
      now() as               date_created
from (
    select
        data.iom_sn,
        data.pl,
        data.flag_date,
        data.pat_errcount,
        (data.vcount_1_max - data.vcount_1_min) as errors_per_day,
        case when (data.vcount_1_max - data.vcount_1_min) > 0
             then
                 (cast (data.pat_errcount as double)*100.0)/
                 (cast ((data.vcount_1_max + data.vcount_1_min) as double))
             else
                 0
             end as per_error_count
    from (
        select
            bcr23.iom_sn,
            bcr23.pl,
            bcr23.pat_errcount,
            bcr23.flag_date,
            a.vcount_1_min,
            b.vcount_1_max
        from
            bcr23_cte bcr23,
            vcount_1_min_cte a,
            vcount_1_max_cte b
        where
            a.iom_sn = bcr23.iom_sn
        and
            b.iom_sn = bcr23.iom_sn
        and
            a.timestamp = bcr23.dt
        and
            b.timestamp = bcr23.dt
        ) data
    ) data2
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s') as timestamp,
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    ad.timestamp_ms, 
    date_format(ad.date_created, '%Y%m%d%H%i%s') as date_created
from 
    aps_data_cte ad
order by 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s')
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
process_bcr23_data <- function(bcr23_results,
                               start_date,
                               end_date,
                               phm_patterns_sk,
                               threshold_type,
                               ihn_level3_desc,
                               algorithm_type,
                               error_count,
                               error_code_reg_expr,
                               threshold_description,
                               thresholds_days,
                               threshold_data_days)
{
    final_results <- empty_results();
    #
    return(final_results);
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
    am.iom_sn = dd.iom_sn
and
    am.message like '%' || '<PATTERN_TEXT>' || '%'
where 
    ae.timestamp_iso  between
        (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
    and 
        dd.dt_max
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
),
aps_data_cte as (
select
    '<PHM_PATTERNS_SK>' as phm_patterns_sk, 
    '<V_RUN_DATE>' as      run_date, 
    c.iom_sn as            iom_sn, 
    c.pl as                pl, 
    c.module_sn as         sn, 
    c.ms_time as           timestamp, 
    '<ALGORITHM_TYPE>' as  algorithm_type, 
    0 as                   testcount, 
    c.errorcount as        errorcount, 
    0 as                   errorpct, 
    0 as                   timestamp_ms, 
    now() as               date_created
from
    count_cte c
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s') as timestamp,
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    ad.timestamp_ms, 
    date_format(ad.date_created, '%Y%m%d%H%i%s') as date_created
from 
    aps_data_cte ad
order by 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s')
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
process_count_data <- function(count_results,
                               start_date,
                               end_date,
                               phm_patterns_sk,
                               threshold_type,
                               ihn_level3_desc,
                               algorithm_type,
                               error_count,
                               error_code_reg_expr,
                               threshold_description,
                               thresholds_days,
                               threshold_data_days)
{
    flagged_results <- empty_results();
    #
    if (nrow(count_results) == 0) {
        return(flagged_results)
    }
    #
    curr_day <- NA
    prev_day <- NA
    consecutive_days <- TRUE
    curr_day_errcount <- 0
    flagging_days <- 0
    ihn_value <- NA
    total_error_count <- 0
    #
    current_sn <- " "
    #
    if (threshold_type == 'CONSECUTIVE') {
        for (irec in 1:nrow(count_results)) {
            #
            cr_record <- count_results[irec, ]
            write_data(cr_record, "debug_count_results_record.csv")
            write_data(cr_record, "debug_all_data.csv", TRUE)
            #
            if (nchar(cr_record[1,"SN"]) <= 0) {
                next
            }
            if (current_sn != cr_record[1,"SN"]) {
                curr_day <- NA
                prev_day <- NA
                consecutive_days <- TRUE
                curr_day_errcount <- 0
                flagging_days <- 0
                ihn_value <- NA
                total_error_count <- 0
            }
            #
            flag <- 'no'
            ihn_value <- NA
            v_flagged_pl <- NA
            v_flagged_exp_code <- NA
            #
            current_sn <- cr_record[1,"SN"]
            current_pl <- cr_record[1,"PL"]
            #
            curr_day <- cr_record[1,"TIMESTAMP"]
            curr_day_tstamp <- as.Date(curr_day, format="%Y%m%d")
            curr_day_errcount <- cr_record[1,"ERRORCOUNT"]
            #
            if (( ! is.na(prev_day)) && (curr_day_tstamp != (prev_day_tstamp + 1))) {
                consecutive_days <- FALSE
            }
            #
            if (consecutive_days && (curr_day_errcount >= error_count)) {
                flagging_days <- flagging_days + 1
            } else {
                flagging_days <- 0
            }
            #
            prev_day <- curr_day
            prev_day_tstamp <- curr_day_tstamp
            #
            if (flagging_days >= thresholds_days) {
                flag <- "yes"
                ihn_value <- ihn_level3_desc
                #
                flagged_record <- list(PHN_PATTERNS_SK=phm_patterns_sk,
                                       PL=current_pl,
                                       MODULESN=current_sn,
                                       FLAG_DATE=curr_day,
                                       CHART_DATA_VALUE=1,
                                       FLAG_YN=1,
                                       IHN_LEVEL3_DESC=ihn_value)
                flagged_results <- rbind(flagged_results,
                                         flagged_record,
                                         stringsAsFactors=FALSE)
                #
                print(sprintf("INFO: SN: %s FLAGGED", current_sn))
            }
        }
    } else if (threshold_type == 'COUNT') {
        for (irec in 1:nrow(count_results)) {
            #
            cr_record <- count_results[irec, ]
            write_data(cr_record, "debug_count_results_record.csv")
            write_data(cr_record, "debug_all_data.csv", TRUE)
            #
            if (nchar(cr_record[1,"SN"]) <= 0) {
                next
            }
            if (current_sn != cr_record[1,"SN"]) {
                curr_day <- NA
                prev_day <- NA
                consecutive_days <- TRUE
                curr_day_errcount <- 0
                flagging_days <- 0
                ihn_value <- NA
                total_error_count <- 0
            }
            #
            flag <- 'no'
            ihn_value <- NA
            v_flagged_pl <- NA
            v_flagged_exp_code <- NA
            #
            current_sn <- cr_record[1,"SN"]
            current_pl <- cr_record[1,"PL"]
            curr_day <- cr_record[1,"TIMESTAMP"]
            #
            curr_day_errcount <- cr_record[1,"ERRORCOUNT"]
            total_error_count <- total_error_count + curr_day_errcount
            #
            if (total_error_count >= error_count) {
                flag <- "yes"
                ihn_value <- ihn_level3_desc
                #
                flagged_record <- list(PHN_PATTERNS_SK=phm_patterns_sk,
                                       PL=current_pl,
                                       MODULESN=current_sn,
                                       FLAG_DATE=curr_day,
                                       CHART_DATA_VALUE=1,
                                       FLAG_YN=1,
                                       IHN_LEVEL3_DESC=ihn_value)
                flagged_results <- rbind(flagged_results,
                                         flagged_record,
                                         stringsAsFactors=FALSE)
                #
                print(sprintf("INFO: SN: %s FLAGGED", current_sn))
            }
        }
    } else if (threshold_type == 'DISCRETE') {
        for (irec in 1:nrow(count_results)) {
            #
            cr_record <- count_results[irec, ]
            write_data(cr_record, "debug_count_results_record.csv")
            write_data(cr_record, "debug_all_data.csv", TRUE)
            #
            if (nchar(cr_record[1,"SN"]) <= 0) {
                next
            }
            if (current_sn != cr_record[1,"SN"]) {
                curr_day <- NA
                prev_day <- NA
                consecutive_days <- TRUE
                curr_day_errcount <- 0
                flagging_days <- 0
                ihn_value <- NA
                total_error_count <- 0
            }
            #
            flag <- 'no'
            ihn_value <- NA
            v_flagged_pl <- NA
            v_flagged_exp_code <- NA
            #
            current_sn <- cr_record[1,"SN"]
            current_pl <- cr_record[1,"PL"]
            curr_day <- cr_record[1,"TIMESTAMP"]
            #
            curr_day_errcount <- cr_record[1,"ERRORCOUNT"]
            if (curr_day_errcount >= error_count) {
                flagging_days <- flagging_days + 1
            }
            if (flagging_days >= threshold_data_days) {
                flag <- "yes"
                ihn_value <- ihn_level3_desc
                #
                flagged_record <- list(PHN_PATTERNS_SK=phm_patterns_sk,
                                       PL=current_pl,
                                       MODULESN=current_sn,
                                       FLAG_DATE=curr_day,
                                       CHART_DATA_VALUE=1,
                                       FLAG_YN=1,
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
    return(flagged_results);
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
    ae2.timestamp_iso between
        (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
    and 
        dd.dt_max
and
    ae2.timestamp = ae.timestamp
where 
    ae.timestamp_iso between
        (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day)
    and 
        dd.dt_max
and
    ae.message = '<LAS_PATTERN_TEXT>' || ': Error 205: Unreadable Barcode' 
group by
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    max(am.pl)
),
lasx_min_count_cte as (
select
    ac.iom_sn,
    ac.timestamp,
    min(ac.max_value) as lasx_min_count
from
    ac_cte ac
inner join
    las205_cte las205
on
    ac.iom_sn = las205.iom_sn
where 
    ac.id = substr('<LAS_PATTERN_TEXT>', 4, 4)
and 
    ac.duration = 'YTD'
and 
    ac.description = 'InstrumentBufferCount' 
and
    ac.timestamp = las205.dt
group by
    ac.iom_sn,
    ac.timestamp
),
lasx_max_count_cte as (
select
    ac.iom_sn,
    ac.timestamp,
    max(ac.max_value) as lasx_max_count
from
    ac_cte ac
inner join
    las205_cte las205
on
    ac.iom_sn = las205.iom_sn
where 
    ac.id = substr('<LAS_PATTERN_TEXT>', 4, 4)
and 
    ac.duration = 'YTD'
and 
    ac.description = 'InstrumentBufferCount' 
and
    ac.timestamp = las205.dt
group by
    ac.iom_sn,
    ac.timestamp
),
aps_data_cte as (
select
    '<PHM_PATTERNS_SK>' as   phm_patterns_sk, 
    '<V_RUN_DATE>' as        run_date, 
    data2.iom_sn as          iom_sn, 
    data2.pl as              pl, 
    data2.sn as              sn, 
    data2.max_timestamp as   timestamp, 
    '<LAS_PATTERN_TEXT>' as  algorithm_type, 
    data2.errors_per_day as  testcount, 
    data2.las_error_count as errorcount, 
    data2.lasx_percentage as errorpct, 
    0 as                     timestamp_ms, 
    now() as                 date_created
from (
    select
        data.iom_sn,
        data.sn,
        data.pl,
        data.max_timestamp,
        data.las_error_count,
        (data.lasx_max_count - data.lasx_min_count) as errors_per_day,
        case when (data.lasx_max_count - data.lasx_min_count) > 0
             then
                 (cast (data.las_error_count as double)*100.0)/
                 (cast ((data.lasx_max_count + data.lasx_min_count) as double))
             else
                 0
             end as lasx_percentage
    from (
        select
            las205.iom_sn,
            las205.sn,
            las205.pl,
            las205.las_error_count,
            las205.max_timestamp,
            a.lasx_min_count,
            b.lasx_max_count
        from
            las205_cte las205,
            lasx_min_count_cte a,
            lasx_max_count_cte b
        where
            a.iom_sn = las205.iom_sn
        and
            b.iom_sn = las205.iom_sn
        and
            a.timestamp = las205.dt
        and
            b.timestamp = las205.dt
        ) data
    ) data2
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s') as timestamp,
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    ad.timestamp_ms, 
    date_format(ad.date_created, '%Y%m%d%H%i%s') as date_created
from 
    aps_data_cte ad
order by 
    ad.sn, 
    date_format(ad.timestamp, '%Y%m%d%H%i%s')
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
process_las205_data <- function(las205_results,
                                start_date,
                                end_date,
                                phm_patterns_sk,
                                threshold_type,
                                ihn_level3_desc,
                                algorithm_type,
                                error_count,
                                error_code_reg_expr,
                                threshold_description,
                                thresholds_days,
                                threshold_data_days)
{
    final_results <- empty_results();
    #
    return(final_results);
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
    # map to IDA names
    #
    pattern_text <- error_code_reg_expr
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
        #
        final_results <- process_bcr1_data(bcr1_results,
                                           start_date,
                                           end_date,
                                           phm_patterns_sk,
                                           threshold_type,
                                           ihn_level3_desc,
                                           algorithm_type,
                                           error_count,
                                           error_code_reg_expr,
                                           threshold_description,
                                           thresholds_days,
                                           threshold_data_days)
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
        #
        final_results <- process_bcr23_data(bcr23_results,
                                            start_date,
                                            end_date,
                                            phm_patterns_sk,
                                            threshold_type,
                                            ihn_level3_desc,
                                            algorithm_type,
                                            error_count,
                                            error_code_reg_expr,
                                            threshold_description,
                                            thresholds_days,
                                            threshold_data_days)
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
        #
        final_results <- process_count_data(count_results,
                                            start_date,
                                            end_date,
                                            phm_patterns_sk,
                                            threshold_type,
                                            ihn_level3_desc,
                                            algorithm_type,
                                            error_count,
                                            error_code_reg_expr,
                                            threshold_description,
                                            thresholds_days,
                                            threshold_data_days)
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
        #
        final_results <- process_las205_data(las205_results,
                                             start_date,
                                             end_date,
                                             phm_patterns_sk,
                                             threshold_type,
                                             ihn_level3_desc,
                                             algorithm_type,
                                             error_count,
                                             error_code_reg_expr,
                                             threshold_description,
                                             thresholds_days,
                                             threshold_data_days)
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
