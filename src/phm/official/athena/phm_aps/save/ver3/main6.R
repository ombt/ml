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
order by 1, 2
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
ebcr1_cte as (
select
    max(ae.productline) as pl, 
    ae.serialnumber as iom_sn, 
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
    (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
group by 
    ae.serialnumber,
    date_trunc('day', ae.timestamp_iso)
),
aps_data as (
select
    '<PHM_PATTERNS_SK>' as phm_patterns_sk, 
    '<V_RUN_DATE>' as run_date, 
    y.iom_sn as iom_sn, 
    y.pl as pl, 
    y.iom_sn as sn, 
    y.flag_date as timestamp, 
    '<ALGORITHM_TYPE>' as algorithm_type, 
    errors_per_day as testcount, 
    y.pat_errcount as errorcount, 
    per_error_count as errorpct, 
    0 as timestamp_ms, 
    now() as date_created
from (
    select
        eb.iom_sn,
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
            ebcr1_cte.iom_sn,
            a.min_max_value as vcount_normal_min,
            b.max_max_value as vcount_normal_max,
            c.min_max_value as vcount_priority_min,
            d.max_max_value as vcount_priority_max 
        from (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.iom_sn
            ) a,
            (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.iom_sn
            ) b,
            (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.iom_sn
            ) c,
            (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.iom_sn
            ) d,
            ebcr1_cte
        where
            a.iom_sn = ebcr1_cte.iom_sn
        and
            b.iom_sn = ebcr1_cte.iom_sn
        and
            c.iom_sn = ebcr1_cte.iom_sn
        and
            d.iom_sn = ebcr1_cte.iom_sn
        ) e
    where
        eb.iom_sn = e.iom_sn
    ) aps_data,
    ebcr1_cte as y
where
    aps_data.iom_sn = y.iom_sn
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    ad.timestamp, 
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    timestamp_ms, 
    ad.date_created
from 
    aps_data ad
order by
    ad.sn,
    ad.timestamp
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
order by 1, 2
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
ebcr23_cte as (
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
),
aps_data as (
select
    '<PHM_PATTERNS_SK>' as phm_patterns_sk, 
    '<V_RUN_DATE>' as run_date, 
    y.iom_sn as iom_sn, 
    y.pl as pl, 
    y.iom_sn as sn, 
    y.flag_date as timestamp, 
    '<ALGORITHM_TYPE>' as algorithm_type, 
    errors_per_day as testcount, 
    y.pat_errcount as errorcount, 
    per_error_count as errorpct, 
    0 as timestamp_ms, 
    now() as date_created
from (
    select
        eb.iom_sn,
        eb.pat_errcount,
        (e.vcount_1_max + e.vcount_1_min) as errors_per_day,
        case when (e.vcount_1_max - e.vcount_1_min) > 0
             then
                 (eb.pat_errcount*100.0) /
                 (e.vcount_1_max - e.vcount_1_min)
             else
                 0
             end as per_error_count
    from 
        ebcr23_cte eb,
        (
        select distinct
            ebcr23_cte.iom_sn,
            a.max_value as vcount_1_min,
            b.max_value as vcount_1_max
        from (
            select 
                ebcr23_cte.iom_sn,
                ac_cte.max_value
            from
                ac_cte,
                ebcr23_cte
            where 
                ac_cte.id = 'tubes' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'CentrifugeCounter' 
            and 
                ac_cte.iom_sn = ebcr23_cte.iom_sn 
            and 
                ac_cte.dt <= ebcr23_cte.dt
            order by 
                ebcr23_cte.iom_sn,
                ac_cte.dt asc
            limit 1
            ) a,
            (
            select 
                ebcr23_cte.iom_sn,
                ac_cte.max_value
            from 
                ac_cte,
                ebcr23_cte
            where 
                ac_cte.id = 'tubes' 
            and 
                ac_cte.duration = 'YTD'
            and 
                ac_cte.description = 'CentrifugeCounter' 
            and 
                ac_cte.iom_sn = ebcr23_cte.iom_sn 
            and 
                ac_cte.dt = (ebcr23_cte.dt + interval '1' day)
            order by
                ebcr23_cte.iom_sn,
                ac_cte.dt desc
            limit 1
            ) b,
            ebcr23_cte
        where
            a.iom_sn = ebcr23_cte.iom_sn
        and
            b.iom_sn = ebcr23_cte.iom_sn
        ) e
    where
        eb.iom_sn = e.iom_sn
    ) aps_data,
    ebcr23_cte as y
where
    aps_data.iom_sn = y.iom_sn
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    ad.timestamp, 
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    timestamp_ms, 
    ad.date_created
from 
    aps_data ad
order by
    ad.sn,
    ad.timestamp
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
get_count_data_query <- function(start_date, 
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
order by 1, 2
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
ebcr1_cte as (
select
    max(ae.productline) as pl, 
    ae.serialnumber as iom_sn, 
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
    (dd.dt - interval '<THRESHOLD_DATA_DAYS>' day) <= ae.timestamp_iso 
and 
    ae.timestamp_iso < dd.dt_max
group by 
    ae.serialnumber,
    date_trunc('day', ae.timestamp_iso)
),
aps_data as (
select
    '<PHM_PATTERNS_SK>' as phm_patterns_sk, 
    '<V_RUN_DATE>' as run_date, 
    y.iom_sn as iom_sn, 
    y.pl as pl, 
    y.iom_sn as sn, 
    y.flag_date as timestamp, 
    '<ALGORITHM_TYPE>' as algorithm_type, 
    errors_per_day as testcount, 
    y.pat_errcount as errorcount, 
    per_error_count as errorpct, 
    0 as timestamp_ms, 
    now() as date_created
from (
    select
        eb.iom_sn,
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
            ebcr1_cte.iom_sn,
            a.min_max_value as vcount_normal_min,
            b.max_max_value as vcount_normal_max,
            c.min_max_value as vcount_priority_min,
            d.max_max_value as vcount_priority_max 
        from (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.iom_sn
            ) a,
            (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.iom_sn
            ) b,
            (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ebcr1_cte.dt <= ac_cte.dt
            group by
                ebcr1_cte.iom_sn
            ) c,
            (
            select 
                ebcr1_cte.iom_sn,
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
                ac_cte.iom_sn = ebcr1_cte.iom_sn 
            and 
                ac_cte.dt = (ebcr1_cte.dt + interval '1' day)
            group by
                ebcr1_cte.iom_sn
            ) d,
            ebcr1_cte
        where
            a.iom_sn = ebcr1_cte.iom_sn
        and
            b.iom_sn = ebcr1_cte.iom_sn
        and
            c.iom_sn = ebcr1_cte.iom_sn
        and
            d.iom_sn = ebcr1_cte.iom_sn
        ) e
    where
        eb.iom_sn = e.iom_sn
    ) aps_data,
    ebcr1_cte as y
where
    aps_data.iom_sn = y.iom_sn
)
select 
    ad.phm_patterns_sk, 
    ad.run_date, 
    ad.iom_sn, 
    ad.pl, 
    ad.sn, 
    ad.timestamp, 
    ad.algorithm_type, 
    ad.testcount, 
    ad.errorcount, 
    ad.errorpct, 
    timestamp_ms, 
    ad.date_created
from 
    aps_data ad
order by
    ad.sn,
    ad.timestamp
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
            return(empty_results())
        }
        #
        names(bcr1_results) <- toupper(names(bcr1_results))
        write_data(bcr1_results, "debug_bcr1_data.csv")
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
                                            algorithm_type,
                                            error_code_reg_expr)
        bcr23_results <- exec_query(params,
                                    db_conn, 
                                    bcr23_query,
                                    options, 
                                    test_period)
        #
        if (errors$occurred()) {
            print(sprintf("INFO: Error reading BCR_23 data")) 
            return(empty_results())
        } else if (nrow(bcr23_results) <= 0) {
            print(sprintf("INFO: No BCR_23 records found"))
            return(empty_results())
        }
        #
        names(bcr23_results) <- toupper(names(bcr23_results))
        write_data(bcr23_results, "debug_bcr23_data.csv")
        #
        print(sprintf("INFO: %d BCR23 records found", nrow(bcr23_results)))
    } else if (algorithm_type == "COUNT") {
        print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
        #
        count_query <- get_count_data_query(start_date, 
                                            end_date,
                                            threshold_data_days,
                                            phm_patterns_sk,
                                            end_date, 
                                            algorithm_type)
        count_results <- exec_query(params,
                                    db_conn, 
                                    count_query,
                                    options, 
                                    test_period)
        #
        if (errors$occurred()) {
            print(sprintf("INFO: Error reading count data")) 
            return(empty_results())
        } else if (nrow(count_results) <= 0) {
            print(sprintf("INFO: No COUNT records found"))
            return(empty_results())
        }
        #
        names(count_results) <- toupper(names(count_results))
        write_data(count_results, "debug_count_data.csv")
        #
        print(sprintf("INFO: %d COUNT records found", nrow(bcr23_results)))
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
