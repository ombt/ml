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
threshold_counts_query_template <- "
select 
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc (completiondate) as flag_date,
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today,
    max (ae.completiondate) as max_compl_date,
    count (ae.errorcode) as error_count,
    trunc ( (count (ae.errorcode) * 100 / ac.tubes_today), 2) as error_percentage
FROM 
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
    lower (n.sn) = lower ('<V_SN1>')
and
    asi.systeminfoid = n.systeminfoid
and
    ac.layout_nodes_id = n.layout_nodes_id
and
    ac.nodetype = ae.nodetype
and
    ac.counter_date = trunc (ae.completiondate)
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
    (('<V_NODETYPE1>' != '%' and ae.nodetype = '<V_NODETYPE1>') or 
     ('<V_NODETYPE1>' = '%' and ae.nodetype like '<V_NODETYPE1>'))
and
    ae.errorcode = '<V_ERRORCODE1>'
and
    nvl (ae.sampleid, <V_SAMP_ID_CHK1>) like
    nvl (<V_SAMP_ID_CHK2>, ae.sampleid)
and
    ae.completiondate between 
        to_timestamp('<V_START_DATE>', 'YYYY-MM-DD HH24:MI:SS') - <V_DATA_DAYS> + 1
    and
        to_timestamp('<V_END_DATE>', 'YYYY-MM-DD HH24:MI:SS')
group by
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc (completiondate),
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today
"
#
y_query_template <- "
select distinct 
    err.module_sn,
    err.device_id,
    il.customer_name as customername,
    il.customer_number as customernumber,
    il.city,
    il.country_code as countrycode,
    pc.country as countryname,
    pc.arearegion as area,
    pc.arearegion,
    err.pl
from (
    select 
        max(sn) as sn,
        max(pl) as pl,
        max(customer_num) as customer_number,
        max(customer) as customer_name,
        max(city) as city,
        max(country_code) as country_code
    from 
        instrumentlisting
    group by sn
    ) il,
    svc_phm_ods.phm_a3600_temp_error err,
    phm_country pc
where
    upper (il.sn) = err.module_sn
and 
    err.run_date = to_timestamp('<V_RUN_DATE>', 
                                'MM/DD/YYYY HH24:MI:SS') 
and 
    il.pl = err.pl
and 
    pc.country_code = il.country_code
and 
    err.phm_thresholds_sk = '<PHM_THRESHOLDS_SK>'
"
#
p_error_count_query_template <- "
select distinct 
    module_sn,
    phm_algorithm_definitions_sk,
    nodetype,
    errorcode,
    nodeid,
    instanceid
from 
    svc_phm_ods.phm_a3600_temp_error
where
    module_sn = upper('<Y_MODULE_SN>')
and
    phm_thresholds_sk = '<PHM_THRESHOLDS_SK>'
and
    run_date = to_timestamp('<V_RUN_DATE>', 
                            'MM/DD/YYYY HH24:MI:SS') 
"
#
p_percentage_query_template <- "
select distinct 
    module_sn,
    phm_thresholds_sk,
    nodetype,
    errorcode,
    nodeid,
    instanceid
from 
    svc_phm_ods.phm_a3600_temp_error
where
    module_sn = upper('<Y_MODULE_SN>')
and
    phm_thresholds_sk = '<PHM_THRESHOLDS_SK>'
and
    run_date = to_timestamp('<V_RUN_DATE>', 
                            'MM/DD/YYYY HH24:MI:SS') 
"
#
p_sd_low_volume_query_template <- "
select distinct 
    module_sn,
    phm_thresholds_sk,
    nodetype,
    errorcode,
    nodeid,
    instanceid
from 
    svc_phm_ods.phm_a3600_temp_error
where
    module_sn = upper('<Y_MODULE_SN>')
and
    phm_thresholds_sk = '<PHM_THRESHOLDS_SK>'
and
    run_date = to_timestamp('<V_RUN_DATE>', 
                            'MM/DD/YYYY HH24:MI:SS') 
"
#
x_error_count_query_template <- "
select 
    *
from 
    svc_phm_ods.phm_a3600_temp_error
where
    module_sn = upper('<P_MODULE_SN>')
and 
    errorcode = '<P_ERRORCODE>'
and 
    run_date = to_timestamp('<V_RUN_DATE>', 
                            'MM/DD/YYYY HH24:MI:SS') 
and 
    phm_thresholds_sk = '<Z_PHM_THRESHOLDS_SK>'
and 
    nodetype = '<P_NODETYPE>'
and 
    instanceid = '<P_INSTANCEID>'
and 
    nodeid = <P_NODEID>
order by 
    flag_date
"
#
y_sd_high_volume_query_template <- "
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
        max(sn) as sn,
        max(pl) as pl,
        max(customer_num) as customer_number,
        max(customer) as customer_name,
        max(city) as city,
        max(country_code) as country_code
    from 
        instrumentlisting
    group by sn) i,
    svc_phm_ods.phm_a3600_temp_error e,
    phm_country pc
where
    upper(i.sn) = e.module_sn
and 
    e.run_date = to_timestamp('<V_RUN_DATE>', 
                              'MM/DD/YYYY HH24:MI:SS') 
and 
    i.pl = e.pl
and 
    pc.country_code = i.country_code
and 
    e.phm_algorithm_definitions_sk = <V_ALG_NUM>
"
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
# substitute into threshold counts query
#
gen_tc_query <- function(query_template,
                         iddr, dd_results,
                         params,
                         v_samp_id_chk1,
                         v_samp_id_chk2)
{
    tc_query <- query_template
    #
    tc_query <- gsub('<V_SN1>', 
                     dd_results[iddr, "SN"], 
                     tc_query, 
                     fixed = TRUE)
    tc_query <- gsub('<V_NODETYPE1>', 
                     params["MODULE", "PARAMETER_VALUE"],
                     tc_query, 
                     fixed = TRUE)
    tc_query <- gsub('<V_ERRORCODE1>', 
                     params["ERROR_CODE_VALUE", "PARAMETER_VALUE"],
                     tc_query, 
                     fixed = TRUE)
    tc_query <- gsub('<V_START_DATE>', 
                     sub('\\.[0-9]*$', 
                         '', 
                         dd_results[iddr, "MIN_COMPL_DATE"], 
                         fixed=FALSE),
                     tc_query, 
                     fixed = TRUE)
    tc_query <- gsub('<V_END_DATE>', 
                     sub('\\.[0-9]*$', 
                         '', 
                         dd_results[iddr, "MAX_COMPL_DATE"], 
                         fixed=FALSE),
                     tc_query, 
                     fixed = TRUE)
    tc_query <- gsub('<V_DATA_DAYS>', 
                     params["THRESHOLD_DATA_DAYS", "PARAMETER_VALUE"],
                     tc_query, 
                     fixed = TRUE)
    tc_query <- gsub('<V_SAMP_ID_CHK1>', 
                     v_samp_id_chk1,
                     tc_query, 
                     fixed = TRUE)
    tc_query <- gsub('<V_SAMP_ID_CHK2>', 
                     v_samp_id_chk2,
                     tc_query, 
                     fixed = TRUE)
    return(tc_query)
}
#
gen_y_query <- function(query_template,
                        test_period,
                        params)
{
    y_query <- query_template
    y_query <- gsub('<V_RUN_DATE>', 
                    test_period["START_DATE", "VALUE"],
                    y_query, 
                    fixed = TRUE)
    y_query <- gsub('<PHM_THRESHOLDS_SK>',
                    params[1,"PHM_PATTERNS_SK"],
                    y_query, 
                    fixed = TRUE)
    return(y_query)
}
#
gen_p_query <- function(query_template,
                        iyr, y_results,
                        test_period,
                        params)
{
    p_query <- query_template
    p_query <- gsub('<Y_MODULE_SN>', 
                    y_results[iyr, "MODULE_SN"], 
                    p_query, 
                    fixed = TRUE)
    p_query <- gsub('<V_RUN_DATE>', 
                    test_period["START_DATE", "VALUE"],
                    p_query, 
                    fixed = TRUE)
    p_query <- gsub('<PHM_THRESHOLDS_SK>',
                    params[1,"PHM_PATTERNS_SK"],
                    p_query, 
                    fixed = TRUE)
    return(p_query)
}
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
    # threshold parameter machinations
    #
    module_type <- params["MODULE", "PARAMETER_VALUE"]
    pattern_description <- params["ERROR_CODE_VALUE", "PARAMETER_VALUE"]
    algorithm_type <- params["ALGORITHM_TYPE", "PARAMETER_VALUE"]
    #
    if (((pattern_description == "0405") && (module_type == "IOM")) ||
        ((pattern_description == "0605") && (module_type == "CM"))) {
        v_samp_id_chk1 <- "NULL"
        v_samp_id_chk2 <- "'amp;U__'"
    } else if ((pattern_description == "5015") && (module_type == "ISR")) {
        v_samp_id_chk1 <- "NULL"
        v_samp_id_chk2 <- "'%'"
    } else {
        v_samp_id_chk1 <- "' '"
        v_samp_id_chk2 <- "'%'"
    }
    write_data(params, "params.csv")
    write_data(params, "all_data.csv", TRUE)
    #
    # device and dates machinations
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
    # get the threshold counts
    #
    empty_df <- data.frame()
    #
    for (iddr in 1:nrow(dd_results)) {
        #
        # generate threshold counts query
        #
        tc_query <- gen_tc_query(threshold_counts_query_template,
                                 iddr, dd_results,
                                 params,
                                 v_samp_id_chk1,
                                 v_samp_id_chk2)
        save_to_file(tc_query, "query.sql")
        tc_query <- gsub("[\n\r]", " ", tc_query)
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
            #
            y_query <- gen_y_query(y_query_template,
                                   test_period,
                                   params)
            y_results <- exec_query(empty_df,
                                    db_conn, 
                                    y_query,
                                    options, 
                                    empty_df)
            #
            if (errors$occurred()) {
                next
            }
            else if (nrow(y_results) <= 0) {
                next
            }
            write_data(y_results, "y_results.csv")
            write_data(y_results, "all_data.csv", TRUE)
            #
            # loop over y-query results
            #
            for (iyr in 1:nrow(y_results)) {
                #
                # generate p query
                #
                p_query <- gen_p_query(p_error_count_query_template,
                                       iyr, y_results,
                                       test_period,
                                       params)
                save_to_file(p_query, "query.sql")
                p_query <- gsub("[\n\r]", " ", p_query)
                #
                p_results <- exec_query(empty_df,
                                        db_conn, 
                                        p_query,
                                        options, 
                                        empty_df)
                #
                if (errors$occurred()) {
                    next
                }
                else if (nrow(p_results) <= 0) {
                    next
                }
                write_data(p_results, "p_error_count.csv")
                write_data(p_results, "all_data.csv", TRUE)
                #
                # loop over p-query results
                #
                for (ipr in 1:nrow(p_results)) {
                    #
                    # generate x query
                    #
                    x_query <- gen_p_query(x_error_count_query_template,
                                           ipr, p_results,
                                           test_period,
                                           params)
                    save_to_file(p_query, "query.sql")
                    x_query <- gsub("[\n\r]", " ", x_query)
                    #
                    x_results <- exec_query(empty_df,
                                            db_conn, 
                                            x_query,
                                            options, 
                                            empty_df)
                    #
                    if (errors$occurred()) {
                        next
                    }
                    else if (nrow(p_results) <= 0) {
                        next
                    }
                    write_data(x_results, "x_error_count.csv")
                    write_data(x_results, "all_data.csv", TRUE)
                    #
                    # loop over x-query results
                    #
                    curr_day          <- NA
                    prev_day          <- NA
                    consecutive_days  <- TRUE
                    curr_day_errcount <- 0
                    flagging_days     <- 0
                    flag              <- 'no'
                    ihn_value         <- NA
                    v_insert_count    <- 0
                    total_error_count <- 0
                    #
                    for (ixr in 1:nrow(x_results)) {
                        flag <- 'no'
                        ihn_value <- NA
                        v_flagged_pl <- NA
                        v_flagged_exp_code <- NA
                        #
                        curr_day <- x.flag_date
                        curr_day_errcount <- x.errorcount
                        #
                        if (( ! is.na(prev_day)) && (curr_day != (prev_day + 1))) {
                            consecutive_days <- false
                        }
                        #
                        if (onsecutive_days &&
                            (curr_day_errcount >= z.threshold_number)) {
                            flagging_days <- flagging_days + 1
                            prev_day <- curr_day
                        }
                        #
                        if (curr_day_errcount < z.threshold_number) {
                            flagging_days <- 0
                            prev_day <- NA
                            consecutive_days <- true
                        }
                        #
                        if ((flagging_days >= z.threshold_number_unit) &&
                            (curr_day_errcount >= z.threshold_number)) {
                            flag <- 'yes'
                            ihn_value <- z.threshold_alert
                            #
                            v_flagged_pl <- null
                            v_flagged_exp_code <- null
                            #
                            phm_algorithm_utilities_1.phm_get_pl_exp_code (
                                z.phm_thresholds_sk,
                                y.pl,
                                null,
                                v_flagged_pl,
                                v_flagged_exp_code)
                        }
                    }
                }
            }
        } else if (algorithm_type == "PERCENTAGE") {
            print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
            #
            y_query <- gen_y_query(y_query_template,
                                   test_period,
                                   params)
            y_results <- exec_query(empty_df,
                                    db_conn, 
                                    y_query,
                                    options, 
                                    empty_df)
            #
            if (errors$occurred()) {
                next
            }
            else if (nrow(y_results) <= 0) {
                next
            }
            write_data(y_results, "y_results.csv")
            write_data(y_results, "all_data.csv", TRUE)
            #
            # loop over y-query results
            #
            for (iyr in 1:nrow(y_results)) {
                #
                # generate p query
                #
                p_query <- gen_p_query(p_percentage_query_template,
                                       iyr, y_results,
                                       test_period,
                                       params)
                save_to_file(p_query, "query.sql")
                p_query <- gsub("[\n\r]", " ", p_query)
                #
                p_results <- exec_query(empty_df,
                                        db_conn, 
                                        p_query,
                                        options, 
                                        empty_df)
                #
                if (errors$occurred()) {
                    next
                }
                else if (nrow(p_results) <= 0) {
                    next
                }
                write_data(p_results, "p_percentage.csv")
                write_data(p_results, "all_data.csv", TRUE)
                #
                # loop over p-query results
                #
                for (ipr in 1:nrow(p_results)) {
                    curr_day          <- NA
                    prev_day          <- NA
                    consecutive_days  <- TRUE
                    curr_day_errcount <- 0
                    flagging_days     <- 0
                    flag              <- 'no'
                    ihn_value         <- NA
                    v_insert_count    <- 0
                    total_error_count <- 0
                    #
                    # generate d query
                    #
                    d_query <- gen_d_query(d_percentage_query_template,
                                           ipr, p_results,
                                           test_period,
                                           params)
                    save_to_file(d_query, "query.sql")
                    d_query <- gsub("[\n\r]", " ", d_query)
                    #
                    d_results <- exec_query(empty_df,
                                            db_conn, 
                                            d_query,
                                            options, 
                                            empty_df)
                    #
                    if (errors$occurred()) {
                        next
                    }
                    else if (nrow(d_results) <= 0) {
                        next
                    }
                    write_data(d_results, "d_percentage.csv")
                    write_data(d_results, "all_data.csv", TRUE)
                    #
                    # loop over d-query results
                    #
                    for (idr in 1:nrow(d_results)) {
                        flag <- 'no'
                        ihn_value <- null
                        v_flagged_pl <- null
                        v_flagged_exp_code <- null
                        #
                        curr_day <- d.flag_date
                        curr_day_errcount <- d.errorpct
                        #
                        if (( ! is.na(prev_day) && 
                            (curr_day != (prev_day + 1))) {
                            consecutive_days <- FALSE
                        }
                        #
                        if (consecutive_days &&
                           (curr_day_errcount >= z.threshold_number)) {
                            flagging_days <- flagging_days + 1
                        }
                        #
                        prev_day <- curr_day
                        #
                        if (curr_day_errcount < z.threshold_number) {
                            flagging_days <- 0
                            prev_day <- NA
                            consecutive_days <- TRUE
                        }
                        #
                        if ((flagging_days >= z.threshold_number_unit) &&
                            (curr_day_errcount >= z.threshold_number)) {
                            flag <- 'yes'
                            ihn_value <- z.threshold_alert
                        }
                        #
                        v_flagged_pl <- null
                        v_flagged_exp_code <- null
                        #
                        phm_algorithm_utilities_1.phm_get_pl_exp_code (
                           z.phm_thresholds_sk,
                           y.pl,
                           null,
                           v_flagged_pl,
                           v_flagged_exp_code)
                    }
                }
            }
        } else if (algorithm_type == "SD_HIGH_VOLUME") {
            print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
            #
            y_query <- gen_y_query(y_sd_high_volume_query_template,
                                   test_period,
                                   params)
            y_results <- exec_query(empty_df,
                                    db_conn, 
                                    y_query,
                                    options, 
                                    empty_df)
            #
            if (errors$occurred()) {
                next
            }
            else if (nrow(y_results) <= 0) {
                next
            }
            write_data(y_results, "y_sd_high_volume_results.csv")
            write_data(y_results, "all_data.csv", TRUE)
            #
            # loop over y-query results
            #
            for (iyr in 1:nrow(y_results)) {
                #
                # generate p query
                #
                p_query <- gen_p_query(p_percentage_query_template,
                                       iyr, y_results,
                                       test_period,
                                       params)
                save_to_file(p_query, "query.sql")
                p_query <- gsub("[\n\r]", " ", p_query)
                #
                p_results <- exec_query(empty_df,
                                        db_conn, 
                                        p_query,
                                        options, 
                                        empty_df)
                #
                if (errors$occurred()) {
                    next
                }
                else if (nrow(p_results) <= 0) {
                    next
                }
         FOR Y
            IN (SELECT DISTINCT E.MODULE_SN,
                                E.DEVICE_ID,
                                I.CUSTOMER_NAME CUSTOMERNAME,
                                I.CUSTOMER_NUMBER CUSTOMERNUMBER,
                                I.CITY,
                                I.COUNTRY_CODE COUNTRYCODE,
                                PC.COUNTRY COUNTRYNAME,
                                PC.AREAREGION AREA,
                                PC.AREAREGION,
                                E.PL
                  FROM (  SELECT MAX (SN) SN,
                                 MAX (PL) PL,
                                 MAX (CUSTOMER_NUM) CUSTOMER_NUMBER,
                                 MAX (CUSTOMER) CUSTOMER_NAME,
                                 MAX (CITY) City,
                                 MAX (COUNTRY_CODE) COUNTRY_CODE
                            FROM INSTRUMENTLISTING
                        GROUP BY sn) I,
                       SVC_PHM_ODS.PHM_A3600_TEMP_ERROR E,
                       PHM_COUNTRY PC
                 WHERE     UPPER (I.SN) = E.MODULE_SN
                       AND E.BATCH_NUM = V_BATCH_NUM
                       AND E.RUN_DATE = V_RUN_DATE
                       AND I.PL = E.PL
                       AND PC.COUNTRY_CODE = I.COUNTRY_CODE
                       AND E.PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM)
         LOOP
            FOR P
               IN (SELECT DISTINCT MODULE_SN,
                                   PHM_THRESHOLDS_SK,
                                   NODETYPE,
                                   ERRORCODE,
                                   NODEID,
                                   INSTANCEID
                     FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                    WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                          AND BATCH_NUM = V_BATCH_NUM
                          AND RUN_DATE = V_RUN_DATE
                          AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK)
            LOOP
               FLAG := 'NO';
               IHN_VALUE := '';
               TODAY_TEST_COUNT := 0;
               FILTER_COUNT := 0;
               TODAY_ERRORPCT := 0;
               PREV_DAY_ERRORPCT := 0;
               V_THRESHOLD_LIMIT := 100000;
               CONSEQ_COUNT := 0;

               SELECT TRUNC (MIN (AE.COMPLETIONDATE)) MIN_COMPL_DATE
                 INTO V_REQ_START_DATE
                 FROM SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
                      A3600_LAYOUT_NODES_PL_SN ALN,
                      IDAOWNER.A3600SYSTEMINFORMATION ASI
                WHERE     BATCH_NUM = V_BATCH_NUM
                      AND RUN_DATE = V_RUN_DATE
                      AND ALN.SN = Y.MODULE_SN
                      AND AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
                      AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
                      AND ALN.CANID = AE.NODEID
                      AND ASI.CURRENT_ROW = 'Y';


               FOR D
                  IN (  SELECT *
                          FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                         WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                               AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                               AND NODETYPE = P.NODETYPE
                               AND ERRORCODE = P.ERRORCODE
                               AND INSTANCEID = P.INSTANCEID
                               AND NODEID = P.NODEID
                               AND BATCH_NUM = V_BATCH_NUM
                               AND RUN_DATE = V_RUN_DATE
                      ORDER BY FLAG_DATE)
               LOOP
                  BEGIN
                     IF TRUNC (D.FLAG_DATE) >= V_REQ_START_DATE
                     THEN
                        SELECT TRUNC (
                                  ABS (AVG (TESTCOUNT) - STDDEV (TESTCOUNT)),
                                  0)
                          INTO FILTER_COUNT
                          FROM (  SELECT *
                                    FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                   WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                         AND PHM_THRESHOLDS_SK =
                                                Z.PHM_THRESHOLDS_SK
                                         AND NODETYPE = P.NODETYPE
                                         AND ERRORCODE = P.ERRORCODE
                                         AND INSTANCEID = P.INSTANCEID
                                         AND NODEID = P.NODEID
                                         AND BATCH_NUM = V_BATCH_NUM
                                         AND RUN_DATE = V_RUN_DATE
                                         --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                         AND FLAG_DATE <= TRUNC (D.FLAG_DATE)
                                ORDER BY FLAG_DATE DESC)
                         WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                        IF FILTER_COUNT > 0
                        THEN
                           SELECT MIN (FLAG_DATE)
                             INTO V_DATE_30TH
                             FROM (  SELECT *
                                       FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                      WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                            AND BATCH_NUM = V_BATCH_NUM
                                            AND RUN_DATE = V_RUN_DATE
                                            AND PHM_THRESHOLDS_SK =
                                                   Z.PHM_THRESHOLDS_SK
                                            AND NODETYPE = P.NODETYPE
                                            AND ERRORCODE = P.ERRORCODE
                                            AND INSTANCEID = P.INSTANCEID
                                            AND NODEID = P.NODEID
                                            --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                            AND FLAG_DATE <=
                                                   TRUNC (D.FLAG_DATE)
                                   ORDER BY FLAG_DATE DESC)
                            WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                           -- Get TODAY_TEST_COUNT
                           SELECT TESTCOUNT
                             INTO TODAY_TEST_COUNT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND FLAG_DATE = TRUNC (D.FLAG_DATE);

                           -- GET TODAY_ERRORPCT
                           SELECT NVL (ERRORPCT, 0)
                             INTO TODAY_ERRORPCT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND PHM_THRESHOLDS_SK = V_ALG_NUM
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND TRUNC (FLAG_DATE) = TRUNC (D.FLAG_DATE);

                           FLAG := 'NO';
                           IHN_VALUE := '';

                           IF TODAY_TEST_COUNT >= FILTER_COUNT
                           THEN
                              SELECT   STDDEV (ERRORPCT) * Z.THRESHOLD_NUMBER
                                     + AVG (ERRORPCT)
                                INTO V_THRESHOLD_LIMIT
                                FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                               WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                     AND BATCH_NUM = V_BATCH_NUM
                                     AND RUN_DATE = V_RUN_DATE
                                     AND PHM_THRESHOLDS_SK =
                                            Z.PHM_THRESHOLDS_SK
                                     AND NODETYPE = P.NODETYPE
                                     AND ERRORCODE = P.ERRORCODE
                                     AND INSTANCEID = P.INSTANCEID
                                     AND NODEID = P.NODEID
                                     AND TESTCOUNT > FILTER_COUNT
                                     AND FLAG_DATE BETWEEN V_DATE_30TH
                                                       AND TRUNC (
                                                              D.FLAG_DATE);

                              IF V_THRESHOLD_LIMIT > 0
                              THEN
                                 IF TODAY_ERRORPCT >= V_THRESHOLD_LIMIT
                                 THEN
                                    CONSEQ_COUNT := 0;
                                    FLAG := 'NO';
                                    IHN_VALUE := '';

                                    --DBMS_OUTPUT.PUT_LINE('in  '||'v_prev_date'||V_PREV_DATE||' Z.THRESHOLD_NUMBER_UNIT '|| Z.THRESHOLD_NUMBER_UNIT
                                    --||'   ' ||'d.flag_date '||TRUNC(D.FLAG_DATE));
                                    FOR I
                                       IN (  SELECT *
                                               FROM (  SELECT *
                                                         FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                                        WHERE     MODULE_SN =
                                                                     UPPER (
                                                                        Y.MODULE_SN)
                                                              AND BATCH_NUM =
                                                                     V_BATCH_NUM
                                                              AND RUN_DATE =
                                                                     V_RUN_DATE
                                                              AND PHM_THRESHOLDS_SK =
                                                                     Z.PHM_THRESHOLDS_SK
                                                              AND NODETYPE =
                                                                     P.NODETYPE
                                                              AND ERRORCODE =
                                                                     P.ERRORCODE
                                                              AND INSTANCEID =
                                                                     P.INSTANCEID
                                                              AND NODEID =
                                                                     P.NODEID
                                                              AND TESTCOUNT >
                                                                     FILTER_COUNT
                                                              AND FLAG_DATE <=
                                                                     TRUNC (
                                                                        D.FLAG_DATE)
                                                     ORDER BY FLAG_DATE DESC)
                                              WHERE ROWNUM <=
                                                       Z.THRESHOLD_NUMBER_UNIT
                                           ORDER BY FLAG_DATE DESC)
                                    LOOP
                                       IF I.ERRORPCT >= V_THRESHOLD_LIMIT
                                       THEN
                                          CONSEQ_COUNT := CONSEQ_COUNT + 1;
                                       ELSE
                                          CONSEQ_COUNT := 0;
                                       END IF;
                                    END LOOP;

                                    IF CONSEQ_COUNT >=
                                          Z.THRESHOLD_NUMBER_UNIT
                                    THEN
                                       FLAG := 'YES';
                                       IHN_VALUE := Z.THRESHOLD_ALERT;

                                       V_FLAGGED_PL := NULL;
                                       V_FLAGGED_EXP_CODE := NULL;

                                       PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE (
                                          V_ALG_NUM,
                                          Y.PL,
                                          NULL,
                                          V_FLAGGED_PL,
                                          V_FLAGGED_EXP_CODE);
                                    ELSE
                                       FLAG := 'NO';
                                       IHN_VALUE := '';
                                    END IF;
                                 ELSE
                                    FLAG := 'NO';
                                    IHN_VALUE := '';
                                 END IF;
                              ELSE
                                 FLAG := 'NO';
                                 IHN_VALUE := '';
                              END IF;
                           ELSE
                              FLAG := 'NO';
                              IHN_VALUE := '';
                           END IF;
                        ELSE
                           TODAY_ERRORPCT := 0;
                           FLAG := 'NO';
                           IHN_VALUE := NULL;
                        END IF;
                     ELSE
                        TODAY_ERRORPCT := 0;
                        FLAG := 'NO';
                        IHN_VALUE := NULL;
                     END IF;
        } else if (algorithm_type == "SD_LOW_VOLUME") {
            print(sprintf("INFO: Algorithm Type: %s", algorithm_type))
            #
            y_query <- gen_y_query(y_query_template,
                                   test_period,
                                   params)
            y_results <- exec_query(empty_df,
                                    db_conn, 
                                    y_query,
                                    options, 
                                    empty_df)
            #
            if (errors$occurred()) {
                next
            }
            else if (nrow(y_results) <= 0) {
                next
            }
            write_data(y_results, "y_results.csv")
            write_data(y_results, "all_data.csv", TRUE)
            #
            # loop over y-query results
            #
            for (iyr in 1:nrow(y_results)) {
                #
                # generate p query
                #
                p_query <- gen_p_query(p_sd_low_volume_query_template,
                                       iyr, y_results,
                                       test_period,
                                       params)
                save_to_file(p_query, "query.sql")
                p_query <- gsub("[\n\r]", " ", p_query)
                #
                p_results <- exec_query(empty_df,
                                        db_conn, 
                                        p_query,
                                        options, 
                                        empty_df)
                #
                if (errors$occurred()) {
                    next
                }
                else if (nrow(p_results) <= 0) {
                    next
                }
                write_data(p_results, "p_sd_low_volume.csv")
                write_data(p_results, "all_data.csv", TRUE)
            FOR P
               IN (SELECT DISTINCT MODULE_SN,
                                   PHM_THRESHOLDS_SK,
                                   NODETYPE,
                                   ERRORCODE,
                                   NODEID,
                                   INSTANCEID
                     FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                    WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                          AND BATCH_NUM = V_BATCH_NUM
                          AND RUN_DATE = V_RUN_DATE
                          AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK)
            LOOP
               FLAG := 'NO';
               IHN_VALUE := '';
               TODAY_TEST_COUNT := 0;
               FILTER_COUNT := 0;
               TODAY_ERRORPCT := 0;
               PREV_DAY_ERRORPCT := 0;
               V_THRESHOLD_LIMIT := 100000;
               CONSEQ_COUNT := 0;

               SELECT TRUNC (MIN (AE.COMPLETIONDATE)) MIN_COMPL_DATE
                 INTO V_REQ_START_DATE
                 FROM SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
                      A3600_LAYOUT_NODES_PL_SN ALN,
                      IDAOWNER.A3600SYSTEMINFORMATION ASI
                WHERE     AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
                      AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
                      AND ALN.CANID = AE.NODEID
                      AND ASI.CURRENT_ROW = 'Y'
                      AND BATCH_NUM = V_BATCH_NUM
                      AND RUN_DATE = V_RUN_DATE
                      AND ALN.SN = Y.MODULE_SN;


               FOR D
                  IN (  SELECT *
                          FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                         WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                               AND BATCH_NUM = V_BATCH_NUM
                               AND RUN_DATE = V_RUN_DATE
                               AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                               AND NODETYPE = P.NODETYPE
                               AND ERRORCODE = P.ERRORCODE
                               AND INSTANCEID = P.INSTANCEID
                               AND NODEID = P.NODEID
                      ORDER BY FLAG_DATE)
               LOOP
                  BEGIN
                     IF TRUNC (D.FLAG_DATE) >= V_REQ_START_DATE
                     THEN
                        SELECT TRUNC (
                                  ABS (AVG (TESTCOUNT) - STDDEV (TESTCOUNT)),
                                  0)
                          INTO FILTER_COUNT
                          FROM (  SELECT *
                                    FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                   WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                         AND BATCH_NUM = V_BATCH_NUM
                                         AND RUN_DATE = V_RUN_DATE
                                         AND PHM_THRESHOLDS_SK =
                                                Z.PHM_THRESHOLDS_SK
                                         AND NODETYPE = P.NODETYPE
                                         AND ERRORCODE = P.ERRORCODE
                                         AND INSTANCEID = P.INSTANCEID
                                         AND NODEID = P.NODEID
                                         --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                         AND FLAG_DATE <= TRUNC (D.FLAG_DATE)
                                ORDER BY FLAG_DATE DESC)
                         WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                        IF FILTER_COUNT > 0
                        THEN
                           SELECT MIN (FLAG_DATE)
                             INTO V_DATE_30TH
                             FROM (  SELECT *
                                       FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                      WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                            AND BATCH_NUM = V_BATCH_NUM
                                            AND RUN_DATE = V_RUN_DATE
                                            AND PHM_THRESHOLDS_SK =
                                                   Z.PHM_THRESHOLDS_SK
                                            AND NODETYPE = P.NODETYPE
                                            AND ERRORCODE = P.ERRORCODE
                                            AND INSTANCEID = P.INSTANCEID
                                            AND NODEID = P.NODEID
                                            --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                            AND FLAG_DATE <=
                                                   TRUNC (D.FLAG_DATE)
                                   ORDER BY FLAG_DATE DESC)
                            WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                           -- Get TODAY_TEST_COUNT
                           SELECT TESTCOUNT
                             INTO TODAY_TEST_COUNT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND FLAG_DATE = TRUNC (D.FLAG_DATE);

                           -- GET TODAY_ERRORPCT
                           SELECT NVL (ERRORPCT, 0)
                             INTO TODAY_ERRORPCT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND PHM_THRESHOLDS_SK = V_ALG_NUM
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND TRUNC (FLAG_DATE) = TRUNC (D.FLAG_DATE);

                           FLAG := 'NO';
                           IHN_VALUE := '';

                           IF TODAY_TEST_COUNT >= FILTER_COUNT
                           THEN
                              SELECT   STDDEV (ERRORPCT) * Z.THRESHOLD_NUMBER
                                     + AVG (ERRORPCT)
                                INTO V_THRESHOLD_LIMIT
                                FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                               WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                     AND BATCH_NUM = V_BATCH_NUM
                                     AND RUN_DATE = V_RUN_DATE
                                     AND PHM_THRESHOLDS_SK = V_ALG_NUM
                                     AND NODETYPE = P.NODETYPE
                                     AND ERRORCODE = P.ERRORCODE
                                     AND INSTANCEID = P.INSTANCEID
                                     AND NODEID = P.NODEID
                                     AND TESTCOUNT < FILTER_COUNT
                                     AND FLAG_DATE BETWEEN V_DATE_30TH
                                                       AND TRUNC (
                                                              D.FLAG_DATE);

                              IF V_THRESHOLD_LIMIT > 0
                              THEN
                                 IF TODAY_ERRORPCT >= V_THRESHOLD_LIMIT
                                 THEN
                                    CONSEQ_COUNT := 0;
                                    FLAG := 'NO';
                                    IHN_VALUE := '';

                                    --DBMS_OUTPUT.PUT_LINE('in  '||'v_prev_date'||V_PREV_DATE||' Z.THRESHOLD_NUMBER_UNIT '|| Z.THRESHOLD_NUMBER_UNIT
                                    --||'   ' ||'d.flag_date '||TRUNC(D.FLAG_DATE));

                                    FOR I
                                       IN (  SELECT *
                                               FROM (  SELECT *
                                                         FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                                        WHERE     MODULE_SN =
                                                                     UPPER (
                                                                        Y.MODULE_SN)
                                                              AND BATCH_NUM =
                                                                     V_BATCH_NUM
                                                              AND RUN_DATE =
                                                                     V_RUN_DATE
                                                              AND PHM_THRESHOLDS_SK =
                                                                     Z.PHM_THRESHOLDS_SK
                                                              AND NODETYPE =
                                                                     P.NODETYPE
                                                              AND ERRORCODE =
                                                                     P.ERRORCODE
                                                              AND INSTANCEID =
                                                                     P.INSTANCEID
                                                              AND NODEID =
                                                                     P.NODEID
                                                              AND TESTCOUNT <
                                                                     FILTER_COUNT
                                                              AND FLAG_DATE <=
                                                                     TRUNC (
                                                                        D.FLAG_DATE)
                                                     ORDER BY FLAG_DATE DESC)
                                              WHERE ROWNUM <=
                                                       Z.THRESHOLD_NUMBER_UNIT
                                           ORDER BY FLAG_DATE DESC)
                                    LOOP
                                       IF I.ERRORPCT >= V_THRESHOLD_LIMIT
                                       THEN
                                          CONSEQ_COUNT := CONSEQ_COUNT + 1;
                                       ELSE
                                          CONSEQ_COUNT := 0;
                                       END IF;
                                    END LOOP;

                                    IF CONSEQ_COUNT >=
                                          Z.THRESHOLD_NUMBER_UNIT
                                    THEN
                                       FLAG := 'YES';
                                       IHN_VALUE := Z.THRESHOLD_ALERT;

                                       V_FLAGGED_PL := NULL;
                                       V_FLAGGED_EXP_CODE := NULL;

                                       PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE (
                                          V_ALG_NUM,
                                          Y.PL,
                                          NULL,
                                          V_FLAGGED_PL,
                                          V_FLAGGED_EXP_CODE);
                                    ELSE
                                       FLAG := 'NO';
                                       IHN_VALUE := '';
                                    END IF;
                                 ELSE
                                    FLAG := 'NO';
                                    IHN_VALUE := '';
                                 END IF;
                              ELSE
                                 FLAG := 'NO';
                                 IHN_VALUE := '';
                              END IF;
                           ELSE
                              FLAG := 'NO';
                              IHN_VALUE := '';
                           END IF;
                        ELSE
                           TODAY_ERRORPCT := 0;
                           FLAG := 'NO';
                           IHN_VALUE := NULL;
                        END IF;
                     ELSE
                        TODAY_ERRORPCT := 0;
                        FLAG := 'NO';
                        IHN_VALUE := NULL;
                     END IF;
            }
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
