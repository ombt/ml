#
# Architect WAM Pattern B
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
    checkpoint("2019-07-01", 
               checkpointLocation=CHECKPOINT_LOCATION)
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
flagged_query_template <- "
select 
    rawb.sn as modulesn,
    rawb.wzprobe as wzprobeb,
    rawb.pl,
    max(rawb.flagb) as flagb,
    min(rawb.flagdateb) as flag_date
from (
    select 
        wzb.architect_moduleserial as sn,
        wzb.architect_productline as pl,
        cast(wzb.washzoneid as varchar) || '.' || wzb.position as wzprobe,
        wzb.eventdate_iso as flagdateb, 
        wzb.ambienttemp,
        case when 
                 wzb.ambienttemp < <MIN_VALUE> 
             and 
                 lag (wzb.ambienttemp) over 
                 (
                     partition by
                         wzb.architect_moduleserial, 
                         wzb.washzoneid, 
                         wzb.position 
                     order by 
                         wzb.eventdate_iso
                 ) < <MIN_VALUE>
             then 
                 'yes' 
             else 
                 'no' 
             end as flagb 
    from (
        select 
            wb.architect_moduleserial,
            wb.architect_productline,
            wb.eventdate_iso, 
            wb.washzoneid -1 as washzoneid,
            '1' as position, 
            wb.position1 as replicateid,
            case when 
                     wb.position1 = lag (wb.position1) over 
                     (
                         order by 
                             wb.architect_moduleserial, 
                             wb.position1, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     ) 
                 and 
                     wb.washzoneid = lag (wb.washzoneid) over 
                     (
                         order by 
                             wb.architect_moduleserial, 
                             wb.position1, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     )
                 and 
                     (wb.eventdate_iso - interval '10' second) < lag (wb.eventdate_iso) over 
                     (
                         order by 
                             wb.architect_moduleserial, 
                             wb.position1, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     )
                 then 
                     'probe 1 second temp'
                 else 
                     'probe 1 first temp' 
                 end as pip_order,
            ( wb.maxtempposition1 - wb.tempdeltaposition1)/1000 as ambienttemp
        from 
            dx.dx_architect_wam wb
        where 
            wb.position1 > 0 
        and 
            '<START_DATE>' <= wb.transaction_date
        and 
            wb.transaction_date < '<END_DATE>'
        union all 
        select 
            wb.architect_moduleserial,
            wb.architect_productline,
            wb.eventdate_iso,
            wb.washzoneid -1 as washzoneid,
            '2' as position,
            wb.position2 as replicateid,
            'probe 2' as pip_order,
            (wb.maxtempposition2 - wb.tempdeltaposition2)/1000 as ambienttemp
        from 
            dx.dx_architect_wam wb
        where 
            wb.position2 > 0 
        and 
            '<START_DATE>' <= wb.transaction_date
        and 
            wb.transaction_date < '<END_DATE>'
        union all 
        select 
            wb.architect_moduleserial, 
            wb.architect_productline,
            wb.eventdate_iso,
            wb.washzoneid -1 as washzoneid,
            '3' as position,
            wb.position3 as replicateid,
            case when 
                     wb.position3 = lag (wb.position3) over 
                     (
                         order by 
                             wb.architect_moduleserial, 
                             wb.position3, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     ) 
                 and 
                     wb.washzoneid = lag (wb.washzoneid) over 
                     (
                         order by 
                             wb.architect_moduleserial, 
                             wb.position3, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     ) 
                 and 
                     (wb.eventdate_iso - interval '10' second) < lag (wb.eventdate_iso) over 
                     (
                         order by 
                             wb.architect_moduleserial, 
                             wb.position3, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     )
                then 
                    'probe 3 second temp'
                else 
                    'probe 3 first temp' 
                end as pip_order,
                (wb.maxtempposition3 - wb.tempdeltaposition3)/1000 as ambienttemp
        from 
            dx.dx_architect_wam wb
        where 
            wb.position3 > 0 
        and 
            '<START_DATE>' <= wb.transaction_date
        and 
            wb.transaction_date < '<END_DATE>'
    ) wzb 
    where 
        not wzb.pip_order = 'probe 3 second temp' 
    and
        not wzb.pip_order = 'probe 1 first temp' 
) rawb
where 
    rawb.flagb = 'yes' 
group by 
    rawb.sn, 
    rawb.pl,
    rawb.wzprobe"
#
modulesn_query_template <- "
select
    distinct(dxr.architect_moduleserial) as modulesn
from
    dx.dx_architect_results dxr
where
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
reliability_query_template <- NA
#
spark_load_data <- function(db_conn,
                            param_sets, 
                            options,
                            test_period)
{
#     library(DBI)
#     #
#     results_tbl <- "dx_205_alinity_i_result"
#     results_uri_template <- "s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/205-alinity-i/Result/transaction_date=<START_DATE>"
#     results_uri <- query_subs(results_uri_template, test_period, "VALUE")
#     #
#     read_in <- spark_read_parquet(db_conn, 
#                                   results_tbl, 
#                                   results_uri)
}
#
#####################################################################
#
# start algorithm
#
main(1, 
     flagged_query_template, 
     modulesn_query_template, 
     reliability_query_template, 
     NA,
     "spark")
#
q(status=0)

