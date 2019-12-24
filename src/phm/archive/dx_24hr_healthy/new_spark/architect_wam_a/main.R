#
# Architect WAM Pattern A
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
flagged_query_template <- "
select 
    rawa.sn as modulesn,
    rawa.wzprobe as wzprobea,
    rawa.pl,
    max(rawa.flaga) as flaga,
    min(rawa.flagdatea) as flag_date
from (
    select 
        wza.architect_moduleserial as sn,
        wza.architect_productline as pl,
        cast(wza.washzoneid as varchar) || '.' || wza.position as wzprobe,
        wza.eventdate_iso as flagdatea,
        wza.maxtemp,
        case when 
                 wza.maxtemp > <ERROR_CODE_VALUE> 
             and 
                 lag (wza.maxtemp) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE> 
             and
                 lag (wza.maxtemp, 2) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE> 
             and
                 lag (wza.maxtemp, 3) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE> 
             and
                 lag (wza.maxtemp, 4) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE>
             then 
                 'yes' 
             else 
                 'no' 
             end as flaga 
    from (
        select 
            upper(trim(wa.architect_moduleserial)) as architect_moduleserial,
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid - 1 as washzoneid,
            '1' as position,
            wa.position1 as replicateid,
            case when 
                     wa.position1 = lag (wa.position1) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position1, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     wa.washzoneid = lag (wa.washzoneid) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position1, 
                             wa.washzoneid, 
                             wa.eventdate_iso
                     ) 
                 and 
                     (wa.eventdate_iso - interval '10' second) < lag (wa.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position1, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     )
                 then 
                     'probe 1 second temp'
                 else 
                     'probe 1 first temp' 
                 end as pip_order, 
            wa.maxtempposition1/1000 maxtemp
        from 
            dx.dx_architect_wam wa
        where 
            wa.position1 > 0 
        and 
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
        union all 
        select 
            upper(trim(wa.architect_moduleserial)),
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid -1 as washzoneid,
            '2' as position,
            wa.position2 as replicateid,
            'probe 2' as pip_order,
            wa.maxtempposition2/1000 maxtemp
        from 
            dx.dx_architect_wam wa
        where 
            wa.position2 > 0 
        and 
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
        union all 
        select 
            upper(trim(wa.architect_moduleserial)),
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid -1 as washzoneid,
            '3' as position,
            wa.position3 as replicateid,
            case when 
                     wa.position3 = lag (wa.position3) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     wa.washzoneid = lag (wa.washzoneid) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     (wa.eventdate_iso - interval '10' second) < lag (wa.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     )
                 then 
                     'probe 3 second temp'
                 else 
                     'probe 3 first temp' end 
                 as pip_order,
            wa.maxtempposition3/1000 maxtemp
        from 
            dx.dx_architect_wam wa
        where 
            wa.position3 > 0 
        and 
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
    ) wza 
    where 
        not wza.pip_order = 'probe 3 second temp' 
    and
        not wza.pip_order = 'probe 1 first temp' 
) rawa
where 
    rawa.flaga = 'yes' 
group by 
    rawa.sn, 
    rawa.pl,
    rawa.wzprobe"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.architect_moduleserial))) as modulesn
from
    dx.dx_architect_results dxr
where
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
chart_data_query_template <- NA
#
spark_load_data <- function(db_conn,
                            param_sets, 
                            options,
                            test_period)
{
# dx.dx_architect_wam wa
# dx.dx_architect_results dxr
    library(DBI)
    #
    results_tbl <- "dx_205_alinity_i_result"
    results_uri_template <- "s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/205-alinity-i/Result/transaction_date=<START_DATE>"
    results_uri <- query_subs(results_uri_template, test_period, "VALUE")
    #
    read_in <- spark_read_parquet(db_conn, 
                                  results_tbl, 
                                  results_uri)
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
     "spark")
#
q(status=0)
