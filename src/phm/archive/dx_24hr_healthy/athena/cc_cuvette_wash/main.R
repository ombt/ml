#
# Alinity CC Cuvette Wash
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
    final.moduleserialnumber as modulesn,
    date_format(max(final.flag_date),'%Y%m%d%H%i%s') as flag_date,
    final.gt20000_gt20perc_sampevents,
    count(final.moduleserialnumber) as count_moduleserialnumber
from (
    select
        middle1.*,
        case when (cast (middle1.num_sampevents_gt20000_percuv as double))/ 
                  (cast (middle1.num_sampevents_percuv as double)) > <CUVETTEWASH_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner1.moduleserialnumber,
            inner1.cuvettenumber,
            max(inner1.datetimestamplocal) as flag_date,
            count(inner1.cuvettenumber) as num_sampevents_percuv,
            sum(inner1.check_gt20000) as num_sampevents_gt20000_percuv
        from (
            select
                sdp.scmserialnumber,
                sdp.datetimestamplocal,
                sdp.dispensebeginaverage,
                sdp.samplekey,
                sdp.testnumber,
                sdp.replicatestart,
                sdp.replicatenumber,
                upper(trim(dpm.moduleserialnumber)) as moduleserialnumber,
                dpm.scmserialnumber,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.scmserialnumber,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > <CUVETTEWASH_DISBEGAVG_MIN>
                     then 1
                     else 0
                     end as check_gt20000
            from
                dx.dx_214_alinity_ci_ccsampledispensepcidata sdp
            left join 
                dx.dx_210_alinity_c_ccdispensepm dpm
            on 
                '<START_DATE>' <= dpm.transaction_date
            and 
                dpm.transaction_date < '<END_DATE>'
            and
                sdp.scmserialnumber = dpm.scmserialnumber
            and 
                dpm.datetimestamplocal
                between 
                    sdp.datetimestamplocal - interval '0.1' second 
                and 
                    sdp.datetimestamplocal + interval '0.1' second
            and 
                sdp.samplekey = dpm.samplekey
            and 
                sdp.testnumber = dpm.toshibatestnumber
            and 
                sdp.replicatestart = dpm.startingreplicatenumber
            and 
                sdp.replicatenumber = dpm.replicatenumber
            left join 
                dx.dx_210_alinity_c_result r
            on 
                '<START_DATE>' <= r.transaction_date
            and 
                r.transaction_date < '<END_DATE>'
            and
                dpm.scmserialnumber = r.scmserialnumber
            and 
                dpm.testid = r.testid
            where
                '<START_DATE>' <= sdp.transaction_date
            and 
                sdp.transaction_date < '<END_DATE>'
            and 
                r.cuvettenumber is not null
        ) inner1       
        group by
            inner1.moduleserialnumber,
            inner1.cuvettenumber
        ) middle1
    ) final
where
    final.gt20000_gt20perc_sampevents = <THRESHOLDS_COUNT>
group by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents
having
    count(final.moduleserialnumber) > <CUVETTEWASH_NUMCUVETTES_MIN>
order by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
from
    dx.dx_210_alinity_c_result dxr
where
    dxr.moduleserialnumber is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
#
chart_data_query_template <- "
select
    final.moduleserialnumber as modulesn,
    date_format(max(final.flag_date),'%Y%m%d%H%i%s') as flag_date,
    final.gt20000_gt20perc_sampevents,
    count(final.moduleserialnumber) as chart_data_value
from (
    select
        middle1.*,
        case when (cast (middle1.num_sampevents_gt20000_percuv as double))/ 
                  (cast (middle1.num_sampevents_percuv as double)) > <CUVETTEWASH_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner1.moduleserialnumber,
            inner1.cuvettenumber,
            max(inner1.datetimestamplocal) as flag_date,
            count(inner1.cuvettenumber) as num_sampevents_percuv,
            sum(inner1.check_gt20000) as num_sampevents_gt20000_percuv
        from (
            select
                sdp.scmserialnumber,
                sdp.datetimestamplocal,
                sdp.dispensebeginaverage,
                sdp.samplekey,
                sdp.testnumber,
                sdp.replicatestart,
                sdp.replicatenumber,
                upper(trim(dpm.moduleserialnumber)) as moduleserialnumber,
                dpm.scmserialnumber,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.scmserialnumber,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > <CUVETTEWASH_DISBEGAVG_MIN>
                     then 1
                     else 0
                     end as check_gt20000
            from
                dx.dx_214_alinity_ci_ccsampledispensepcidata sdp
            left join 
                dx.dx_210_alinity_c_ccdispensepm dpm
            on 
                '<START_DATE>' <= dpm.transaction_date
            and 
                dpm.transaction_date < '<END_DATE>'
            and
                sdp.scmserialnumber = dpm.scmserialnumber
            and 
                dpm.datetimestamplocal
                between 
                    sdp.datetimestamplocal - interval '0.1' second 
                and 
                    sdp.datetimestamplocal + interval '0.1' second
            and 
                sdp.samplekey = dpm.samplekey
            and 
                sdp.testnumber = dpm.toshibatestnumber
            and 
                sdp.replicatestart = dpm.startingreplicatenumber
            and 
                sdp.replicatenumber = dpm.replicatenumber
            left join 
                dx.dx_210_alinity_c_result r
            on 
                '<START_DATE>' <= r.transaction_date
            and 
                r.transaction_date < '<END_DATE>'
            and
                dpm.scmserialnumber = r.scmserialnumber
            and 
                dpm.testid = r.testid
            where
                '<START_DATE>' <= sdp.transaction_date
            and 
                sdp.transaction_date < '<END_DATE>'
            and 
                r.cuvettenumber is not null
        ) inner1       
        group by
            inner1.moduleserialnumber,
            inner1.cuvettenumber
        ) middle1
    ) final
where
    final.gt20000_gt20perc_sampevents = <THRESHOLDS_COUNT>
group by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents
order by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$COUNT_MODULESERIALNUMBER
    #
    return(flagged_results)
}
#
use_suppression <- TRUE
#
generate_suppression <- function(params, 
                                 rel_db_con, 
                                 options,
                                 test_period)
{
    query <- "
SELECT
  DISTINCT(UPPER(CALCULATEDSN)) AS MODULESN
FROM
  TICKETHEADER TH
INNER JOIN TICKETPRODUCT TP
  ON TH.TICKET_SQ = TP.TICKET_SQ
WHERE 
  TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '205'
      AND TP.ACTION_TAKEN IN ('N110', 'N120')
        AND TP.LIST_NUM LIKE ' A-30104916%'  
"
    #
    return(exec_query(params, rel_db_con, query, options, test_period))
}
#
#####################################################################
#
# start algorithm
#
main(7, 
     flagged_query_template, 
     modulesn_query_template, 
     chart_data_query_template,
     "210")
#
q(status=0)
