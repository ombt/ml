#
# Alinity CC Cuvette Integrity
#
#####################################################################
#
# required libraries
#
library(getopt)
library(DBI)
library(RJDBC)
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
common_utils_path <- file.path(".", "common_utils.R")
if ( ! file.exists(common_utils_path)) {
    if (nchar(Sys.getenv("DEV_ROOT")) == 0) {
        stop("No 'common_utils.R' found")
    }
    common_utils_path <- file.path(Sys.getenv("DEV_ROOT"),
                                   "rlib",
                                   "common_utils.R")
    if ( ! file.exists(common_utils_path)) {
        stop("No DEV_ROOT 'common_utils.R' found")
    }
}
source(common_utils_path)
#
#####################################################################
#
# algorithm specific
#
one_query_template <- "
select
    eval2.moduleserialnumber as modulesn,
    case when (count(eval2.cuvettenumber) <= <CUVETTEINTEGRITY_NUMCUVETTES_MAX>)
         then 1
         else 0
         end as num_cuvettes_max,
    date_format(max(eval2.flag_date),'%Y%m%d%H%i%s') as flag_date,
    eval2.num_sampevents_percuv,
    eval2.num_sampevents_gt20000_percuv,
    eval2.num_sampevents_percuv_gt_20,
    sum (eval2.gt20000_gt20perc_sampevents) as num_gt20000_gt20perc_sampevents
from (
    select
        eval.moduleserialnumber,
        eval.cuvettenumber,
        eval.flag_date,
        eval.num_sampevents_percuv,
        eval.num_sampevents_gt20000_percuv,
        case when (eval.num_sampevents_percuv > <CUVETTEINTEGRITY_SAMPEVENTS_MIN>)
             then 1
             else 0
             end as num_sampevents_percuv_gt_20,
        case when ((cast (eval.num_sampevents_gt20000_percuv as double) / 
                    cast (eval.num_sampevents_percuv as double)) > <CUVETTEINTEGRITY_PERCSAMPEVENTS_MIN>)
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            raw.moduleserialnumber,
            raw.cuvettenumber,
            max(raw.datetimestamplocal) as flag_date,
            count(raw.cuvettenumber) as num_sampevents_percuv,
            sum(case when (raw.dispensebeginaverage > <CUVETTEINTEGRITY_DISBEGAVG_MIN>)
                     then 1
                     else 0
                     end) as num_sampevents_gt20000_percuv
        from (
            select
                r.moduleserialnumber,
                r.testid,
                r.cuvettenumber,
                r.datetimestamplocal,
                sdp.dispensebeginaverage
            from
                dx.dx_210_alinity_c_result r
            inner join 
                dx.dx_210_alinity_c_ccdispensepm dpm
            on 
                '<START_DATE>' <= dpm.transaction_date
            and 
                dpm.transaction_date < '<END_DATE>'
            and
                r.moduleserialnumber = dpm.moduleserialnumber
            and
                r.scmserialnumber = dpm.scmserialnumber
            and
                r.testid = dpm.testid
            inner join
                dx.dx_214_alinity_ci_ccsampledispensepcidata sdp
            on
                '<START_DATE>' <= sdp.transaction_date
            and 
                sdp.transaction_date < '<END_DATE>'
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
            where 
                '<START_DATE>' <= r.transaction_date
            and 
                r.transaction_date < '<END_DATE>'
            and 
                r.cuvettenumber is not null
            and 
                r.cuvettenumber 
                between 
                    <CUVETTEINTEGRITY_SEGMENT1>
                and
                    <CUVETTEINTEGRITY_SEGMENT2>
            ) raw
        group by
            raw.moduleserialnumber,
            raw.cuvettenumber
        order by
            raw.moduleserialnumber,
            raw.cuvettenumber
        ) eval
    ) eval2
group by
    eval2.moduleserialnumber,
    eval2.num_sampevents_percuv,
    eval2.num_sampevents_gt20000_percuv,
    eval2.num_sampevents_percuv_gt_20
order by 
    eval2.moduleserialnumber"
#
post_processing <- function(results,
                            params, 
                            db_conn, 
                            query, 
                            options, 
                            test_period, 
                            flagged)
{
    #
    # flagged modulesn records
    #
    flagged <- with(results, ((NUM_CUVETTES_MAX >= 1) &
                              (NUM_SAMPEVENTS_PERCUV_GT_20 >= 1) &
                              (NUM_GT20000_GT20PERC_SAMPEVENTS >= 1)))
    #
    return(flagged_post_processing(results, flagged))
}
#
#####################################################################
#
# start algorithm
#
main(7, one_query_template, FALSE, "210")
#
q(status=0)
