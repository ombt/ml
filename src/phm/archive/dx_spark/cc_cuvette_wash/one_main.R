#
# Alinity CC Cuvette Wash
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
                dpm.moduleserialnumber,
                r.cuvettenumber,
                sdp.datetimestamplocal,
                case when sdp.dispensebeginaverage > <CUVETTEWASH_DISBEGAVG_MIN>
                     then 1
                     else 0
                     end as check_gt20000
            from
                dx.dx_214_ccsampledispensepcidata sdp
            left join 
                dx.dx_210_ccdispensepm dpm
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
                dx.dx_210_result r
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
group by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents
order by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents"
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
    # algorithm parameters
    #
    threshold_count <- 
        params["THRESHOLD_COUNT","PARAMETER_VALUE"]
    cuvettewash_numcuvettes_min <- 
        params["CUVETTEWASH_NUMCUVETTES_MIN","PARAMETER_VALUE"]
    #
    # flagged modulesn records
    #
    flagged <- ((results$GT20000_GT20PERC_SAMPEVENTS == threshold_count) &
                (results$COUNT_MODULESERIALNUMBER > cuvettewash_numcuvettes_min))
    #
    return(flagged_post_processing(results, flagged))
}
#
#####################################################################
#
# start algorithm
#
main(7, one_query_template, FALSE, "210", "spark")
#
q(status=0)
