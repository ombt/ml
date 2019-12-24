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
flagged_query_template <- "
select
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents,
    to_char(max(final2.flag_date), 'YYYYMMDDHH24MISS') as flag_date,
    count(final2.modulesn) as count_modulesn
from (
    select
        middle2.*,
        (middle2.num_sampevents_gt20000_percuv / 
         middle2.num_sampevents_percuv) as perc_sampevents_gt20000_percuv,
        case when (middle2.num_sampevents_gt20000_percuv / 
                   middle2.num_sampevents_percuv) > <CUVETTEINTEGRITY_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner2.modulesn,
            inner2.cuvettenumber,
            max(inner2.logdate_local) as flag_date,
            count(inner2.cuvettenumber) as num_sampevents_percuv,
            sum(inner2.check_gt20000) as num_sampevents_gt20000_percuv
        from (
            select
                sdp.systemsn,
                sdp.logdate_local,
                sdp.dispensebeginaverage,
                sdp.samplekey,
                sdp.testnumber,
                sdp.replicatestart,
                sdp.replicatenumber,
                dpm.modulesn,
                dpm.systemsn,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.systemsn,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > <CUVETTEINTEGRITY_DISBEGAVG_MIN>
                     then 1
                     else 0
                     end as check_gt20000
            from
                idaqowner.icq_ccsampledispci sdp
            left join 
                idaqowner.icq_ccdispensepm dpm
            on 
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= dpm.logdate_local
            and 
                dpm.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
            and
                sdp.systemsn = dpm.systemsn
            and 
                dpm.logdate_local 
                between 
                    sdp.logdate_local - interval '0.1' second 
                and 
                    sdp.logdate_local + interval '0.1' second
            and 
                sdp.samplekey = dpm.samplekey
            and 
                sdp.testnumber = dpm.toshibatestnumber
            and 
                sdp.replicatestart = dpm.startingreplicatenumber
            and 
                sdp.replicatenumber = dpm.replicatenumber
            left join 
                idaqowner.icq_results r
            on 
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
            and 
                r.logdate_local < to_timestamp('<END_DATE>', 
                                               'MM/DD/YYYY HH24:MI:SS')
            and
                dpm.systemsn = r.systemsn
            and 
                dpm.testid = r.testid
            and 
                r.cuvettenumber is not null
            where
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= sdp.logdate_local
            and 
                sdp.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
        ) inner2        
        group by
            inner2.modulesn,
            inner2.cuvettenumber
        ) middle2
    where
        middle2.num_sampevents_percuv > <CUVETTEINTEGRITY_SAMPEVENTS_MIN>
    and 
        middle2.cuvettenumber 
        between 
            <CUVETTEINTEGRITY_SEGMENT1>
        and 
            <CUVETTEINTEGRITY_SEGMENT2>
    ) final2
where
    final2.gt20000_gt20perc_sampevents = <THRESHOLDS_COUNT>
group by
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents
having
    count(final2.modulesn) <= <CUVETTEINTEGRITY_NUMCUVETTES_MAX>
order by
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents"
#
not_flagged_query_template <- "
select
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents,
    to_char(max(final2.flag_date), 'YYYYMMDD') as flag_date,
    count(final2.modulesn) as count_modulesn
from (
    select
        middle2.*,
        (middle2.num_sampevents_gt20000_percuv / 
         middle2.num_sampevents_percuv) as perc_sampevents_gt20000_percuv,
        case when (middle2.num_sampevents_gt20000_percuv / 
                   middle2.num_sampevents_percuv) > <CUVETTEINTEGRITY_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner2.modulesn,
            inner2.cuvettenumber,
            max(inner2.logdate_local) as flag_date,
            count(inner2.cuvettenumber) as num_sampevents_percuv,
            sum(inner2.check_gt20000) as num_sampevents_gt20000_percuv
        from (
            select
                sdp.systemsn,
                sdp.logdate_local,
                sdp.dispensebeginaverage,
                sdp.samplekey,
                sdp.testnumber,
                sdp.replicatestart,
                sdp.replicatenumber,
                dpm.modulesn,
                dpm.systemsn,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.systemsn,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > <CUVETTEINTEGRITY_DISBEGAVG_MIN>
                     then 1
                     else 0
                     end as check_gt20000
            from
                idaqowner.icq_ccsampledispci sdp
            left join 
                idaqowner.icq_ccdispensepm dpm
            on 
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= dpm.logdate_local
            and 
                dpm.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
            and
                sdp.systemsn = dpm.systemsn
            and 
                dpm.logdate_local 
                between 
                    sdp.logdate_local - interval '0.1' second 
                and 
                    sdp.logdate_local + interval '0.1' second
            and 
                sdp.samplekey = dpm.samplekey
            and 
                sdp.testnumber = dpm.toshibatestnumber
            and 
                sdp.replicatestart = dpm.startingreplicatenumber
            and 
                sdp.replicatenumber = dpm.replicatenumber
            left join 
                idaqowner.icq_results r
            on 
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
            and 
                r.logdate_local < to_timestamp('<END_DATE>', 
                                               'MM/DD/YYYY HH24:MI:SS')
            and
                dpm.systemsn = r.systemsn
            and 
                dpm.testid = r.testid
            and 
                r.cuvettenumber is not null
            where
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= sdp.logdate_local
            and 
                sdp.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
        ) inner2        
        group by
            inner2.modulesn,
            inner2.cuvettenumber
        ) middle2
    where
        not ( middle2.num_sampevents_percuv > <CUVETTEINTEGRITY_SAMPEVENTS_MIN> )
    and 
        middle2.cuvettenumber 
        between 
            <CUVETTEINTEGRITY_SEGMENT1>
        and 
            <CUVETTEINTEGRITY_SEGMENT2>
    ) final2
where
    not ( final2.gt20000_gt20perc_sampevents = <THRESHOLDS_COUNT> )
group by
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents
having
    not ( count(final2.modulesn) <= <CUVETTEINTEGRITY_NUMCUVETTES_MAX> )
order by
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents"
#
#####################################################################
#
# start algorithm
#
main(7, flagged_query_template, TRUE, "210", "ida")
# main(7, not_flagged_query_template, FALSE, "210", "ida")
#
q(status=0)

