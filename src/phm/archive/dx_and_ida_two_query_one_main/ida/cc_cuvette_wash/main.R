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
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source libs
#
rlibpath <- Sys.getenv("PHM_ROOT")
if (nchar(rlibpath) == 0) {
    stop("PHM_ROOT not defined")
}
source(file.path(rlibpath,"rlib","common_utils.R"))
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select
    final.modulesn,
    final.gt20000_gt20perc_sampevents,
    to_char(max(final.flag_date), 'YYYYMMDDHH24MISS') as flag_date,
    count(final.modulesn) as count_modulesn
from (
    select
        middle1.*,
        case when (middle1.num_sampevents_gt20000_percuv / 
                   middle1.num_sampevents_percuv) > <CUVETTEWASH_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner1.modulesn,
            inner1.cuvettenumber,
            max(inner1.logdate_local) as flag_date,
            count(inner1.cuvettenumber) as num_sampevents_percuv,
            sum(inner1.check_gt20000) as num_sampevents_gt20000_percuv
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
                case when sdp.dispensebeginaverage > <CUVETTEWASH_DISBEGAVG_MIN>
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
            where
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= sdp.logdate_local
            and 
                sdp.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
            and 
                r.cuvettenumber is not null
            ) inner1       
        group by
            inner1.modulesn,
            inner1.cuvettenumber
        ) middle1
    ) final
where
    final.gt20000_gt20perc_sampevents = <THRESHOLD_COUNT>
group by
    final.modulesn,
    final.gt20000_gt20perc_sampevents
having
    count(final.modulesn) > <CUVETTEWASH_NUMCUVETTES_MIN>
order by
    final.modulesn,
    final.gt20000_gt20perc_sampevents"
#
not_flagged_query_template <- "
select
    final.modulesn,
    final.gt20000_gt20perc_sampevents,
    to_char(max(final.flag_date), 'YYYYMMDD') as flag_date,
    count(final.modulesn) as count_modulesn
from (
    select
        middle1.*,
        case when (middle1.num_sampevents_gt20000_percuv / 
                   middle1.num_sampevents_percuv) > <CUVETTEWASH_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner1.modulesn,
            inner1.cuvettenumber,
            max(inner1.logdate_local) as flag_date,
            count(inner1.cuvettenumber) as num_sampevents_percuv,
            sum(inner1.check_gt20000) as num_sampevents_gt20000_percuv
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
                case when sdp.dispensebeginaverage > <CUVETTEWASH_DISBEGAVG_MIN>
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
            where
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= sdp.logdate_local
            and 
                sdp.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
            and 
                r.cuvettenumber is not null
            ) inner1        
        group by
            inner1.modulesn,
            inner1.cuvettenumber
        ) middle1
    ) final
where
    not ( final.gt20000_gt20perc_sampevents = <THRESHOLD_COUNT> )
group by
    final.modulesn,
    final.gt20000_gt20perc_sampevents
having
    not ( count(final.modulesn) > <CUVETTEWASH_NUMCUVETTES_MIN> )
order by
    final.modulesn,
    final.gt20000_gt20perc_sampevents"
#
#####################################################################
#
# start algorithm
#
main("ida", 7, flagged_query_template, not_flagged_query_template, "210")
#
q(status=0)

