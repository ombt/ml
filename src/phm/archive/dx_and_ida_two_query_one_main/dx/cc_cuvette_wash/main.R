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
                dpm.moduleserialnumber,
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
where
    final.gt20000_gt20perc_sampevents = <THRESHOLD_COUNT>
group by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents
having
    count(final.moduleserialnumber) > <CUVETTEWASH_NUMCUVETTES_MIN>
order by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents"
#
not_flagged_query_template <- "
select
    final.moduleserialnumber as modulesn,
    date_format(max(final.flag_date),'%Y%m%d') as flag_date,
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
                dpm.moduleserialnumber,
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
where
    not ( final.gt20000_gt20perc_sampevents = <THRESHOLD_COUNT> )
group by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents
having
    not ( count(final.moduleserialnumber) > <CUVETTEWASH_NUMCUVETTES_MIN> )
order by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents"
#
#####################################################################
#
# start algorithm
#
main("dx", 7, flagged_query_template, not_flagged_query_template, "210")
#
q(status=0)
