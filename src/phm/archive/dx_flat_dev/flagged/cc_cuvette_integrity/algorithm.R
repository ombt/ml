#
# Alinity CC Cuvette Integrity
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
select
    final2.moduleserialnumber as modulesn,
    date_format(max(final2.flag_date),'%Y%m%d%H%i%s') as flag_date,
    final2.gt20000_gt20perc_sampevents,
    count(final2.moduleserialnumber) as count_moduleserialnumber
from (
    select
        middle2.*,
        case when (cast (middle2.num_sampevents_gt20000_percuv as double) / 
                   cast (middle2.num_sampevents_percuv as double)) > <CUVETTEINTEGRITY_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner2.moduleserialnumber,
            inner2.cuvettenumber,
            max(inner2.datetimestamplocal) as flag_date,
            count(inner2.cuvettenumber) as num_sampevents_percuv,
            sum(inner2.check_gt20000) as num_sampevents_gt20000_percuv
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
                case when sdp.dispensebeginaverage > <CUVETTEINTEGRITY_DISBEGAVG_MIN>
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
            and 
                r.cuvettenumber is not null
            where
                '<START_DATE>' <= sdp.transaction_date
            and 
                sdp.transaction_date < '<END_DATE>'
        ) inner2        
        group by
            inner2.moduleserialnumber,
            inner2.cuvettenumber
        order by
            inner2.moduleserialnumber,
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
    final2.moduleserialnumber,
    final2.gt20000_gt20perc_sampevents
having
    count(final2.moduleserialnumber) <= <CUVETTEINTEGRITY_NUMCUVETTES_MAX>
order by
    final2.moduleserialnumber,
    final2.gt20000_gt20perc_sampevents"
#
# number of days to check
#
number_of_days <- 7
#
# product line code for output file
#
product_line_code <- "210"

