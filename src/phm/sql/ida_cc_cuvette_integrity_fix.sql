select
    final2.deviceid,
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents,
    count(final2.modulesn) as count_modulesn
from (
    select
        middle2.*,
        (middle2.num_sampevents_gt20000_percuv / 
         middle2.num_sampevents_percuv) as perc_sampevents_gt20000_percuv,
        case when (middle2.num_sampevents_gt20000_percuv / 
                   middle2.num_sampevents_percuv) > 0.2
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner2.deviceid,
            inner2.modulesn,
            inner2.cuvettenumber,
            count(inner2.cuvettenumber) as num_sampevents_percuv,
            sum(inner2.check_gt20000) as num_sampevents_gt20000_percuv
        from (
            select
                sdp.deviceid,
                sdp.systemsn,
                sdp.logdate_local,
                sdp.dispensebeginaverage,
                sdp.samplekey,
                sdp.testnumber,
                sdp.replicatestart,
                sdp.replicatenumber,
                dpm.modulesn,
                dpm.systemsn,
                dpm.logdate_local,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.systemsn,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > 8500
                     then 1
                     else 0
                     end as check_gt20000
            from
                idaqowner.icq_ccsampledispci sdp
            left join 
                idaqowner.icq_ccdispensepm dpm
            on 
                to_timestamp('11/05/2018 00:00:00', 
                             'MM/DD/YYYY HH24:MI:SS') <= dpm.logdate_local
            and 
                dpm.logdate_local < to_timestamp('11/07/2018 00:00:00', 
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
                to_timestamp('11/05/2018 00:00:00', 
                             'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
            and 
                r.logdate_local < to_timestamp('11/07/2018 00:00:00', 
                                               'MM/DD/YYYY HH24:MI:SS')
            and
                dpm.systemsn = r.systemsn
            and 
                dpm.testid = r.testid
            and 
                r.cuvettenumber is not null
            where
                to_timestamp('11/05/2018 00:00:00', 
                             'MM/DD/YYYY HH24:MI:SS') <= sdp.logdate_local
            and 
                sdp.logdate_local < to_timestamp('11/07/2018 00:00:00', 
                                                 'MM/DD/YYYY HH24:MI:SS')
        ) inner2        
        group by
            inner2.deviceid,
            inner2.modulesn,
            inner2.cuvettenumber
        order by
            inner2.deviceid,
            inner2.modulesn,
            inner2.cuvettenumber
        ) middle2
    where
        middle2.num_sampevents_percuv > 20
    and 
        middle2.cuvettenumber between 1 and 11
    ) final2
where
    final2.gt20000_gt20perc_sampevents = 1
group by
    final2.deviceid,
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents
having
    count(final2.modulesn) <= 8
order by
    final2.deviceid,
    final2.modulesn,
    final2.gt20000_gt20perc_sampevents

