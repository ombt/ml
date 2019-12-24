select
    final2.deviceid,
    final2.moduleserialnumber,
    final2.gt20000_gt20perc_sampevents,
    count(final2.moduleserialnumber) as count_moduleserialnumber
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
            inner2.moduleserialnumber,
            inner2.cuvettenumber,
            count(inner2.cuvettenumber) as num_sampevents_percuv,
            sum(inner2.check_gt20000) as num_sampevents_gt20000_percuv
        from (
            select
                sdp.deviceid,
                sdp.scmserialnumber,
                sdp.datetimestamplocal,
                sdp.dispensebeginaverage,
                sdp.samplekey,
                sdp.testnumber,
                sdp.replicatestart,
                sdp.replicatenumber,
                dpm.moduleserialnumber,
                dpm.scmserialnumber,
                dpm.datetimestamplocal,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.scmserialnumber,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > 8500
                     then 1
                     else 0
                     end as check_gt20000
            from
                dx.dx_210_ccsampledispensepcidata sdp
            left join 
                dx.dx_210_ccdispensepm dpm
            on 
                date_parse('11/05/2018 00:00:00', '%m/%d/%Y %T') <= dpm.datetimestamplocal
            and 
                dpm.datetimestamplocal < date_parse('11/07/2018 00:00:00', '%m/%d/%Y %T') 
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
                date_parse('11/05/2018 00:00:00', '%m/%d/%Y %T') <= r.datetimestamplocal
            and 
                r.datetimestamplocal < date_parse('11/07/2018 00:00:00', '%m/%d/%Y %T') 
            and
                dpm.scmserialnumber = r.scmserialnumber
            and 
                dpm.testid = r.testid
            and 
                r.cuvettenumber is not null
            where
                date_parse('11/05/2018 00:00:00', '%m/%d/%Y %T') <= sdp.datetimestamplocal
            and 
                sdp.datetimestamplocal < date_parse('11/07/2018 00:00:00', '%m/%d/%Y %T') 
        ) inner2        
        group by
            inner2.deviceid,
            inner2.moduleserialnumber,
            inner2.cuvettenumber
        order by
            inner2.deviceid,
            inner2.moduleserialnumber,
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
    final2.moduleserialnumber,
    final2.gt20000_gt20perc_sampevents
having
    count(final2.moduleserialnumber) <= 8
order by
    final2.deviceid,
    final2.moduleserialnumber,
    final2.gt20000_gt20perc_sampevents

