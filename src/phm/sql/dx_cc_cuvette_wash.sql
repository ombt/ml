select
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents,
    count(final.moduleserialnumber) as count_moduleserialnumber
from (
    select
        middle1.*,
        (middle1.num_sampevents_gt20000_percuv / 
         middle1.num_sampevents_percuv) as perc_sampevents_gt20000_percuv,
        case when (middle1.num_sampevents_gt20000_percuv / 
                   middle1.num_sampevents_percuv) > 0.2
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner1.moduleserialnumber,
            inner1.cuvettenumber,
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
                dpm.datetimestamplocal,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.scmserialnumber,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > 20000
                     then 1
                     else 0
                     end as check_gt20000
            from
                dx.dx_214_ccsampledispensepcidata sdp
            left join 
                dx.dx_214_ccdispensepm dpm
            on 
                date_parse('11/22/2018 00:00:00', '%m/%d/%Y %T') <= dpm.datetimestamplocal
            and 
                dpm.datetimestamplocal < date_parse('11/30/2018 00:00:00', '%m/%d/%Y %T') 
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
                dx.dx_214_result r
            on 
                date_parse('11/22/2018 00:00:00', '%m/%d/%Y %T') <= r.datetimestamplocal
            and 
                r.datetimestamplocal < date_parse('11/30/2018 00:00:00', '%m/%d/%Y %T') 
            and
                dpm.scmserialnumber = r.scmserialnumber
            and 
                dpm.testid = r.testid
            where
                date_parse('11/22/2018 00:00:00', '%m/%d/%Y %T') <= sdp.datetimestamplocal
            and 
                sdp.datetimestamplocal < date_parse('11/30/2018 00:00:00', '%m/%d/%Y %T') 
            and 
                r.cuvettenumber is not null
        ) inner1        
        group by
            inner1.moduleserialnumber,
            inner1.cuvettenumber
        order by
            inner1.moduleserialnumber,
            inner1.cuvettenumber
        ) middle1
    ) final
where
    final.gt20000_gt20perc_sampevents = 1
group by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents
having
    count(final.moduleserialnumber) > 37
order by
    final.moduleserialnumber,
    final.gt20000_gt20perc_sampevents

