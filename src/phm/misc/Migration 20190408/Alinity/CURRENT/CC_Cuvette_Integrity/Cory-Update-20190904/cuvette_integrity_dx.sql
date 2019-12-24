
select * from (
select
    final2.moduleserialnumber as modulesn,
    final2.cuvettenumber,
    date_format(max(final2.flag_date),'%Y%m%d%H%i%s') as flag_date,
    final2.gt20000_gt20perc_sampevents,    
    count(final2.moduleserialnumber) over(partition by final2.moduleserialnumber) as count_moduleserialnumber
from (
    select
        middle2.*,
        case when (cast (middle2.num_sampevents_gt20000_percuv as double) / 
                   cast (middle2.num_sampevents_percuv as double)) > .2
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
                upper(trim(dpm.moduleserialnumber)) as moduleserialnumber,
                dpm.scmserialnumber,
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
                dx.dx_214_alinity_ci_ccsampledispensepcidata sdp
            left join 
                dx.dx_210_alinity_c_ccdispensepm dpm
            on 
            --    '2019-08-01' <= dpm.transaction_date
            --and 
            --    dpm.transaction_date < '2019-08-08'
            --and
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
           --     '2019-08-01' <= r.transaction_date
           -- and 
           --     r.transaction_date < '2019-08-08'
           -- and
                dpm.scmserialnumber = r.scmserialnumber
            and 
                dpm.testid = r.testid
           -- and 
           --     r.cuvettenumber is not null
            where
                '2019-08-09' <= sdp.transaction_date
            and 
                sdp.transaction_date < '2019-08-16'
            and
                '2019-08-09' <= dpm.transaction_date
            and 
                dpm.transaction_date < '2019-08-16'
            and
                '2019-08-09' <= r.transaction_date
            and 
                r.transaction_date < '2019-08-16'
            and
                r.cuvettenumber is not null
        ) inner2        
        group by
            inner2.moduleserialnumber,
            inner2.cuvettenumber
        ) middle2
    where
        middle2.num_sampevents_percuv > 20
--    and 
--        middle2.cuvettenumber 
--        between 
--            1
--        and 
--            11
    ) final2
where
    final2.gt20000_gt20perc_sampevents = 1
group by
    final2.moduleserialnumber,
    final2.cuvettenumber,
    final2.gt20000_gt20perc_sampevents
    
 order by final2.moduleserialnumber,final2.cuvettenumber
) final3       
    
where 
    final3.count_moduleserialnumber <= 8
    and 
    final3.cuvettenumber 
        between 
            155
        and 
            165
