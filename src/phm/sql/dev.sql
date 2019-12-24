select
    eval2.moduleserialnumber,
    eval2.cuvettenumber,
    eval2.flag_date,
    eval2.num_sampevents_percuv,
    eval2.num_sampevents_gt20000_percuv,
    eval2.num_sampevents_percuv_gt_20,
    eval2.gt20000_gt20perc_sampevents,
    sum (eval2.gt20000_gt20perc_sampevents) as num_gt20000_gt20perc_sampevents
from (
    select
        eval.moduleserialnumber,
        eval.cuvettenumber,
        eval.flag_date,
        eval.num_sampevents_percuv,
        eval.num_sampevents_gt20000_percuv,
        case when (eval.num_sampevents_percuv > 20)
             then 1
             else 0
             end as num_sampevents_percuv_gt_20,
        case when ((cast (eval.num_sampevents_gt20000_percuv as double) / 
                    cast (eval.num_sampevents_percuv as double)) > 0.2)
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            raw.moduleserialnumber,
            raw.cuvettenumber,
            max(raw.datetimestamplocal) as flag_date,
            count(raw.cuvettenumber) as num_sampevents_percuv,
            sum(case when (raw.dispensebeginaverage > 20000)
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
                dx.dx_210_result r
            inner join 
                dx.dx_210_ccdispensepm dpm
            on 
                '2018-11-20' <= dpm.transaction_date
            and 
                dpm.transaction_date < '2018-11-28'
            and
                r.moduleserialnumber = dpm.moduleserialnumber
            and
                r.scmserialnumber = dpm.scmserialnumber
            and
                r.testid = dpm.testid
            inner join
                dx.dx_214_ccsampledispensepcidata sdp
            on
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
                '2018-11-20' <= r.transaction_date
            and 
                r.transaction_date < '2018-11-28'
            and 
                r.cuvettenumber is not null
            and 
                r.cuvettenumber between 1 and 11
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
    eval2.cuvettenumber,
    eval2.flag_date,
    eval2.num_sampevents_percuv,
    eval2.num_sampevents_gt20000_percuv,
    eval2.num_sampevents_percuv_gt_20,
    eval2.gt20000_gt20perc_sampevents
order by 
    eval2.moduleserialnumber,
    eval2.cuvettenumber
