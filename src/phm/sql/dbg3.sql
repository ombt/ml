select
    count(*)
from
    idaqowner.icq_ccsampledispci xxx
where
    to_timestamp('02/01/2019 00:00:00', 
                 'MM/DD/YYYY HH24:MI:SS') <= xxx.logdate_local
and 
    xxx.logdate_local < to_timestamp('02/08/2019 00:00:00', 
                                     'MM/DD/YYYY HH24:MI:SS')

select
    count(*)
from
    idaqowner.icq_ccdispensepm xxx
where
    to_timestamp('02/01/2019 00:00:00', 
                 'MM/DD/YYYY HH24:MI:SS') <= xxx.logdate_local
and 
    xxx.logdate_local < to_timestamp('02/08/2019 00:00:00', 
                                     'MM/DD/YYYY HH24:MI:SS')

select
    count(*)
from
    idaqowner.icq_results xxx
where
    to_timestamp('02/01/2019 00:00:00', 
                 'MM/DD/YYYY HH24:MI:SS') <= xxx.logdate_local
and 
    xxx.logdate_local < to_timestamp('02/08/2019 00:00:00', 
                                     'MM/DD/YYYY HH24:MI:SS')

select
    count(*)
    -- sdp.systemsn,
    -- sdp.logdate_local,
    -- sdp.dispensebeginaverage,
    -- sdp.samplekey,
    -- sdp.testnumber,
    -- sdp.replicatestart,
    -- sdp.replicatenumber,
    -- dpm.modulesn,
    -- dpm.systemsn,
    -- dpm.logdate_local,
    -- dpm.samplekey,
    -- dpm.toshibatestnumber,
    -- dpm.startingreplicatenumber,
    -- dpm.replicatenumber,
    -- r.systemsn,
    -- r.testid as results_testid,
    -- r.cuvettenumber,
    -- case when sdp.dispensebeginaverage > 20000
         -- then 1
         -- else 0
         -- end as check_gt20000
from
    idaqowner.icq_ccsampledispci sdp
left join 
    idaqowner.icq_ccdispensepm dpm
on 
    to_timestamp('02/01/2019 00:00:00', 
                 'MM/DD/YYYY HH24:MI:SS') <= dpm.logdate_local
and 
    dpm.logdate_local < to_timestamp('02/08/2019 00:00:00', 
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
    to_timestamp('02/01/2019 00:00:00', 
                 'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
and 
    r.logdate_local < to_timestamp('02/08/2019 00:00:00', 
                                     'MM/DD/YYYY HH24:MI:SS')
and
    dpm.systemsn = r.systemsn
and 
    dpm.testid = r.testid
and 
    r.cuvettenumber is not null
where
    to_timestamp('02/01/2019 00:00:00', 
                 'MM/DD/YYYY HH24:MI:SS') <= sdp.logdate_local
and 
    sdp.logdate_local < to_timestamp('02/08/2019 00:00:00', 
                                     'MM/DD/YYYY HH24:MI:SS')
