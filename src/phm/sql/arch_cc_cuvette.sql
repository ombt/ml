--
-- idaowner.results_cc -
-- 
-- assayid
-- assaystatus
-- calculatedabsorbance
-- calibrationdate
-- calibrationid
-- completiondate
-- constituentreplid1
-- constituentreplid2
-- constituentreplid3
-- constituentreplid4
-- customerinfoid
-- cuvettenumber
-- deviceid
-- dilutionid
-- dilutionprotocol
-- dt_key
-- exceptioncode
-- exceptionstring
-- fileid
-- initiationdate
-- loaddate
-- moduleid
-- moduleinfoid
-- modulesndrm
-- modulesnlog
-- operator
-- ordereddate
-- parentid
-- patientid
-- postsamplerefread
-- presamplerefread
-- primarywavelength
-- productline
-- reagentlot
-- reagentsn
-- replicateid
-- reportedresult
-- reportedresultunits
-- reportedresultvalue
-- result
-- resultcomment
-- resultdetails
-- resultflags
-- resultinterpretation
-- sampledelta
-- sampleid
-- samplelot
-- sampleread
-- sampletype
-- secondarywavelength
-- systeminfoid
-- systemsn
-- 
-- idaowner.pressures_dis -
-- 
-- completiondate
-- deviceid
-- fileid
-- loaddate
-- logfield10
-- logfield11
-- logfield12
-- logfield13
-- logfield14
-- logfield15
-- logfield16
-- logfield17
-- logfield18
-- logfield19
-- logfield20
-- logfield21
-- logfield22
-- logfield23
-- logfield24
-- logfield25
-- logfield26
-- logfield27
-- logfield28
-- logfield29
-- moduleid
-- modulesndrm
-- replicateid
-- resultbyte3
-- resultbyte4
-- resultbyte5
-- resultbyte6
-- resultcode
-- resultcodex

-- select
--     r.modulesndrm,
--     r.cuvettenumber,
--     max(r.completiondate) as flag_date,
--     count(p.logfield25) as disbeginave_count,
--     sum(case when (cast (p.logfield25 as integer) > 20000)
--              then 1
--              else 0
--              end) as disbeginave_gt20000_count
-- from
--     idaowner.results_cc r
-- inner join
--     idaowner.pressures_dis p
-- on
--     p.resultcode = 30
-- and
--     (sysdate - 7) < p.completiondate
-- and 
--     ((p.modulesndrm like 'C4%') or
--      (p.modulesndrm like 'C16%'))
-- and
--     p.replicateid is not null
-- and
--     r.modulesndrm = p.modulesndrm
-- and
--     r.replicateid = p.replicateid
-- where
--     (sysdate - 7) < r.completiondate
-- and 
--     ((r.modulesndrm like 'C4%') or
--      (r.modulesndrm like 'C16%'))
-- and
--     r.replicateid is not null
-- -- and
--     -- (rownum < 500)
-- group by
--     r.modulesndrm,
--     r.cuvettenumber
-- order by
--     r.modulesndrm,
--     r.cuvettenumber
-- 
-- select
--     r.modulesndrm,
--     r.cuvettenumber,
--     max(r.completiondate) as flag_date,
--     count(p.logfield24) as disreadyave_count,
--     sum(case when (cast (p.logfield24 as integer) > 15000)
--              then 1
--              else 0
--              end) as disreadyave_gt15000_count
-- from
--     idaowner.results_cc r
-- inner join
--     idaowner.pressures_dis p
-- on
--     p.resultcode = 30
-- and
--     (sysdate - 7) < p.completiondate
-- and 
--     ((p.modulesndrm like 'C4%') or
--      (p.modulesndrm like 'C16%'))
-- and
--     p.replicateid is not null
-- and
--     r.modulesndrm = p.modulesndrm
-- and
--     r.replicateid = p.replicateid
-- where
--     (sysdate - 7) < r.completiondate
-- and 
--     ((r.modulesndrm like 'C4%') or
--      (r.modulesndrm like 'C16%'))
-- and
--     r.replicateid is not null
-- -- and
--     -- (rownum < 500)
-- group by
--     r.modulesndrm,
--     r.cuvettenumber
-- order by
--     r.modulesndrm,
--     r.cuvettenumber
-- 

select
    r.modulesndrm,
    r.cuvettenumber,
    max(r.completiondate) as flag_date,
    -- count(p.logfield24) as disreadyave_count,
    -- sum(case when (cast (p.logfield24 as integer) > 15000)
             -- then 1
             -- else 0
             -- end) as disreadyave_gt15000_count,
    count(p.logfield25) as disbeginave_count,
    sum(case when (cast (p.logfield25 as integer) > 20000)
             then 1
             else 0
             end) as disbeginave_gt20000_count
from
    idaowner.results_cc r
inner join
    idaowner.pressures_dis p
on
    p.resultcode = 30
and
    (sysdate - 7) < trunc(p.completiondate)
and 
    ((p.modulesndrm like 'C4%') or
     (p.modulesndrm like 'C16%'))
and
    p.replicateid is not null
and
    r.modulesndrm = p.modulesndrm
and
    r.replicateid = p.replicateid
where
    (sysdate - 7) < trunc(r.completiondate)
and 
    ((r.modulesndrm like 'C4%') or
     (r.modulesndrm like 'C16%'))
and
    r.replicateid is not null
-- and
    -- (rownum < 500)
group by
    r.modulesndrm,
    r.cuvettenumber
order by
    r.modulesndrm,
    r.cuvettenumber

