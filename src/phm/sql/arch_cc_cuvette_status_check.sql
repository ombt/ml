with rawdata as (
    select
        r.modulesndrm,
        r.cuvettenumber,
        r.completiondate,
        p.logfield24 as disreadyave,
        p.logfield25 as disbeginave
    from
        idaowner.results_cc r
    inner join
        idaowner.pressures_dis p
    on
        p.resultcode = '30'
    and
        (sysdate - 7) < p.completiondate
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
        (sysdate - 7) < r.completiondate
    and 
        ((r.modulesndrm like 'C4%') or
         (r.modulesndrm like 'C16%'))
    and
        r.replicateid is not null
)
select
    c4middle.modulesndrm as modulesn,
    c4middle.cuvettetype,
    max(c4middle.flag_date) as flag_date,
    case when (count(c4middle.exceed_percuv_pct_thld) <= 4)
         then 1
         else 0
         end as flagged
from (
    select
        c4inner.modulesndrm,
        c4inner.cuvettenumber,
        c4inner.cuvettetype,
        c4inner.flag_date,
        c4inner.sample_count,
        c4inner.exceed_threshold_count,
        case when ((c4inner.sample_count > 20) and
                   (100*(c4inner.exceed_threshold_count/c4inner.sample_count) > 20))
             then
                 1
             else
                 0
             end as exceed_percuv_pct_thld
    from (
        select
            rawdata.modulesndrm,
            rawdata.cuvettenumber,
            'c4' as cuvettetype,
            max(rawdata.completiondate) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > 20000)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesndrm like 'C4%'
        group by
            rawdata.modulesndrm,
            rawdata.cuvettenumber
        ) c4inner
    ) c4middle
group by
    c4middle.modulesndrm,
    c4middle.cuvettetype
union all
select
    c16middle.modulesndrm,
    c16middle.cuvettetype,
    max(c16middle.flag_date),
    case when (count(c16middle.exceed_percuv_pct_thld) <= 7)
         then 1
         else 0
         end as flagged
from (
    select
        c16inner.modulesndrm,
        c16inner.cuvettenumber,
        c16inner.cuvettetype,
        c16inner.flag_date,
        c16inner.sample_count,
        c16inner.exceed_threshold_count,
        case when ((c16inner.sample_count > 20) and
                   (100*(c16inner.exceed_threshold_count/c16inner.sample_count) > 20))
             then
                 1
             else
                 0
             end as exceed_percuv_pct_thld
    from (
        select
            rawdata.modulesndrm,
            rawdata.cuvettenumber,
            'c16-aline' as cuvettetype,
            max(rawdata.completiondate) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > 20000)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesndrm like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 0
        group by
            rawdata.modulesndrm,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesndrm,
            rawdata.cuvettenumber,
            'c16-bline' as cuvettetype,
            max(rawdata.completiondate) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > 20000)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesndrm like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 1
        group by
            rawdata.modulesndrm,
            rawdata.cuvettenumber
        ) c16inner
    ) c16middle
group by
    c16middle.modulesndrm,
    c16middle.cuvettetype

