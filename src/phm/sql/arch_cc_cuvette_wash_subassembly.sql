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
    middle.modulesndrm as modulesn,
    middle.cuvettetype,
    max(middle.flag_date) as flag_date,
    case when ((100*sum(middle.exceed_percuv_pct_thld)/count(middle.cuvettenumber)) > 20)
         then 1
         else 0
         end as flagged
from (
    select
        inner.modulesndrm,
        inner.cuvettenumber,
        inner.cuvettetype,
        inner.flag_date,
        inner.sample_count,
        inner.exceed_threshold_count,
        case when ((inner.sample_count > 20) and
                   (100*(inner.exceed_threshold_count/inner.sample_count) > 20))
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
        union all
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
        ) inner
    ) middle
group by
    middle.modulesndrm,
    middle.cuvettetype
order by
    middle.modulesndrm,
    middle.cuvettetype
