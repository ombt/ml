select 
    final.modulesn,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner.modulesn,
        count(inner.modulesn) as num_tests,
        avg(inner.backlash) as avg_backlash
    from (
        select
            ia.modulesn,
            regexp_substr(ia.activity,'\PosDiff:\s(.*?)\Z',1,1,null,1) as backlash
            from 
                idaqowner.icq_instrumentactivity ia
            where
                to_timestamp('02/01/2019 00:00:00', 
                             'MM/DD/YYYY HH24:MI:SS') <= ia.logdate_local
            and 
                ia.logdate_local < to_timestamp('03/01/2019 00:00:00', 
                                                'MM/DD/YYYY HH24:MI:SS')
            and 
                ia.activity like 'SyringeCheckResult for pipettor: SamplePipettor%'
        ) inner
    group by
        inner.modulesn
    ) final
where
    final.avg_backlash > 95
and 
    final.num_tests >= 5
order by
    final.modulesn
