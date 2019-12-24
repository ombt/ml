select 
    final.moduleserialnumber,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner1.moduleserialnumber,
        count(inner1.moduleserialnumber) as num_tests,
        avg(cast (inner1.backlash as double)) as avg_backlash
    from (
        select
            ia.moduleserialnumber,
            regexp_extract(ia.activity,'^.*PosDiff: *(\d+).*$',1) as backlash
        from 
            dx.dx_205_instrumentactivity ia
        where
            date_parse('02/01/2019 00:00:00', '%m/%d/%Y %T') <= ia.datetimestamplocal
        and 
            ia.datetimestamplocal < date_parse('03/01/2019 00:00:00', '%m/%d/%Y %T') 
        and 
            ia.activity like 'SyringeCheckResult for pipettor: SamplePipettor%'
        ) inner1
    group by
        inner1.moduleserialnumber
    ) final
where
    final.avg_backlash > 95
and 
    final.num_tests >= 5
order by
    final.moduleserialnumber
