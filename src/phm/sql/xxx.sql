-- 
-- with rawdata as (
--     select
--         tbl2.deviceid as deviceid,
--         tbl2.modulesndrm as modulesn,
--         tbl2.productline as pl,
--         tbl2.pipetter as pipetter,
--         date_trunc('day', tbl2.completiondate_iso) as trunc_comp_date,
--         avg(tbl2.frontendpressure) as avg_fep,
--         max(tbl2.completiondate_iso) as max_comp_date
--     from ( 
--         select 
--             tbl1.deviceid,
--             tbl1.modulesndrm,
--             tbl1.completiondate_iso,
--             tbl1.productline,
--             coalesce (
--                 case when substr(tbl1.modulesndrm,1,2)='I1' 
--                      then 
--                          'I1' 
--                      end,
--                 case when tbl1.location in ('INNER_REAGENT', 
--                                              'MEDIAL_REAGENT', 
--                                              'OUTER_REAGENT', 
--                                              'R1_INNER_REAGENT', 
--                                              'R1_MEDIAL_REAGENT',
--                                              'R1_OUTER_REAGENT') and
--                           tbl1.pipetter = ('PTRGNT1') 
--                      then 
--                           'R1' 
--                      end,
--                 case when tbl1.location in ('INNER_REAGENT', 
--                                              'OUTER_REAGENT', 
--                                              'R1_MEDIAL_REAGENT', 
--                                              'R1_OUTER_REAGENT') and
--                           tbl1.pipetter = ('PTRGNT2')
--                      then 
--                          'R1' 
--                      end,
--                 case when tbl1.location in ('INNER_REAGENT', 
--                                              'MEDIAL_REAGENT', 
--                                              'OUTER_REAGENT', 
--                                              'R1_INNER_REAGENT', 
--                                              'R1_OUTER_REAGENT', 
--                                              'R1_MEDIAL_REAGENT') and
--                           tbl1.pipetter = ('RGNT1') 
--                      then 
--                          'R1' 
--                      end,
--                 case when tbl1.location in ( 'RV2') and
--                           tbl1.pipetter = ('RGNT1')
--                      then 
--                          'R1' 
--                      end,
--                 case when tbl1.location in ('RV48') and
--                           tbl1.pipetter = ('RGNT1')
--                      then 
--                          'R2' 
--                      end,
--                 case when tbl1.location in ('R2_INNER_REAGENT', 
--                                              'R2_OUTER_REAGENT', 
--                                              'R2_MEDIAL_REAGENT') and
--                           tbl1.pipetter = ('RGNT1') 
--                      then 
--                          'R2' 
--                      end,
--                 case when tbl1.location in ('MEDIAL_REAGENT', 
--                                              'R2_INNER_REAGENT') and
--                           tbl1.pipetter = ('RGNT2')
--                      then 
--                          'R2'
--                      end,
--                 case when tbl1.location in ('MEDIAL_REAGENT', 
--                                              'R2_INNER_REAGENT') and
--                           tbl1.pipetter = ('RGNT3')
--                      then 
--                          'R2'
--                      end,
--                 case when tbl1.location in ('RV24') and
--                           tbl1.pipetter in ('PTSAMP1', 
--                                              'PTSAMP2')
--                      then 
--                          'SAMP'
--                      end,
--                 case when tbl1.location in ('RV2') and
--                           tbl1.pipetter in ('PTSAMP1', 
--                                              'PTSAMP2')
--                      then 
--                          'R1'
--                      end,
--                 case when tbl1.location in ('ISH_SAMPLE', 
--                                              'LAS_SAMPLE') and
--                           tbl1.pipetter in ('SAMP')
--                      then 
--                          'SAMP'
--                      end,
--                 case when tbl1.location in ('STAT_SAMPLE') and
--                           tbl1.pipetter in ('SAMP')
--                      then 
--                          'STATSAMP'
--                      else 
--                          tbl1.pipetter 
--                      end
--                 ) pipetter,
--             tbl1.frontendpressure
--         from ( 
--             select 
--                 ip.architect_deviceid as deviceid,
--                 ip.architect_moduleserial as modulesndrm,
--                 ip.architect_productline as productline,
--                 ip.frontendpressure,
--                 ip.pipetter,
--                 ip.completiondate_iso,
--                 ip.location
--             from 
--                 dx.dx_architect_pm ip
--             where
--                 '2019-06-01' <= ip.transaction_date
--             and
--                 ip.transaction_date < '2019-06-08'
--         ) tbl1
--     ) tbl2
--     group by
--         tbl2.deviceid,
--         tbl2.modulesndrm,
--         tbl2.productline,
--         tbl2.pipetter,
--         date_trunc('day', tbl2.completiondate_iso)
-- )
-- select
--     final.pl,
--     final.modulesn,
--     final.pipetter,
--     final.flag_date
-- from (
--     select
--         derived.pl,
--         derived.modulesn,
--         derived.pipetter,
--         sum(derived.fep_gt27000_cnt) as fep_gt27000_cnt,
--         sum(derived.fep_cnt) as fep_cnt,
--         max(derived.flag_date) as flag_date
--     from (
--         select 
--             rawdata.modulesn,
--             rawdata.pl,
--             rawdata.pipetter,
--             sum(case when rawdata.avg_fep > 27000
--                      then 1
--                      else 0
--                      end) as fep_gt27000_cnt,
--             count(rawdata.avg_fep) as fep_cnt,
--             max(rawdata.max_comp_date) as flag_date
--         from 
--             rawdata
--         group by
--             rawdata.modulesn,
--             rawdata.pl,
--             rawdata.pipetter,
--             rawdata.trunc_comp_date
--         ) derived
--     group by
--         derived.pl,
--         derived.modulesn,
--         derived.pipetter
--     ) final
-- where
--     final.fep_gt27000_cnt >= 5
-- order by
--     final.pl,
--     final.modulesn,
--     final.pipetter


with rawdata as (
select 
    d3.msn as modulesn,
    max(d3.flag_date) as flag_date,
    sum(d3.sum_exceed_range_threshold) as total_range_cnt,
    sum(d3.sum_exceed_stddev_threshold) as total_stddev_cnt
from (
    select 
        d2.msn,
        d2.flag_date,
        sum(d2.range_flag) over (
            order by 
                d2.msn,
                d2.flag_date
            asc rows 3 preceding
        ) as sum_exceed_range_threshold,
        sum(d2.stddev_flag) over (
            order by 
                d2.msn,
                d2.flag_date
            asc rows 3 preceding
        ) as sum_exceed_stddev_threshold
    from (
        select 
            d1.msn,
            d1.flag_date,
            d1.p1,
            case when ((d1.rnum > 9) and ( d1.p1 > 0) and
                       ((d1.p1 > (d1.avg_p1 + 3.0*(d1.max_p1 - d1.min_p1)/4.0)) or
                        (d1.p1 < (d1.avg_p1 - 3.0*(d1.max_p1 - d1.min_p1)/4.0))))
                 then 1
                 else 0 end as range_flag,
            case when ((d1.rnum > 9) and (d1.p1 > 0) and
                       ((d1.p1 > (d1.avg_p1 + 3.0*d1.stddev_p1)) or
                        (d1.p1 < (d1.avg_p1 - 3.0*d1.stddev_p1))))
                 then 1
                 else 0 end as stddev_flag
        from ( 
            select
                dpm.moduleserialnumber as msn,
                dpm.pressured1 as p1,
                row_number() over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal) asc
                ) as rnum,
                max(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as max_p1,
                min(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as min_p1,
                avg(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as avg_p1,
                stddev(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as stddev_p1,
                date_trunc('day', dpm.datetimestamplocal) as flag_date
            from
                dx.dx_210_alinity_c_ccdispensepm dpm
            where
                '2019-01-01' <= dpm.transaction_date
            and
                dpm.transaction_date < '2019-01-15'
            and 
                dpm.pressured1 is not null
            order by
                dpm.moduleserialnumber,
                date_trunc('day', dpm.datetimestamplocal)
        ) d1
    ) d2
) d3
group by
    d3.msn
)
select 
    rawdata.modulesn,
    rawdata.flag_date,
    rawdata.total_range_cnt,
    rawdata.total_stddev_cnt
from 
    rawdata
where
    rawdata.total_range_cnt > 1
and
    rawdata.total_stddev_cnt > 1
order by
    rawdata.modulesn
