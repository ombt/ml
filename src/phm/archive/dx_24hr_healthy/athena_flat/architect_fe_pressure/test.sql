with rawdata as (
    select
        tbl2.deviceid as deviceid,
        tbl2.modulesndrm as modulesn,
        tbl2.productline as pl,
        tbl2.pipetter as pipetter,
        date_trunc('day', tbl2.completiondate_iso) as trunc_comp_date,
        avg(tbl2.frontendpressure) as avg_fep,
        max(tbl2.completiondate_iso) as max_comp_date
    from ( 
        select 
            tbl1.deviceid,
            tbl1.modulesndrm,
            tbl1.completiondate_iso,
            tbl1.productline,
            coalesce (
                case when substr(tbl1.modulesndrm,1,2)='I1' 
                     then 
                         'I1' 
                     end,
                case when tbl1.location in ('INNER_REAGENT', 
                                             'MEDIAL_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_INNER_REAGENT', 
                                             'R1_MEDIAL_REAGENT',
                                             'R1_OUTER_REAGENT') and
                          tbl1.pipetter = ('PTRGNT1') 
                     then 
                          'R1' 
                     end,
                case when tbl1.location in ('INNER_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_MEDIAL_REAGENT', 
                                             'R1_OUTER_REAGENT') and
                          tbl1.pipetter = ('PTRGNT2')
                     then 
                         'R1' 
                     end,
                case when tbl1.location in ('INNER_REAGENT', 
                                             'MEDIAL_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_INNER_REAGENT', 
                                             'R1_OUTER_REAGENT', 
                                             'R1_MEDIAL_REAGENT') and
                          tbl1.pipetter = ('RGNT1') 
                     then 
                         'R1' 
                     end,
                case when tbl1.location in ( 'RV2') and
                          tbl1.pipetter = ('RGNT1')
                     then 
                         'R1' 
                     end,
                case when tbl1.location in ('RV48') and
                          tbl1.pipetter = ('RGNT1')
                     then 
                         'R2' 
                     end,
                case when tbl1.location in ('R2_INNER_REAGENT', 
                                             'R2_OUTER_REAGENT', 
                                             'R2_MEDIAL_REAGENT') and
                          tbl1.pipetter = ('RGNT1') 
                     then 
                         'R2' 
                     end,
                case when tbl1.location in ('MEDIAL_REAGENT', 
                                             'R2_INNER_REAGENT') and
                          tbl1.pipetter = ('RGNT2')
                     then 
                         'R2'
                     end,
                case when tbl1.location in ('MEDIAL_REAGENT', 
                                             'R2_INNER_REAGENT') and
                          tbl1.pipetter = ('RGNT3')
                     then 
                         'R2'
                     end,
                case when tbl1.location in ('RV24') and
                          tbl1.pipetter in ('PTSAMP1', 
                                             'PTSAMP2')
                     then 
                         'SAMP'
                     end,
                case when tbl1.location in ('RV2') and
                          tbl1.pipetter in ('PTSAMP1', 
                                             'PTSAMP2')
                     then 
                         'R1'
                     end,
                case when tbl1.location in ('ISH_SAMPLE', 
                                             'LAS_SAMPLE') and
                          tbl1.pipetter in ('SAMP')
                     then 
                         'SAMP'
                     end,
                case when tbl1.location in ('STAT_SAMPLE') and
                          tbl1.pipetter in ('SAMP')
                     then 
                         'STATSAMP'
                     else 
                         tbl1.pipetter 
                     end
                ) pipetter,
            tbl1.frontendpressure
        from ( 
            select 
                ip.architect_deviceid as deviceid,
                upper(trim(ip.architect_moduleserial)) as modulesndrm,
                ip.architect_productline as productline,
                ip.frontendpressure,
                ip.pipetter,
                ip.completiondate_iso,
                ip.location
            from 
                dx.dx_architect_pm ip
            where
                ip.architect_productline is not null
            and
                ip.architect_productline in ( '115', '116', '117' )
            and
                '2019-10-10' <= ip.transaction_date
            and 
                ip.transaction_date < '2019-10-16'
        ) tbl1
    ) tbl2
    group by
        tbl2.deviceid,
        tbl2.modulesndrm,
        tbl2.productline,
        tbl2.pipetter,
        date_trunc('day', tbl2.completiondate_iso)
)
select
    final.pl,
    final.modulesn,
    final.pipetter,
    final.fep_gt27000_cnt,
    date_format(final.flag_date,'%Y%m%d%H%i%s') as flag_date
from (
    select
        derived.pl,
        derived.modulesn,
        derived.pipetter,
        sum(derived.fep_gt27000_cnt) as fep_gt27000_cnt,
        sum(derived.fep_cnt) as fep_cnt,
        max(derived.flag_date) as flag_date
    from (
        select 
            rawdata.modulesn,
            rawdata.pl,
            rawdata.pipetter,
            sum(case when rawdata.avg_fep > 27000
                     then 1
                     else 0
                     end) as fep_gt27000_cnt,
            count(rawdata.avg_fep) as fep_cnt,
            max(rawdata.max_comp_date) as flag_date
        from 
            rawdata
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.pipetter,
            rawdata.trunc_comp_date
        ) derived
    group by
        derived.pl,
        derived.modulesn,
        derived.pipetter
    ) final
where
    final.fep_gt27000_cnt >= 5
-- and
    -- final.pipetter = 'I1'
order by
    final.pl,
    final.modulesn,
    final.pipetter
