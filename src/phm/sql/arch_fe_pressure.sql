-- with rawdata as (
--     select
--         r.modulesndrm,
--         r.cuvettenumber,
--         r.completiondate,
--         p.logfield24 as disreadyave,
--         p.logfield25 as disbeginave
--     from
--         idaowner.results_cc r
--     inner join
--         idaowner.pressures_dis p
--     on
--         p.resultcode = '30'
--     and
--         (sysdate - 7) < p.completiondate
--     and 
--         ((p.modulesndrm like 'C4%') or
--          (p.modulesndrm like 'C16%'))
--     and
--         p.replicateid is not null
--     and
--         r.modulesndrm = p.modulesndrm
--     and
--         r.replicateid = p.replicateid
--     where
--         (sysdate - 7) < r.completiondate
--     and 
--         ((r.modulesndrm like 'C4%') or
--          (r.modulesndrm like 'C16%'))
--     and
--         r.replicateid is not null
-- )
-- select
--     middle1.modulesndrm as modulesn,
--     middle1.cuvettetype,
--     'cc_cuvette_lls' as algorithm,
--     max(middle1.flag_date) as flag_date,
--     case when ((100*sum(middle1.exceed_percuv_pct_thld)/count(middle1.cuvettenumber)) > 10)
--          then 1
--          else 0
--          end as flagged
-- from (
--     select
--         inner.modulesndrm,
--         inner.cuvettenumber,
--         inner.cuvettetype,
--         inner.flag_date,
--         inner.sample_count,
--         inner.exceed_threshold_count,
--         case when ((inner.sample_count > 20) and
--                    (100*(inner.exceed_threshold_count/inner.sample_count) > 10))
--              then
--                  1
--              else
--                  0
--              end as exceed_percuv_pct_thld
--     from (
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c4' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disreadyave) as sample_count,
--             sum(case when (cast (rawdata.disreadyave as integer) > 15000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C4%'
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         union all
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c16-aline' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disreadyave) as sample_count,
--             sum(case when (cast (rawdata.disreadyave as integer) > 15000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C16%'
--         and
--             mod(rawdata.cuvettenumber,2) = 0
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         union all
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c16-bline' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disreadyave) as sample_count,
--             sum(case when (cast (rawdata.disreadyave as integer) > 15000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C16%'
--         and
--             mod(rawdata.cuvettenumber,2) = 1
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         ) inner
--     ) middle1
-- group by
--     middle1.modulesndrm,
--     middle1.cuvettetype
-- union all
-- select
--     c4middle.modulesndrm as modulesn,
--     c4middle.cuvettetype,
--     'cc_cuvette_status_check' as algorithm,
--     max(c4middle.flag_date) as flag_date,
--     case when (count(c4middle.exceed_percuv_pct_thld) <= 4)
--          then 1
--          else 0
--          end as flagged
-- from (
--     select
--         c4inner.modulesndrm,
--         c4inner.cuvettenumber,
--         c4inner.cuvettetype,
--         c4inner.flag_date,
--         c4inner.sample_count,
--         c4inner.exceed_threshold_count,
--         case when ((c4inner.sample_count > 20) and
--                    (100*(c4inner.exceed_threshold_count/c4inner.sample_count) > 20))
--              then
--                  1
--              else
--                  0
--              end as exceed_percuv_pct_thld
--     from (
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c4' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disbeginave) as sample_count,
--             sum(case when (cast (rawdata.disbeginave as integer) > 20000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C4%'
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         ) c4inner
--     ) c4middle
-- group by
--     c4middle.modulesndrm,
--     c4middle.cuvettetype
-- union all
-- select
--     c16middle.modulesndrm,
--     c16middle.cuvettetype,
--     'cc_cuvette_status_check' as algorithm,
--     max(c16middle.flag_date),
--     case when (count(c16middle.exceed_percuv_pct_thld) <= 7)
--          then 1
--          else 0
--          end as flagged
-- from (
--     select
--         c16inner.modulesndrm,
--         c16inner.cuvettenumber,
--         c16inner.cuvettetype,
--         c16inner.flag_date,
--         c16inner.sample_count,
--         c16inner.exceed_threshold_count,
--         case when ((c16inner.sample_count > 20) and
--                    (100*(c16inner.exceed_threshold_count/c16inner.sample_count) > 20))
--              then
--                  1
--              else
--                  0
--              end as exceed_percuv_pct_thld
--     from (
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c16-aline' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disbeginave) as sample_count,
--             sum(case when (cast (rawdata.disbeginave as integer) > 20000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C16%'
--         and
--             mod(rawdata.cuvettenumber,2) = 0
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         union all
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c16-bline' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disbeginave) as sample_count,
--             sum(case when (cast (rawdata.disbeginave as integer) > 20000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C16%'
--         and
--             mod(rawdata.cuvettenumber,2) = 1
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         ) c16inner
--     ) c16middle
-- group by
--     c16middle.modulesndrm,
--     c16middle.cuvettetype
-- union all
-- select
--     middle2.modulesndrm as modulesn,
--     middle2.cuvettetype,
--     'cc_cuvette_wash_subassembly' as algorithm,
--     max(middle2.flag_date) as flag_date,
--     case when ((100*sum(middle2.exceed_percuv_pct_thld)/count(middle2.cuvettenumber)) > 20)
--          then 1
--          else 0
--          end as flagged
-- from (
--     select
--         inner.modulesndrm,
--         inner.cuvettenumber,
--         inner.cuvettetype,
--         inner.flag_date,
--         inner.sample_count,
--         inner.exceed_threshold_count,
--         case when ((inner.sample_count > 20) and
--                    (100*(inner.exceed_threshold_count/inner.sample_count) > 20))
--              then
--                  1
--              else
--                  0
--              end as exceed_percuv_pct_thld
--     from (
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c4' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disbeginave) as sample_count,
--             sum(case when (cast (rawdata.disbeginave as integer) > 20000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C4%'
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         union all
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c16-aline' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disbeginave) as sample_count,
--             sum(case when (cast (rawdata.disbeginave as integer) > 20000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C16%'
--         and
--             mod(rawdata.cuvettenumber,2) = 0
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         union all
--         select
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber,
--             'c16-bline' as cuvettetype,
--             max(rawdata.completiondate) as flag_date,
--             count(rawdata.disbeginave) as sample_count,
--             sum(case when (cast (rawdata.disbeginave as integer) > 20000)
--                      then 1
--                      else 0
--                      end) as exceed_threshold_count
--         from
--             rawdata
--         where
--             rawdata.modulesndrm like 'C16%'
--         and
--             mod(rawdata.cuvettenumber,2) = 1
--         group by
--             rawdata.modulesndrm,
--             rawdata.cuvettenumber
--         ) inner
--     ) middle2
-- group by
--     middle2.modulesndrm,
--     middle2.cuvettetype
-- 
--         SELECT 
--             DEVICEID,
--             MODULESNDRM,
--             TRUNC(COMPLETIONDATE) DT,
--             MAX(COMPLETIONDATE) FLAGDATE,
--             COALESCE(
--                 CASE WHEN SUBSTR(P.MODULESNDRM,1,2)='I1' 
--                      THEN 
--                          'I1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT',
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT1') 
--                      THEN 
--                           'R1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT', 
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT2')
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ( 'RV2') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ('RV48') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R2' 
--                      END,
--                 CASE WHEN P.location IN ('R2_INNER_REAGENT', 
--                                          'R2_OUTER_REAGENT', 
--                                          'R2_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R2' 
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT2')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT3')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('RV24') AND
--                           P.PIPETTER IN ('PTSAMP1', 
--                                          'PTSAMP2')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('RV2') AND
--                           P.PIPETTER IN ('PTSAMP1', 
--                                          'PTSAMP2')
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('ISH_SAMPLE', 
--                                          'LAS_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('STAT_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'STATSAMP'
--                      ELSE 
--                          P.PIPETTER 
--                      END
--                 ) PIPETTER,
--             MEDIAN(FRONTENDPRESSURE) MED_PRSR,
--             MAX(COMPLETIONDATE) MAX_COMP_DATE
--         FROM 
--             SVC_PHM_ODS.PHM_ODS_PRESSURES_IA P
--         WHERE 
--             LOADDATE BETWEEN 
--                 V_START_DATE - DAYS_NUM 
--             AND 
--                 V_END_DATE 
--         SELECT 
--             DEVICEID,
--             MODULESNDRM,
--             TRUNC(COMPLETIONDATE) DT,
--             MAX(COMPLETIONDATE) FLAGDATE,
--             COALESCE(
--                 CASE WHEN SUBSTR(P.MODULESNDRM,1,2)='I1' 
--                      THEN 
--                          'I1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT',
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT1') 
--                      THEN 
--                           'R1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT', 
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT2')
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ( 'RV2') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ('RV48') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R2' 
--                      END,
--                 CASE WHEN P.location IN ('R2_INNER_REAGENT', 
--                                          'R2_OUTER_REAGENT', 
--                                          'R2_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R2' 
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT2')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT3')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('RV24') AND
--                           P.PIPETTER IN ('PTSAMP1', 
--                                          'PTSAMP2')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('RV2') AND
--                           P.PIPETTER IN ('PTSAMP1', 
--                                          'PTSAMP2')
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('ISH_SAMPLE', 
--                                          'LAS_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('STAT_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'STATSAMP'
--                      ELSE 
--                          P.PIPETTER 
--                      END
--                 ) PIPETTER,
--             MEDIAN(FRONTENDPRESSURE) MED_PRSR,
--             MAX(COMPLETIONDATE) MAX_COMP_DATE
--         FROM 
--             SVC_PHM_ODS.PHM_ODS_PRESSURES_IA P
--         WHERE 
--             LOADDATE BETWEEN 
--                 V_START_DATE - DAYS_NUM 
--             AND 
--                 V_END_DATE 
-- ) 
-- 
--     SELECT 
--         * 
--     FROM (
--         SELECT 
--             DEVICEID,
--             MODULESNDRM,
--             TRUNC(COMPLETIONDATE) DT,
--             MAX(COMPLETIONDATE) FLAGDATE,
--             COALESCE(
--                 CASE WHEN SUBSTR(P.MODULESNDRM,1,2)='I1' 
--                      THEN 
--                          'I1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT',
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT1') 
--                      THEN 
--                           'R1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT', 
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT2')
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ( 'RV2') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R1' 
--                      END,
--                 CASE WHEN P.location IN ('RV48') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R2' 
--                      END,
--                 CASE WHEN P.location IN ('R2_INNER_REAGENT', 
--                                          'R2_OUTER_REAGENT', 
--                                          'R2_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R2' 
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT2')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT3')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('RV24') AND
--                           P.PIPETTER IN ('PTSAMP1', 
--                                          'PTSAMP2')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('RV2') AND
--                           P.PIPETTER IN ('PTSAMP1', 
--                                          'PTSAMP2')
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('ISH_SAMPLE', 
--                                          'LAS_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('STAT_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'STATSAMP'
--                      ELSE 
--                          P.PIPETTER 
--                      END
--                 ) PIPETTER,
--             MEDIAN(FRONTENDPRESSURE) MED_PRSR,
--             MAX(COMPLETIONDATE) MAX_COMP_DATE
--         FROM 
--             SVC_PHM_ODS.PHM_ODS_PRESSURES_IA P
--         WHERE 
--             LOADDATE BETWEEN 
--                 V_START_DATE - DAYS_NUM 
--             AND 
--                 V_END_DATE 
--         GROUP BY 
--             DEVICEID,
--             MODULESNDRM,
--             TRUNC(COMPLETIONDATE),
--             COALESCE( 
--                 CASE WHEN SUBSTR(P.MODULESNDRM,1,2)='I1' 
--                      THEN 
--                          'I1' 
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT',
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT1') 
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT', 
--                                          'R1_OUTER_REAGENT') AND
--                           P.PIPETTER = ('PTRGNT2')
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('INNER_REAGENT', 
--                                          'MEDIAL_REAGENT', 
--                                          'OUTER_REAGENT', 
--                                          'R1_INNER_REAGENT', 
--                                          'R1_OUTER_REAGENT', 
--                                          'R1_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('RV2') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('RV48') AND
--                           P.PIPETTER = ('RGNT1')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('R2_INNER_REAGENT', 
--                                          'R2_OUTER_REAGENT', 
--                                          'R2_MEDIAL_REAGENT') AND
--                           P.PIPETTER = ('RGNT1') 
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT2')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('MEDIAL_REAGENT', 
--                                          'R2_INNER_REAGENT') AND
--                           P.PIPETTER = ('RGNT3')
--                      THEN 
--                          'R2'
--                      END,
--                 CASE WHEN P.location IN ('RV24') AND
--                           P.PIPETTER IN ('PTSAMP1', 'PTSAMP2')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('RV2') AND
--                           P.PIPETTER IN ('PTSAMP1', 
--                                          'PTSAMP2')
--                      THEN 
--                          'R1'
--                      END,
--                 CASE WHEN P.location IN ('ISH_SAMPLE', 
--                                          'LAS_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'SAMP'
--                      END,
--                 CASE WHEN P.location IN ('STAT_SAMPLE') AND
--                           P.PIPETTER IN ('SAMP')
--                      THEN 
--                          'STATSAMP'
--                      ELSE 
--                          P.PIPETTER 
--                      END
--             ) 
--     ) 
--     WHERE 
--         PIPETTER = V_PIPETTER 
--     ORDER BY 
--         1,2,3,4;

--             DEVICEID,
--             MODULESNDRM,
--             TRUNC(COMPLETIONDATE) DT,
--             MAX(COMPLETIONDATE) FLAGDATE,
--             MEDIAN(FRONTENDPRESSURE) MED_PRSR,
--             MAX(COMPLETIONDATE) MAX_COMP_DATE
--         FROM 
--             SVC_PHM_ODS.PHM_ODS_PRESSURES_IA P
--         WHERE 
--             LOADDATE BETWEEN 
--                 V_START_DATE - DAYS_NUM 
--             AND 
--                 V_END_DATE 
--         GROUP BY 
--             DEVICEID,
--             MODULESNDRM,
--             TRUNC(COMPLETIONDATE),

with rawdata as (
    select
        middle.deviceid as deviceid,
        middle.modulesndrm as modulesn,
        middle.pipetter as pipetter,
        trunc(middle.completiondate) as trunc_comp_date,
        median(middle.frontendpressure) as median_fep,
        max(middle.completiondate) as max_comp_date
    from ( 
        select 
            inner.deviceid,
            inner.modulesndrm,
            inner.completiondate,
            coalesce (
                case when substr(inner.modulesndrm,1,2)='I1' 
                     then 
                         'I1' 
                     end,
                case when inner.location in ('INNER_REAGENT', 
                                             'MEDIAL_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_INNER_REAGENT', 
                                             'R1_MEDIAL_REAGENT',
                                             'R1_OUTER_REAGENT') and
                          inner.pipetter = ('PTRGNT1') 
                     then 
                          'R1' 
                     end,
                case when inner.location in ('INNER_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_MEDIAL_REAGENT', 
                                             'R1_OUTER_REAGENT') and
                          inner.pipetter = ('PTRGNT2')
                     then 
                         'R1' 
                     end,
                case when inner.location in ('INNER_REAGENT', 
                                             'MEDIAL_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_INNER_REAGENT', 
                                             'R1_OUTER_REAGENT', 
                                             'R1_MEDIAL_REAGENT') and
                          inner.pipetter = ('RGNT1') 
                     then 
                         'R1' 
                     end,
                case when inner.location in ( 'RV2') and
                          inner.pipetter = ('RGNT1')
                     then 
                         'R1' 
                     end,
                case when inner.location in ('RV48') and
                          inner.pipetter = ('RGNT1')
                     then 
                         'R2' 
                     end,
                case when inner.location in ('R2_INNER_REAGENT', 
                                             'R2_OUTER_REAGENT', 
                                             'R2_MEDIAL_REAGENT') and
                          inner.pipetter = ('RGNT1') 
                     then 
                         'R2' 
                     end,
                case when inner.location in ('MEDIAL_REAGENT', 
                                             'R2_INNER_REAGENT') and
                          inner.pipetter = ('RGNT2')
                     then 
                         'R2'
                     end,
                case when inner.location in ('MEDIAL_REAGENT', 
                                             'R2_INNER_REAGENT') and
                          inner.pipetter = ('RGNT3')
                     then 
                         'R2'
                     end,
                case when inner.location in ('RV24') and
                          inner.pipetter in ('PTSAMP1', 
                                             'PTSAMP2')
                     then 
                         'SAMP'
                     end,
                case when inner.location in ('RV2') and
                          inner.pipetter in ('PTSAMP1', 
                                             'PTSAMP2')
                     then 
                         'R1'
                     end,
                case when inner.location in ('ISH_SAMPLE', 
                                             'LAS_SAMPLE') and
                          inner.pipetter in ('SAMP')
                     then 
                         'SAMP'
                     end,
                case when inner.location in ('STAT_SAMPLE') and
                          inner.pipetter in ('SAMP')
                     then 
                         'STATSAMP'
                     else 
                         inner.pipetter 
                     end
                ) pipetter,
            inner.frontendpressure
        from ( 
            select 
                ip.deviceid,
                ip.modulesndrm,
                ip.frontendpressure,
                ip.pipetter,
                ip.completiondate,
                ip.location
            from 
                idaowner.pressures_ia ip
            where
                (sysdate-5) <= ip.completiondate
            and
                ip.completiondate < sysdate
            -- inner join
                -- idaowner.idalogfiles ilf
            -- on
                -- (sysdate-1) <= ilf.loadendtime 
            -- and
                -- ilf.loadendtime < sysdate
            -- and
                -- ip.fileid = ilf.fileid
        ) inner
    ) middle
    group by
        middle.deviceid,
        middle.modulesndrm,
        middle.pipetter,
        trunc(middle.completiondate)
)
select
    final.modulesn,
    final.pipetter,
    final.flag_date
from (
    select
        derived.modulesn,
        derived.pipetter,
        sum(derived.fep_gt27000_cnt) as fep_gt27000_cnt,
        sum(derived.fep_cnt) as fep_cnt,
        max(derived.flag_date) as flag_date
    from (
        select 
            rawdata.modulesn,
            rawdata.pipetter,
            sum(case when rawdata.median_fep > 27000
                     then 1
                     else 0
                     end) as fep_gt27000_cnt,
            count(rawdata.median_fep) as fep_cnt,
            max(rawdata.max_comp_date) as flag_date
        from 
            rawdata
        group by
            rawdata.modulesn,
            rawdata.pipetter,
            rawdata.trunc_comp_date
        ) derived
    group by
        derived.modulesn,
        derived.pipetter
    ) final
where
    final.fep_gt27000_cnt >= 5
order by
    final.modulesn,
    final.pipetter

