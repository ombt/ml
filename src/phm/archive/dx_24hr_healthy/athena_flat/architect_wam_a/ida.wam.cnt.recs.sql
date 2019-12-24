
--   select 
--       run_date,
--       case when (phm_algorithm_definitions_sk = 1061) 
--            then
--                'WAM A'
--            when (phm_algorithm_definitions_sk = 1062) 
--            then
--                'WAM B'
--            else
--                'UNKNOWN'
--            end as wam_type,
--       sum(case when flag_yn='YES' then 1 else 0 end) as flagged_wam_per_day,
--       sum(case when flag_yn='NO' then 1 else 0 end) as not_flagged_wam_per_day,
--       null as dummy
--   from
--       svc_phm_owner.phm_alg_output
--   where
--       flag_yn = 'YES'
--   and
--       phm_algorithm_definitions_sk in ( 1061,1062 )
--   -- and 
--       -- flag_date >= (sysdate-7)
--       -- run_date >= trunc(sysdate-30)
--   -- and 
--       -- run_date <= trunc(sysdate)
--   group by 
--       run_date,
--       case when (phm_algorithm_definitions_sk = 1061) 
--            then
--                'WAM A'
--            when (phm_algorithm_definitions_sk = 1062) 
--            then
--                'WAM B'
--            else
--                'UNKNOWN'
--            end
--   order by 
--       run_date 

--   select 
--       trunc(flagdate) as flag_date,
--       case when (phm_algorithm_definitions_sk = 1061) 
--            then
--                'WAM A'
--            when (phm_algorithm_definitions_sk = 1062) 
--            then
--                'WAM B'
--            else
--                'UNKNOWN'
--            end as wam_type,
--       sum(case when flaga='YES' then 1 else 0 end) as flagged_wam_per_day,
--       sum(case when flaga='NO' then 1 else 0 end) as not_flagged_wam_per_day,
--       null as dummy
--   from
--       svc_phm_owner.phm_wam_output_1
--   where
--       flaga = 'YES'
--   and
--       phm_algorithm_definitions_sk in ( 1061,1062 )
--   and 
--       trunc(flagdate) >= (sysdate-7)
--   and 
--       flagdate <= trunc(sysdate)
--   group by 
--       trunc(flagdate),
--       case when (phm_algorithm_definitions_sk = 1061) 
--            then
--                'WAM A'
--            when (phm_algorithm_definitions_sk = 1062) 
--            then
--                'WAM B'
--            else
--                'UNKNOWN'
--            end
--   order by 
--       trunc(flagdate)

  select 
      trunc(flagdate) as flag_date,
      case when (phm_algorithm_definitions_sk = 1061) 
           then
               'WAM A'
           when (phm_algorithm_definitions_sk = 1062) 
           then
               'WAM B'
           else
               'UNKNOWN'
           end as wam_type,
      flaga as flagged,
      count(flaga) as flag_cnt,
      count(distinct sn) as sn_cnt,
      null as dummy
  from
      svc_phm_owner.phm_wam_output_1
  where
      -- flaga = 'YES'
  -- and
      phm_algorithm_definitions_sk in ( 1061,1062 )
  and 
      trunc(flagdate) >= (sysdate-30)
  and 
      flagdate <= trunc(sysdate)
  group by 
      trunc(flagdate),
      case when (phm_algorithm_definitions_sk = 1061) 
           then
               'WAM A'
           when (phm_algorithm_definitions_sk = 1062) 
           then
               'WAM B'
           else
               'UNKNOWN'
           end,
      flaga
  order by 
      trunc(flagdate)

