
  select distinct
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
      sn,
      null as dummy
  from
      svc_phm_owner.phm_wam_output_1
  where
      flaga = 'YES'
  and
      phm_algorithm_definitions_sk in ( 1061,1062 )
  and 
      trunc(flagdate) >= (sysdate-30)
  and 
      flagdate <= trunc(sysdate)
  order by 
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
      sn

