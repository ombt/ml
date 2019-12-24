  -- 
    -- batch_num
    -- city
    -- country
    -- created_by
    -- customer
    -- customer_number
    -- date_created
    -- device_id
    -- device_value
    -- experience_code
    -- flag_date
    -- flag_yn
    -- ihn_level3_desc
    -- is_manual_exec
    -- phm_algorithm_definitions_sk
    -- phm_patterns_sk
    -- phm_product_line_code
    -- phm_thresholds_sk
    -- processid
    -- product_family
    -- remarks
    -- run_date
    -- sn
    
  select 
      batch_num,
      city,
      country,
      created_by,
      customer,
      customer_number,
      date_created,
      device_id,
      device_value,
      experience_code,
      flag_date,
      flag_yn,
      ihn_level3_desc,
      is_manual_exec,
      phm_algorithm_definitions_sk,
      phm_patterns_sk,
      phm_product_line_code,
      phm_thresholds_sk,
      processid,
      product_family,
      remarks,
      run_date,
      sn,
      null as dummy
  from
      svc_phm_owner.phm_alg_output
  where
      flag_yn = 'YES'
  and
      phm_algorithm_definitions_sk in ( 1061 )
  and 
      -- flag_date >= (sysdate-7)
      run_date >= trunc(sysdate-10)
  and 
      run_date <= trunc(sysdate)
  order by 
      batch_num asc,
      flag_date asc;

