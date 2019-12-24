-- -- 
-- -- batch_num
-- -- city
-- -- country
-- -- created_by
-- -- customer
-- -- customer_number
-- -- date_created
-- -- device_id
-- -- device_value
-- -- experience_code
-- -- flag_date
-- -- flag_yn
-- -- ihn_level3_desc
-- -- is_manual_exec
-- -- phm_algorithm_definitions_sk
-- -- phm_patterns_sk
-- -- phm_product_line_code
-- -- phm_thresholds_sk
-- -- processid
-- -- product_family
-- -- remarks
-- -- run_date
-- -- sn
-- 
-- select 
--     batch_num,
--     city,
--     country,
--     created_by,
--     customer,
--     customer_number,
--     date_created,
--     device_id,
--     device_value,
--     experience_code,
--     flag_date,
--     flag_yn,
--     ihn_level3_desc,
--     is_manual_exec,
--     phm_algorithm_definitions_sk,
--     phm_patterns_sk,
--     phm_product_line_code,
--     phm_thresholds_sk,
--     processid,
--     product_family,
--     remarks,
--     run_date,
--     sn,
--     null as dummy
-- from
--     svc_phm_owner.phm_alg_output
-- where
--     flag_yn = 'YES'
-- and
--     created_by like '%5756%'
-- order by 
--     flag_date desc
-- 
--     -- phm_algorithm_definitions_sk in ( 1003 )
--     -- phm_algorithm_definitions_sk in ( 1064 )
--     -- phm_algorithm_definitions_sk in ( 20190, 20191 )
--     -- phm_algorithm_definitions_sk in ( 20142, 20143, 20144, 20145, 20146, 20147, 20148 )
-- order by 
--     -- run_date desc
-- 
-- -- SELECT 
--    -- ROWID, CUSTOMER, CUSTOMER_NUMBER, DEVICE_ID, 
--    -- SN, COUNTRY, CITY, 
--    -- PHM_ALGORITHM_DEFINITIONS_SK, PHM_THRESHOLDS_SK, FLAG_DATE, 
--    -- DEVICE_VALUE, FLAG_YN, IHN_LEVEL3_DESC, 
--    -- REMARKS, CREATED_BY, DATE_CREATED, 
--    -- PRODUCT_FAMILY, BATCH_NUM, PHM_PATTERNS_SK, 
--    -- RUN_DATE, PROCESSID, PHM_PRODUCT_LINE_CODE, 
--    -- EXPERIENCE_CODE, IS_MANUAL_EXEC
-- -- FROM SVC_PHM_OWNER.PHM_ALG_OUTPUT
-- -- Where
--              -- FLAG_DATE > TRUNC(SYSDATE)-3      
--              -- and FLAG_DATE < TRUNC(SYSDATE)
--                    
-- -- and (CREATED_BY like '%5756%')
-- -- and FLAG_YN='YES' 

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
    run_date between (sysdate-7) and sysdate
and
    -- phm_patterns_sk between 10000 and 10331
    phm_algorithm_definitions_sk in ( 1001, 1065 )
order by 
    flag_date desc

