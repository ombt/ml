
--    CURSOR ALL_THRESHOLDS (V_ALG_NUM NUMBER)
--    IS
--     SELECT 
--         PHM_PATTERNS_SK, 
--         PATTERN_NAME, 
--         PHM_THRESHOLDS_SK, 
--         ISSUE_DESCRIPTION, 
--         ALGORITHM_TYPE, 
--         PATTERN_TEXT, 
--         ERROR_COUNT, 
--         IHN_LEVEL3_DESC , 
--         TO_NUMBER(THRESHOLD_DATA_DAYS) AS THRESHOLD_DATA_DAYS, 
--         TO_NUMBER(THRESHOLDS_DAYS) AS THRESHOLDS_DAYS, 
--         THRESHOLD_DESCRIPTION, 
--         THRESHOLD_TYPE as THRESHOLD_TYPE   
--     from (
--         SELECT 
--             tp.phm_patterns_sk, 
--             p.pattern_name as pattern_name, 
--             thr.phm_thresholds_sk, 
--             ihn.issue_description, 
--             tp.parameter_name, 
--             tp.parameter_values  
--         from 
--             phm_threshold_parameter tp, 
--             phm_patterns p, 
--             (
--                 select 
--                     phm_patterns_sk, 
--                     issue_description 
--                 from 
--                     phm_algorithm_ihns pai 
--                 where 
--                     -- pai.phm_algorithm_definitions_sk = V_ALG_NUM
--                     pai.phm_algorithm_definitions_sk = 1060
--             ) ihn,
--             (
--                 select 
--                     phm_patterns_sk, 
--                     phm_thresholds_sk 
--                 from 
--                     phm_thresholds pt 
--                 where 
--                     -- pt.phm_algorithm_definitions_sk = V_ALG_NUM
--                     pt.phm_algorithm_definitions_sk = 1060
--             ) thr 
--         where 
--             tp.phm_patterns_sk = p.phm_patterns_sk 
--         and 
--             nvl(tp.delete_flag, 'N') <> 'Y'
--         and 
--             -- tp.phm_d_algorithm_definitions_sk = V_ALG_NUM 
--             tp.phm_d_algorithm_definitions_sk = 1060
--         and 
--             p.phm_patterns_sk = ihn.phm_patterns_sk 
--         and 
--             p.phm_patterns_sk = thr.phm_patterns_sk
--         ) 
--     pivot (
--         max(parameter_values) for parameter_name in 
--         (
--             'ALGORITHM_TYPE' as ALGORITHM_TYPE, 
--             'ERROR_CODE_REG_EXPR' as PATTERN_TEXT , 
--             'ERROR_COUNT' as ERROR_COUNT, 
--             'IHN_LEVEL3_DESC' as IHN_LEVEL3_DESC, 
--             'THRESHOLD_DATA_DAYS' as THRESHOLD_DATA_DAYS, 
--             'THRESHOLDS_DAYS' as THRESHOLDS_DAYS, 
--             'THRESHOLD_DESCRIPTION' as THRESHOLD_DESCRIPTION, 
--             'THRESHOLD_TYPE' as THRESHOLD_TYPE
--         )
--     )
--     ORDER by 
--         algorithm_type, 
--         pattern_name;

SELECT 
    PHM_PATTERNS_SK, 
    PATTERN_NAME, 
    PHM_THRESHOLDS_SK, 
    ISSUE_DESCRIPTION, 
    ALGORITHM_TYPE, 
    PATTERN_TEXT, 
    ERROR_COUNT, 
    IHN_LEVEL3_DESC , 
    TO_NUMBER(THRESHOLD_DATA_DAYS) AS THRESHOLD_DATA_DAYS, 
    TO_NUMBER(THRESHOLDS_DAYS) AS THRESHOLDS_DAYS, 
    THRESHOLD_DESCRIPTION, 
    THRESHOLD_TYPE as THRESHOLD_TYPE   
from (
    SELECT 
        tp.phm_patterns_sk, 
        p.pattern_name as pattern_name, 
        thr.phm_thresholds_sk, 
        ihn.issue_description, 
        tp.parameter_name, 
        tp.parameter_values  
    from 
        phm_threshold_parameter tp, 
        phm_patterns p, 
        (
            select 
                phm_algorithm_definitions_sk,
                phm_patterns_sk, 
                issue_description 
            from 
                phm_algorithm_ihns pai 
            -- where 
                -- pai.phm_algorithm_definitions_sk = V_ALG_NUM
                -- pai.phm_algorithm_definitions_sk = 1080
        ) ihn,
        (
            select 
                phm_algorithm_definitions_sk,
                phm_patterns_sk, 
                phm_thresholds_sk 
            from 
                phm_thresholds pt 
            -- where 
                -- pt.phm_algorithm_definitions_sk = V_ALG_NUM
                -- pt.phm_algorithm_definitions_sk = 1080
        ) thr 
    where 
        ihn.phm_algorithm_definitions_sk = thr.phm_algorithm_definitions_sk 
    and
        thr.phm_algorithm_definitions_sk = tp.phm_d_algorithm_definitions_sk 
    and
        tp.phm_patterns_sk = p.phm_patterns_sk 
    and 
        nvl(tp.delete_flag, 'N') <> 'Y'
    -- and 
        -- tp.phm_d_algorithm_definitions_sk = V_ALG_NUM 
        -- tp.phm_d_algorithm_definitions_sk = 1080
    and 
        p.phm_patterns_sk = ihn.phm_patterns_sk 
    and 
        p.phm_patterns_sk = thr.phm_patterns_sk
    and
        p.phm_patterns_sk in ( 10329, 10330, 10331 )
    ) 
pivot (
    max(parameter_values) for parameter_name in 
    (
        'ALGORITHM_TYPE' as ALGORITHM_TYPE, 
        'ERROR_CODE_REG_EXPR' as PATTERN_TEXT , 
        'ERROR_COUNT' as ERROR_COUNT, 
        'IHN_LEVEL3_DESC' as IHN_LEVEL3_DESC, 
        'THRESHOLD_DATA_DAYS' as THRESHOLD_DATA_DAYS, 
        'THRESHOLDS_DAYS' as THRESHOLDS_DAYS, 
        'THRESHOLD_DESCRIPTION' as THRESHOLD_DESCRIPTION, 
        'THRESHOLD_TYPE' as THRESHOLD_TYPE
    )
)
ORDER by 
    algorithm_type, 
    pattern_name;
