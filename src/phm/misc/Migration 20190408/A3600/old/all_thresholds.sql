-- select * from SVC_PHM_OWNER.PHM_ALGORITHM_DEFINITIONS where algorithm_name like '%3600%'
-- 
-- A3600 algorithms have vn_ALG_NUM = 1040
--
-- CURSOR ALL_THRESHOLDS (vn_ALG_NUM    NUMBER)
-- IS

    select 
        phm_patterns_sk phm_thresholds_sk,
        case when threshold_number is null 
             then 
                 9999
             else 
                 to_number (threshold_number)
             end threshold_number,
        case when threshold_number_unit is null 
             then 
                 9999
             else 
                 to_number (threshold_number_unit)
             end threshold_number_unit,
        threshold_number_desc,
        phm_patterns_sk phm_patterns_sk,
        phm_thresholds_sk as thresholds_sk_val,
        pattern_description,
        threshold_alert,
        algorithm_type as algorithm_type,
        case when threshold_data_days is null 
        then 
            9999
        else 
            to_number (threshold_data_days)
        end threshold_data_days,
        case when module_type = 'ALL' 
             then 
                 '%' 
             else 
                 module_type 
             end module_type
    from (
        select 
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
                    phm_patterns_sk, 
                    issue_description
                from 
                    phm_algorithm_ihns pai
                where 
                    pai.phm_algorithm_definitions_sk = 1040 -- vn_alg_num
            ) ihn,
            (
                select 
                    phm_patterns_sk, 
                    phm_thresholds_sk
                from 
                    phm_thresholds pt
                where 
                    pt.phm_algorithm_definitions_sk = 1040 -- vn_alg_num
            ) thr
        where     
            tp.phm_patterns_sk = p.phm_patterns_sk
        and 
            nvl (tp.delete_flag, 'N') <> 'Y'
        and 
            tp.phm_d_algorithm_definitions_sk = 1040 -- vn_alg_num
        and 
            p.phm_patterns_sk = ihn.phm_patterns_sk
        and 
            p.phm_patterns_sk = thr.phm_patterns_sk
    ) pivot ( 
        max ( parameter_values )
        for 
            parameter_name
        in (
            'ALGORITHM_TYPE' as algorithm_type,
            'ERROR_CODE_VALUE' as pattern_description,
            'ERROR_COUNT' as threshold_number,
            'IHN_LEVEL3_DESC' as threshold_alert,
            'THRESHOLD_DATA_DAYS' as threshold_data_days,
            'THRESHOLDS_DAYS' as threshold_number_unit,
            'THRESHOLD_DESCRIPTION' as threshold_number_desc,
            'MODULE' as module_type
        )
    )
    order by 
        phm_thresholds_sk,
        algorithm_type, 
        pattern_name;

