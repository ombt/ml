with threshold_params as (
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
        algorithm_type, 
        pattern_name
)
SELECT ASI.PRODUCTLINEREF,
               ASI.DEVICEID,
               ASI.SYSTEMSN,
               ALN.SN,
               AE.NODETYPE,
               AE.ERRORCODE,
               MAX (AE.COMPLETIONDATE) MAX_COMPL_DATE,
               TRUNC (MIN (AE.COMPLETIONDATE)) MIN_COMPL_DATE
          FROM SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
               A3600_LAYOUT_NODES_PL_SN ALN,
               IDAOWNER.A3600SYSTEMINFORMATION ASI
         WHERE     BATCH_NUM = V_BATCH_NUM
               AND RUN_DATE = V_RUN_DATE
               AND AE.ERRORCODE = tps2.pattern_description
               AND AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
               AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
               AND ALN.SN IS NOT NULL
               AND ALN.CANID = AE.NODEID
               AND ASI.CURRENT_ROW = 'Y'
               AND (   (tps2.module_type != '%' AND AE.NODETYPE = tps2.module_type)
                    OR (tps2.module_type = '%' AND AE.NODETYPE LIKE tps2.module_type))
      GROUP BY ASI.PRODUCTLINEREF,
               ASI.DEVICEID,
               ASI.SYSTEMSN,
               ALN.SN,
               AE.NODETYPE,
               AE.ERRORCODE
      ORDER BY ASI.SYSTEMSN, AE.NODETYPE, AE.ERRORCODE;
from (
    select
        tps.phm_thresholds_sk,
        tps.threshold_number_unit,
        tps.threshold_number_desc,
        tps.phm_patterns_sk,
        tps.thresholds_sk_val,
        tps.pattern_description,
        tps.threshold_alert,
        tps.algorithm_type,
        tps.threshold_data_days,
        tps.module_type,
        case when ((tps.pattern_description = '0405' and 
                    tps.module_type = 'IOM') or 
                   (tps.pattern_description = '0605' and 
                    tps.module_type = 'CM'))
             then
                 NULL
             when (tps.pattern_description = '5015' and 
                   tps.module_type = 'ISR')
             then
                 NULL
             else
                ' '
             end as v_samp_id_chk1,
        case when ((tps.pattern_description = '0405' and 
                    tps.module_type = 'IOM') or 
                   (tps.pattern_description = '0605' and 
                    tps.module_type = 'CM'))
             then
                 'amp;U__'
             when (tps.pattern_description = '5015' and 
                   tps.module_type = 'ISR')
             then
                '%'
             else
                '%'
             end as v_samp_id_chk2
    from 
        threshold_params tps
) tps2

