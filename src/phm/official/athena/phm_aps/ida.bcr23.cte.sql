--     CURSOR ALL_THRESHOLDS (V_ALG_NUM NUMBER)
--     IS
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
--                     pai.phm_algorithm_definitions_sk = V_ALG_NUM
--             ) ihn,
--             (
--                 select 
--                     phm_patterns_sk, 
--                     phm_thresholds_sk 
--                 from 
--                     phm_thresholds pt 
--                 where 
--                     pt.phm_algorithm_definitions_sk = V_ALG_NUM
--             ) thr 
--         where 
--             tp.phm_patterns_sk = p.phm_patterns_sk 
--         and 
--             nvl(tp.delete_flag, 'N') <> 'Y'
--         and 
--             tp.phm_d_algorithm_definitions_sk = V_ALG_NUM 
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
-- 
-- ================================================================================================================
-- 
--    CURSOR CURAPS_DEVICES_DATES -- ( STARTDATE DATE, ENDDATE DATE)
--    IS
--        SELECT  
--            SN, 
--            MIN(TRUNC(TIMESTAMP)) DT, 
--            MAX(TIMESTAMP) DT_MAX
--        FROM 
--            SVC_PHM_ODS.PHM_ODS_APS_ERRORS
--        WHERE 
--            RUN_DATE = V_RUN_DATE 
--        AND 
--            BATCH_NUM = V_BATCH_NUM  
--        AND 
--            TRUNC(TIMESTAMP) <> TRUNC(SYSDATE) -- This check is to ignore current day data as the data for current day could be coming next day  because of the way rules were defined in AbbottLink
--        GROUP BY 
--            SN 
--        ORDER BY 1,2;
-- 
-- ================================================================================================================
-- 
--    CURSOR CURAPS_ERRORS_BCR23 (VSN VARCHAR2, 
--                                VSTARTDATE DATE, 
--                                VENDDATE DATE, 
--                                PATTERN_TEXT VARCHAR2)
--    IS 
--        SELECT 
--            DEVICE_ID, 
--            MAX(PSM.IOM_SN) IOM_SN, 
--            PSM.PL, 
--            PSM.SN, 
--            TRUNC(TIMESTAMP) DT, 
--            COUNT(*) PAT_ERRCOUNT, 
--            MAX(TIMESTAMP) FLG_DATE
--        FROM 
--            SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, 
--            SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM 
--        WHERE 
--            AE.MESSAGE LIKE '%' || PATTERN_TEXT  || '%' AND PSM.MESSAGE LIKE '%' || PATTERN_TEXT  || '%' 
--        and 
--            AE.SN = PSM.IOM_SN 
--        AND 
--            AE.SN = VSN  
--        AND 
--            TIMESTAMP BETWEEN 
--                VSTARTDATE 
--            AND 
--                VENDDATE
--        GROUP BY 
--            DEVICE_ID, 
--            PSM.PL, 
--            PSM.SN, 
--            TRUNC(TIMESTAMP)
--        ORDER BY 
--            IOM_SN, 
--            SN, 
--            DT;
-- 
-- ================================================================================================================
-- 
--         FOR X IN CURAPS_DEVICES_DATES
--          LOOP
--         -- DBMS_OUTPUT.PUT_LINE('SN: ' || X.SN || ', X.DT: ' || X.DT || ', DT_MAX: ' || X.DT_MAX);
--             V_TOTAL_COUNT := V_TOTAL_COUNT + 1;
--             FOR Y IN (
--                 SELECT 
--                     DEVICE_ID,
--                     TRUNC(TIMESTAMP) DT, 
--                     SN, 
--                     DURATION, 
--                     DESCRIPTION, 
--                     ID, 
--                     MAX(VALUE) MAX_VALUE, 
--                     MIN(VALUE) MIN_VALUE
--                 FROM 
--                     APS_COUNTERS 
--                 WHERE 
--                     SN = X.SN 
--                 AND 
--                     TRUNC(TIMESTAMP) BETWEEN 
--                         X.DT 
--                     AND 
--                         X.DT_MAX + 1 -- TRUNC(TIMESTAMP) BETWEEN X.DT AND X.DT_MAX
--                 AND 
--                     ID IN ('normal','priority','tubes',
--                            '1','2','3','4','5','6','7','8')  
--                 AND 
--                     DURATION IN ('YTD')
--                 AND 
--                     DESCRIPTION IN ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
--                 GROUP BY 
--                     DEVICE_ID, 
--                     TRUNC(TIMESTAMP), 
--                     SN, 
--                     DURATION, 
--                     DESCRIPTION, 
--                     ID )
--             LOOP
--                 SELECT 
--                     COUNT(1) INTO V_COUNTERS_COUNT 
--                 FROM 
--                     PHM_APS_COUNTERS_TEMP
--                 WHERE 
--                     ID = Y.ID 
--                 AND 
--                     DURATION = Y.DURATION  
--                 AND 
--                     DESCRIPTION = Y.DESCRIPTION 
--                 AND 
--                     SN = Y.SN 
--                 AND 
--                     TIMESTAMP = Y.DT;
-- 
--                 -- DBMS_OUTPUT.PUT_LINE('Y.SN: ' || Y.SN || ', Y.DURATION: ' || Y.DURATION || ', Y.DT: ' || Y.DT || ', Y.ID: ' || Y.ID || ', Y.DESCRIPTION: ' || Y.DESCRIPTION || ', V_COUNTERS_COUNT: ' || V_COUNTERS_COUNT);
-- 
--                 IF V_COUNTERS_COUNT > 0 
--                 THEN
--                     UPDATE 
--                         PHM_APS_COUNTERS_TEMP 
--                     SET 
--                         MAX_VALUE = Y.MAX_VALUE, 
--                         MIN_VALUE = Y.MIN_VALUE 
--                     WHERE 
--                         ID = Y.ID 
--                     AND 
--                         DURATION = Y.DURATION  
--                     AND 
--                         DESCRIPTION = Y.DESCRIPTION 
--                     AND 
--                         SN = Y.SN 
--                     AND 
--                         TIMESTAMP = Y.DT;
--                     V_UPDATE_COUNT := V_UPDATE_COUNT + 1;
--                 ELSE
--                     INSERT INTO PHM_APS_COUNTERS_TEMP 
--                     (
--                         DEVICE_ID, 
--                         SN , 
--                         TIMESTAMP, 
--                         ID, 
--                         DURATION, 
--                         DESCRIPTION, 
--                         MIN_VALUE, 
--                         MAX_VALUE
--                     ) 
--                     VALUES
--                     (
--                         Y.DEVICE_ID, 
--                         Y.SN, 
--                         Y.DT, 
--                         Y.ID, 
--                         Y.DURATION, 
--                         Y.DESCRIPTION, 
--                         Y.MIN_VALUE, 
--                         Y.MAX_VALUE
--                     );
--                     V_INSERT_COUNT := V_INSERT_COUNT + 1;
--                 END IF;
--             END LOOP;
--         END LOOP;
-- 
-- ================================================================================================================
-- 
--         FOR TH IN ALL_THRESHOLDS (V_ALG_NUM)
--         LOOP
--              
--             IF TH.ALGORITHM_TYPE = 'BCR_2/3'  
--             THEN
--                 V_INSERT_COUNT := 0;
--                 FOR X IN CURAPS_DEVICES_DATES
--                 LOOP
--                     VSTARTDATE := X.DT - TH.THRESHOLDS_DAYS;
--                     FOR Y IN CURAPS_ERRORS_BCR23 (X.SN, 
--                                                   VSTARTDATE, 
--                                                   X.DT_MAX, 
--                                                   TH.PATTERN_TEXT)
--                     LOOP
--                         VCOUNT_1_MIN    := 0;
--                         VCOUNT_1_MAX    := 0;
--                         ERRORS_PER_DAY  := 0;
--                         PER_ERROR_COUNT := 0;
--                         BEGIN
--                             SELECT 
--                                 MAX_VALUE INTO VCOUNT_1_MIN 
--                             FROM (
--                                 SELECT 
--                                     * 
--                                 FROM 
--                                     PHM_APS_COUNTERS_TEMP 
--                                 WHERE 
--                                     ID = 'tubes' 
--                                 AND 
--                                     DURATION = 'YTD' 
--                                 AND 
--                                     DESCRIPTION = 'CentrifugeCounter' 
--                                 AND 
--                                     SN = Y.IOM_SN 
--                                 AND 
--                                     TIMESTAMP <= Y.DT 
--                                 ORDER BY 
--                                     TIMESTAMP DESC
--                             ) 
--                             WHERE 
--                                 ROWNUM < 2;
--                         EXCEPTION WHEN NO_DATA_FOUND 
--                         THEN
--                             VCOUNT_1_MIN := 0;
--                             V_ERROR_MESSAGE := 'NOT ABLE TO GET MAX VALUE FOR TUBES-YTD-CENTRIFUGECOUNTER FOR ' || Y.IOM_SN || ' FOR DATE ' || Y.DT || ', WITH ERROR :' || SQLERRM;
--                             --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
--                         END;
-- 
--                         BEGIN
--                             SELECT 
--                                 MAX_VALUE INTO VCOUNT_1_MAX 
--                             FROM 
--                                 PHM_APS_COUNTERS_TEMP 
--                             WHERE 
--                                 ID = 'tubes' 
--                             AND 
--                                 DURATION = 'YTD' 
--                             AND 
--                                 DESCRIPTION = 'CentrifugeCounter' 
--                             AND 
--                                 SN = Y.IOM_SN 
--                             AND 
--                                 TIMESTAMP = Y.DT + 1;
--                         EXCEPTION WHEN NO_DATA_FOUND 
--                         THEN
--                             VCOUNT_1_MAX := 0;
--                             V_ERROR_MESSAGE := 'NOT ABLE TO GET MAX VALUE FOR TUBES-YTD-CENTRIFUGECOUNTER FOR ' || Y.IOM_SN || ' FOR DATE ' || Y.DT || ', WITH ERROR :' || SQLERRM;
--                             --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
--                         END;
-- 
--                         DBMS_OUTPUT.PUT_LINE('VCOUNT_1_MIN: ' || VCOUNT_1_MIN || ', VCOUNT_1_MAX: ' || VCOUNT_1_MAX || ', Y.IOM_SN: ' || Y.IOM_SN || ', Y.DT: ' || Y.DT); 
-- 
--                         BEGIN
--                             ERRORS_PER_DAY := VCOUNT_1_MAX - VCOUNT_1_MIN;
--                             IF ERRORS_PER_DAY > 0 
--                             THEN
--                                 PER_ERROR_COUNT := (Y.PAT_ERRCOUNT * 100) / ERRORS_PER_DAY;
--                             ELSE
--                                 PER_ERROR_COUNT := 0;
--                             END IF;
--                         EXCEPTION WHEN OTHERS THEN
--                             PER_ERROR_COUNT := 0; ERRORS_PER_DAY := 0;
--                             V_ERROR_MESSAGE := 'NOT ABLE TO CALCULATE PER_DAY_ERROR_COUNT OR NOT ABLE TO INSERT DATA INTO PHM_APS_DATA FOR ' || Y.SN || ' FOR DATE ' || Y.FLG_DATE || ' FOR ' || VALGNAME || ', WITH ERROR :' || SQLERRM;
--                             -- PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
--                         END;
--                      
--                         DBMS_OUTPUT.PUT_LINE('Inserting BCR_2/3 data into PHM_APS_DATA for PL: ' || Y.PL || ', IOM_SN: ' || Y.IOM_SN || ', Y.SN: ' || Y.SN || ', PATTERN: ' || TH.PATTERN_TEXT || ', Y.FLG_DATE: ' || Y.FLG_DATE || ', ERRORS_PER_DAY: ' || ERRORS_PER_DAY || ', Y.PAT_ERRCOUNT: ' || Y.PAT_ERRCOUNT || ', PER_ERROR_COUNT: ' || PER_ERROR_COUNT);                      
--                           
--                         INSERT INTO SVC_PHM_OWNER.PHM_APS_DATA 
--                         (
--                             PHM_ALGORITHM_DEFINITIONS_SK, 
--                             PHM_PATTERNS_SK, 
--                             BATCH_NUM, 
--                             RUN_DATE, 
--                             DEVICE_ID, 
--                             IOM_SN, 
--                             PL, 
--                             SN, 
--                             TIMESTAMP, 
--                             ALGORITHM_TYPE, 
--                             TESTCOUNT, 
--                             ERRORCOUNT, 
--                             ERRORPCT, 
--                             TIMESTAMP_MS, 
--                             ADDED_BY, 
--                             DATE_CREATED
--                         )
--                         VALUES
--                         (
--                             V_ALG_NUM, 
--                             TH.PHM_PATTERNS_SK, 
--                             V_BATCH_NUM, 
--                             V_RUN_DATE, 
--                             Y.DEVICE_ID, 
--                             Y.IOM_SN, 
--                             Y.PL, 
--                             Y.SN, 
--                             Y.FLG_DATE, 
--                             TH.ALGORITHM_TYPE, 
--                             ERRORS_PER_DAY, 
--                             Y.PAT_ERRCOUNT, 
--                             TRUNC(PER_ERROR_COUNT, 5), 
--                             0, 
--                             VALGNAME, 
--                             SYSDATE
--                         );
-- 
--                         V_INSERT_COUNT := V_INSERT_COUNT + 1;
-- 
--                         IF MOD(V_INSERT_COUNT, 10000) = 0 
--                         THEN 
--                             COMMIT; 
--                         END IF;
--                     END LOOP;                  
--                 END LOOP;
--                 COMMIT;
--             END IF;
-- 
--         END LOOP;
-- 
-- ================================================================================================================
-- 
--         FOR Z IN ALL_THRESHOLDS(V_ALG_NUM)
--         LOOP
--             IF Z.ALGORITHM_TYPE = 'BCR_2/3' 
--             THEN
--                 DBMS_OUTPUT.PUT_LINE('Processing the thresholds for BCR_2/3 algorithm ' || Z.PATTERN_TEXT || ', THRESHOLD_DESCRIPTION: ' || Z.THRESHOLD_DESCRIPTION); 
--                 FOR Y IN (
--                     SELECT DISTINCT 
--                         IL.PL, 
--                         PAT.SN, 
--                         PAT.DEVICE_ID, 
--                         IL.CUSTOMER, 
--                         IL.CUSTOMER_NUM, 
--                         PC.AREAREGION, 
--                         PC.COUNTRY
--                     FROM 
--                         INSTRUMENTLISTING IL, 
--                         PHM_APS_DATA PAT, 
--                         PHM_COUNTRY PC 
--                     WHERE 
--                         PAT.SN = IL.SN
--                     AND   
--                         PAT.PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK 
--                     AND  
--                         PC.COUNTRY_CODE = IL.COUNTRY_CODE
--                     AND  
--                         PAT.BATCH_NUM = V_BATCH_NUM 
--                     AND 
--                         PAT.RUN_DATE = V_RUN_DATE
--                     AND 
--                         INST_STATUS = 'Active' 
--                     AND 
--                         CMS_STATUS = 'Active')
--                 LOOP
--     
--                     CURR_DAY := NULL;
--                     PREV_DAY := NULL;
--                     CONSECUTIVE_DAYS := TRUE;
--                     TODAY_ERRORPCT := 0; 
--                     FLAGGING_DAYS := 0;
--                     FLAG := 'NO';
--                     IHN_VALUE := NULL;
--                     V_INSERT_COUNT := 0;
--     
--                     FOR D IN (
--                         SELECT 
--                             * 
--                         FROM 
--                             PHM_APS_DATA 
--                         WHERE  
--                             SN = Y.SN 
--                         AND 
--                             PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK 
--                         AND 
--                             BATCH_NUM = V_BATCH_NUM 
--                         AND 
--                             RUN_DATE = V_RUN_DATE 
--                         ORDER BY 
--                             TIMESTAMP
--                         )
--                     LOOP
--                         BEGIN
--     
--                         FLAG := 'NO';
--                         IHN_VALUE := NULL;
--                         V_FLAGGED_PL := NULL;
--                         V_FLAGGED_EXP_CODE := NULL;  
--     
--                         -- Consecutive days scenario Ex: 10 errors/day for 2 consecutive days; 2 errors/day for 3 consecutive days
--                         IF (Z.THRESHOLD_TYPE = 'CONSECUTIVE') 
--                         THEN
--                             CURR_DAY := TRUNC(D.TIMESTAMP);
--                             TODAY_ERRORPCT := D.ERRORPCT;
--                             -- check if next date in the cursor is one less then the previous day? - means consecutive or not
--                             IF (PREV_DAY IS NOT NULL AND CURR_DAY <> (PREV_DAY + 1)) 
--                             THEN
--                                 CONSECUTIVE_DAYS := FALSE;
--                             END IF;
--     
--                             IF (CONSECUTIVE_DAYS = TRUE AND (TODAY_ERRORPCT >= Z.ERROR_COUNT)) 
--                             THEN
--                                 FLAGGING_DAYS := FLAGGING_DAYS + 1;
--                             ELSE
--                                 FLAGGING_DAYS := 0;                        
--                             END IF;  
--     
--                             PREV_DAY := CURR_DAY;
--     
--                             IF (FLAGGING_DAYS >= Z.THRESHOLDS_DAYS) 
--                             THEN
--                                 FLAG := 'YES';
--                                 IHN_VALUE :=  Z.ISSUE_DESCRIPTION;  --Z.THRESHOLD_ALERT;
--                                 PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Z.PHM_PATTERNS_SK, Y.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
--                             END IF;
--                         END IF;                       
--                         EXCEPTION
--                         WHEN OTHERS THEN
--                             FLAG := 'NO'; IHN_VALUE := NULL; TODAY_ERRORPCT:= 0;
--                             VSQLERRORMSG :=  ' PER CENT OF COUNT OF ERRORS HAS FAILED FOR PHM_PATTERNS_SK ' || Z.PHM_PATTERNS_SK || ' FOR '|| Y.SN ||' FOR DATE '|| D.TIMESTAMP || ', ERROR :' || SQLERRM;
--                             PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG  (V_PROCESS_ID,V_ALG_NUM,V_RUN_DATE,V_BATCH_NUM,VSQLERRORMSG,VALGNAME);
--                         END;
--     
--                         DBMS_OUTPUT.PUT_LINE('THRESHOLD_TYPE: ' || Z.THRESHOLD_TYPE || ', SN: ' || Y.SN || ', TIMESTAMP: ' ||  D.TIMESTAMP || ', CURR_DAY: ' || CURR_DAY || ', PREV_DAY: ' || PREV_DAY || ', CONSECUTIVE_DAYS: ' || (CASE WHEN (CONSECUTIVE_DAYS = TRUE) THEN 'TRUE'  ELSE 'FALSE' END) || ', TODAY_ERRORPCT: ' || TODAY_ERRORPCT || ', FLAG: ' || FLAG || ', IHN_VALUE: ' || IHN_VALUE || ', V_FLAGGED_PL: ' || V_FLAGGED_PL || ', V_FLAGGED_EXP_CODE: ' || V_FLAGGED_EXP_CODE);  
--                         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(Y.CUSTOMER, 
--                                                                                  Y.CUSTOMER_NUM, 
--                                                                                  Y.DEVICE_ID, 
--                                                                                  Y.SN, 
--                                                                                  Y.COUNTRY, 
--                                                                                  Y.AREAREGION, 
--                                                                                  V_ALG_NUM , 
--                                                                                  Z.PHM_THRESHOLDS_SK, 
--                                                                                  D.TIMESTAMP, 
--                                                                                  TODAY_ERRORPCT, 
--                                                                                  FLAG, 
--                                                                                  IHN_VALUE, 
--                                                                                  NULL, 
--                                                                                  VALGNAME, 
--                                                                                  V_PROD_FAMILY, 
--                                                                                  V_BATCH_NUM, 
--                                                                                  Z.PHM_PATTERNS_SK, 
--                                                                                  V_RUN_DATE, 
--                                                                                  V_PROCESS_ID, 
--                                                                                  V_FLAGGED_PL, 
--                                                                                  V_FLAGGED_EXP_CODE);
--                              
--                         V_INSERT_COUNT := V_INSERT_COUNT + 1;
--                         IF MOD(V_INSERT_COUNT, 10000) = 0 
--                         THEN 
--                             COMMIT; 
--                         END IF;
--                     END LOOP;
--                 END LOOP;
--                 COMMIT;
--             END IF;
--               
--         END LOOP;  -- END of loop for Thresholds
-- 
-- 

with threshold_parameters_cte as (
select 
    phm_patterns_sk, 
    pattern_name, 
    phm_thresholds_sk, 
    issue_description, 
    algorithm_type, 
    pattern_text, 
    error_count, 
    ihn_level3_desc , 
    to_number(threshold_data_days) as threshold_data_days, 
    to_number(thresholds_days) as thresholds_days, 
    threshold_description, 
    threshold_type as threshold_type   
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
                phm_algorithm_definitions_sk,
                phm_patterns_sk, 
                issue_description 
            from 
                phm_algorithm_ihns pai 
        ) ihn,
        (
            select 
                phm_algorithm_definitions_sk,
                phm_patterns_sk, 
                phm_thresholds_sk 
            from 
                phm_thresholds pt 
        ) thr 
    where 
        ihn.phm_algorithm_definitions_sk = thr.phm_algorithm_definitions_sk 
    and
        thr.phm_algorithm_definitions_sk = tp.phm_d_algorithm_definitions_sk 
    and
        tp.phm_patterns_sk = p.phm_patterns_sk 
    and 
        nvl(tp.delete_flag, 'N') <> 'Y'
    and 
        p.phm_patterns_sk = ihn.phm_patterns_sk 
    and 
        p.phm_patterns_sk = thr.phm_patterns_sk
    and
        -- p.phm_patterns_sk in ( 10329, 10330, 10331 )
        p.phm_patterns_sk in ( 10330 )
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
order by 
    algorithm_type, 
    pattern_name
),
dates_and_devices as (
select  
    sn, 
    min(trunc(timestamp)) dt, 
    max(timestamp) dt_max
from 
    svc_phm_ods.phm_ods_aps_errors
where 
    run_date = trunc(sysdate)
and 
    batch_num = 'BTH0600'
and 
    trunc(timestamp) <> trunc(sysdate)
group by 
    sn 
order by 1,2
),
aps_errors_bcr23_cte as (
select
    tp.pattern_text,
    tp.thresholds_days,
    ae.device_id, 
    trunc(ae.timestamp) as dt, 
    max(ae.timestamp) as flg_date,
    count(*) as pat_errcount, 
    max(psm.iom_sn) as iom_sn, 
    psm.pl, 
    psm.sn
from
    threshold_parameters_cte tp
inner join
    svc_phm_ods.phm_ods_aps_errors ae
on
    ae.message like '%' || tp.pattern_text  || '%' 
inner join
    svc_gsr_owner.aps_messages_pl_sn_map psm 
on
    psm.message like '%' || tp.pattern_text  || '%' 
inner join
    dates_and_devices dd
on
    ae.sn = dd.sn
and
    psm.iom_sn = dd.sn
and 
    timestamp between 
        (dd.dt - tp.thresholds_days)
    and 
        dd.dt_max
group by 
    tp.pattern_text,
    tp.thresholds_days,
    ae.device_id, 
    psm.pl, 
    psm.sn, 
    trunc(ae.timestamp)
order by 
    tp.pattern_text,
    tp.thresholds_days,
    max(psm.iom_sn), 
    psm.sn, 
    trunc(ae.timestamp)
)
select * from aps_errors_bcr23_cte 
