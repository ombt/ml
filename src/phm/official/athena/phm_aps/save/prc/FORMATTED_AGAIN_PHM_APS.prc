CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_APS(V_ALG_NUM NUMBER,V_RUN_DATE DATE,V_BATCH_NUM VARCHAR2, V_UNIX_ID VARCHAR2)
AS

    VALGNAME VARCHAR(25);
    VSQLERRORMSG VARCHAR(2000);
    VSTARTDATE DATE;
    VENDDATE DATE;
    V_30DAYS_MIN_DATE DATE;
    COUNT_FLAG NUMBER(10);

    VCOUNT_NORMAL_MIN     NUMBER;
    PER_ERROR_COUNT       NUMBER;
    ERRORS_PER_DAY        NUMBER;
    VCOUNT_NORMAL_MAX     NUMBER;
    VCOUNT_PRIORITY_MAX   NUMBER;
    VCOUNT_PRIORITY_MIN   NUMBER;
    VERR205_THRESHOLD     NUMBER(10,5);
    FLAG_IND              NUMBER;

    LAS_MAX_PERCENTAGE NUMBER(15,5);
    LAS_MAX_ERROR_COUNT NUMBER;
    LAS_ERRORS_PER_DAY NUMBER;

    LASX_MIN_COUNT NUMBER; 
    LASX_MAX_COUNT NUMBER;
    LASX_PERCENTAGE NUMBER(15,5); 

    CURR_DAY DATE;
    PREV_DAY DATE;
    CONSECUTIVE_DAYS BOOLEAN;
    CURR_DAY_ERRCOUNT NUMBER; 
    FLAGGING_DAYS NUMBER;
    TOTAL_ERROR_COUNT NUMBER;

    VSTARTDATE_SD      DATE;
    VDEVICEID          VARCHAR2(10);
    VSN                VARCHAR2(10);
    VCSQLERRORMSG     VARCHAR2(500);
    VDATE   DATE;
    VCUR_ERR_205          NUMBER;
    VCUR_TEST_COUNT NUMBER;
    VCUR_ERR_COUNT NUMBER;
    VCUR_MSDT DATE;
    VTRSHLD_205_ALERT  VARCHAR2(100);


    VCOUNT_1_MIN          NUMBER;
    VCOUNT_1_MAX          NUMBER;

    PREV_DAY_ERRCOUNT NUMBER(10):= 0;
    TODAY_ERRCOUNT NUMBER(10):= 0;

    PREV_DAY_ERRORPCT  NUMBER(10,5);
    TODAY_ERRORPCT  NUMBER(10,5);

    VERR205_FLAG VARCHAR2(10) := 'NO';
    VPRE_ERR_205 NUMBER(10) := 0;

    LAS4_ERROR_COUNT NUMBER;
    LAS4_PERCENTAGE NUMBER(15,5);
    TH_DT_COUNT NUMBER(15);

    TODAY_TEST_COUNT NUMBER(10):= 0;
    ONE_SD_BELOW_MEAN_COUNT NUMBER(10):= 0;
    SD15_ABOVE_30MEAN_ERRORPCT NUMBER(15,8):= 100000;

    CONSEQ_COUNT NUMBER(2):= 0;
    FLAG VARCHAR(25) := 'NO';
    IHN_VALUE VARCHAR(100) := NULL;
    V_RERUN_PREV_DATE DATE;
    V_INSERT_COUNT NUMBER(19);
    V_UPDATE_COUNT NUMBER(19);
    V_TOTAL_COUNT NUMBER(19);
    V_EXISTING_REC_COUNT NUMBER(19);
    V_FLAGGED_PL       VARCHAR2(10);
    V_FLAGGED_EXP_CODE VARCHAR2(10);


--    CURSOR ALL_THRESHOLDS (V_ALG_NUM NUMBER)
--    IS
--    SELECT PTT.PHM_THRESHOLDS_SK,PTT.THRESHOLD_NUMBER,PTT.THRESHOLD_NUMBER_UNIT,PTT.THRESHOLD_NUMBER_DESC,
--    PP.PHM_PATTERNS_SK,PP.PATTERN_DESCRIPTION,PTT.THRESHOLD_ALERT,PTT.THRESHOLD_TYPE, PTT.THRESHOLD_DATA_DAYS
--    FROM PHM_PATTERNS PP, PHM_THRESHOLDS PTT WHERE PP.PHM_PATTERNS_SK = PTT.PHM_PATTERNS_SK
--    AND PP.PHM_ALGORITHM_DEFINITIONS_SK = PTT.PHM_ALGORITHM_DEFINITIONS_SK --AND PTT.THRESHOLD_TYPE <> 'COUNT'
--    AND PP.PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM ;
    
    CURSOR ALL_THRESHOLDS (V_ALG_NUM NUMBER)
    IS
    SELECT PHM_PATTERNS_SK, PATTERN_NAME, PHM_THRESHOLDS_SK, ISSUE_DESCRIPTION, ALGORITHM_TYPE, PATTERN_TEXT, ERROR_COUNT, IHN_LEVEL3_DESC
        , TO_NUMBER(THRESHOLD_DATA_DAYS) AS THRESHOLD_DATA_DAYS, TO_NUMBER(THRESHOLDS_DAYS) AS THRESHOLDS_DAYS, THRESHOLD_DESCRIPTION, THRESHOLD_TYPE as THRESHOLD_TYPE   
    from (
        SELECT tp.phm_patterns_sk, p.pattern_name as pattern_name, thr.phm_thresholds_sk, ihn.issue_description, tp.parameter_name, tp.parameter_values  
        from phm_threshold_parameter tp, phm_patterns p, 
        (select phm_patterns_sk, issue_description from phm_algorithm_ihns pai where pai.phm_algorithm_definitions_sk = V_ALG_NUM) ihn,
        (select phm_patterns_sk, phm_thresholds_sk from phm_thresholds pt where pt.phm_algorithm_definitions_sk = V_ALG_NUM) thr 
        where tp.phm_patterns_sk = p.phm_patterns_sk and nvl(tp.delete_flag, 'N') <> 'Y'
        and tp.phm_d_algorithm_definitions_sk = V_ALG_NUM and p.phm_patterns_sk = ihn.phm_patterns_sk and p.phm_patterns_sk = thr.phm_patterns_sk
    ) pivot (max(parameter_values) for parameter_name in  ('ALGORITHM_TYPE' as ALGORITHM_TYPE, 'ERROR_CODE_REG_EXPR' as PATTERN_TEXT
        , 'ERROR_COUNT' as ERROR_COUNT, 'IHN_LEVEL3_DESC' as IHN_LEVEL3_DESC, 'THRESHOLD_DATA_DAYS' as THRESHOLD_DATA_DAYS
        , 'THRESHOLDS_DAYS' as THRESHOLDS_DAYS, 'THRESHOLD_DESCRIPTION' as THRESHOLD_DESCRIPTION, 'THRESHOLD_TYPE' as THRESHOLD_TYPE)
    )
    ORDER by algorithm_type, pattern_name;
    

--    CURSOR  CURAPS_DEVICE_LAS (V_SN VARCHAR2, V_START_DATE Date,V_END_DATE DATE )
--    IS
--    SELECT ERR.DEVICE_ID,SN, TRUNC(TIMESTAMP) DT ,MAX(TIMESTAMP) MAX_TIMESTAMP
--    FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS ERR
--    WHERE TIMESTAMP BETWEEN V_START_DATE AND V_END_DATE AND SN = V_SN
--    GROUP BY ERR.DEVICE_ID ,SN , TRUNC(TIMESTAMP)
--    ORDER BY 1,2,3;
    
    CURSOR  CURAPS_DEVICE_LAS (V_SN VARCHAR2, V_START_DATE DATE, V_END_DATE DATE, LAS_PATTERN_TEXT VARCHAR2)
    IS
    SELECT MAX(A.DEVICE_ID) DEVICE_ID, MAX(A.IOM_SN) IOM_SN, MAX(A.PL) PL, MAX(A.SN) SN, A.DT, MAX(A.TIMESTAMP) MAX_TIMESTAMP, COUNT(*) LAS_ERROR_COUNT FROM 
         (SELECT AE.DEVICE_ID, PSM.IOM_SN, PSM.PL, PSM.SN, TRUNC(AE.TIMESTAMP) DT, AE.TIMESTAMP from SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM 
                WHERE AE.SN = V_SN AND AE.MESSAGE = LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' AND TIMESTAMP BETWEEN V_START_DATE AND V_END_DATE
                AND PSM.MESSAGE LIKE LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' || '%' and AE.SN = PSM.IOM_SN) A
       , (SELECT DEVICE_ID, SN, TRUNC(TIMESTAMP) DT, TIMESTAMP from SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE 
                WHERE SN = V_SN AND MESSAGE like 'Interface Module Unreadable Barcode -%' AND TIMESTAMP BETWEEN V_START_DATE AND V_END_DATE) B
    WHERE A.TIMESTAMP = B.TIMESTAMP
    GROUP BY A.DT ORDER BY 1, 2, 3;

    CURSOR CURAPS_ERRORS_BCR1 (VSN VARCHAR, V_START_DATE DATE, V_END_DATE DATE)
    IS
    SELECT DEVICE_ID, MAX(PL) PL, SN, TRUNC(TIMESTAMP) DT, COUNT (*) PAT_ERRCOUNT, MAX(TIMESTAMP) FLG_DATE
    FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS WHERE MESSAGE LIKE '%BCR%1%' AND SN = VSN
    AND TIMESTAMP BETWEEN V_START_DATE AND V_END_DATE  -- TRUNC(SD_START_DATE) AND TRUNC(SD_END_DATE)
    GROUP BY DEVICE_ID, SN, TRUNC(TIMESTAMP)
    ORDER BY DEVICE_ID, SN, DT;

--   CURSOR CURAPS_ERRORS_BCR23 (VSN VARCHAR, VSTARTDATE    DATE, VENDDATE DATE)
--   IS
--   SELECT DEVICE_ID, SN,TRUNC (TIMESTAMP) DT,COUNT (*) PAT_ERRCOUNT,MAX (TIMESTAMP) FLG_DATE
--   FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS WHERE     (MESSAGE LIKE '%BCR%2%' OR MESSAGE LIKE '%BCR%3%')
--   AND SN = VSN  AND TIMESTAMP BETWEEN TRUNC(VSTARTDATE) AND TRUNC(VENDDATE)
--   GROUP BY DEVICE_ID, SN, TRUNC (TIMESTAMP)
--   ORDER BY 1, 2, 3;
   
   CURSOR CURAPS_ERRORS_BCR23 (VSN VARCHAR2, VSTARTDATE DATE, VENDDATE DATE, PATTERN_TEXT VARCHAR2)
   IS 
   SELECT DEVICE_ID, MAX(PSM.IOM_SN) IOM_SN, PSM.PL, PSM.SN, TRUNC(TIMESTAMP) DT, COUNT(*) PAT_ERRCOUNT, MAX(TIMESTAMP) FLG_DATE
   FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM 
   WHERE  AE.MESSAGE LIKE '%' || PATTERN_TEXT  || '%' AND PSM.MESSAGE LIKE '%' || PATTERN_TEXT  || '%' and AE.SN = PSM.IOM_SN 
   AND AE.SN = VSN  AND TIMESTAMP BETWEEN VSTARTDATE AND VENDDATE
   GROUP BY DEVICE_ID, PSM.PL, PSM.SN, TRUNC(TIMESTAMP)
   ORDER BY IOM_SN, SN, DT;
   
   -- Get the min and max timestamps grouping by SN for the given batch and run_date; Only get this data for the previous day
   CURSOR CURAPS_DEVICES_DATES -- ( STARTDATE DATE, ENDDATE DATE)
   IS
   SELECT  SN, MIN(TRUNC(TIMESTAMP)) DT, MAX(TIMESTAMP) DT_MAX
   FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS
   WHERE RUN_DATE = V_RUN_DATE AND BATCH_NUM = V_BATCH_NUM  
        AND TRUNC(TIMESTAMP) <> TRUNC(SYSDATE) -- This check is to ignore current day data as the data for current day could be coming next day  because of the way rules were defined in AbbottLink
   GROUP BY SN ORDER BY 1,2;

   V_RUN_MODE VARCHAR2(25);
   V_PROCESS_TYPE VARCHAR2(25);
   V_ROUTINE_NAME VARCHAR2(25);
   V_PROCESS_ID NUMBER(10);
   V_PROCESS_STATUS  VARCHAR2(25) := 'STARTTED' ;
   V_ERROR_MESSAGE VARCHAR2(4000);
   V_PROD_FAMILY  VARCHAR2(25);
   V_ROUTINE_TYPE VARCHAR2(25);
   V_INST_PL VARCHAR2(10);
   V_CUSTOMER  VARCHAR2(50);
   V_CUST_NUM  VARCHAR2(25);
   V_CITY  VARCHAR2(25);
   V_COUNTRY  VARCHAR2(25);
   V_COUNTERS_COUNT NUMBER(5);

BEGIN

  VENDDATE := SYSDATE;

  PHM_ALGORITHM_UTILITIES_1.PHM_GET_ALG_DETAILS(V_ALG_NUM, VALGNAME,V_PROCESS_TYPE,V_ROUTINE_NAME,V_RUN_MODE,V_PROD_FAMILY);
  V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
  
  
  DBMS_OUTPUT.PUT_LINE('STARTING APS ALGORITHM EXECUTION!!  VALGNAME: ' || VALGNAME || ', STATUS: ' || V_PROCESS_STATUS || ', SYSDATE: ' || SYSDATE);  

  IF VALGNAME IS NOT NULL THEN

    BEGIN
         -- DELETE FROM PHM_APS_TEMP WHERE BATCH_NUM = V_BATCH_NUM; -- TODO - check why removing the entire batch data irrespective of the date?
         -- DELETE FROM PHM_ALG_OUTPUT WHERE PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM AND  BATCH_NUM = V_BATCH_NUM; -- TODO - check why removing the entire batch data irrespective of the date?
         -- STEP 5a : CHECK DATA EXISTS FOR THE BATCH_NUM in PHM_APS_DATA table     
         SELECT COUNT(*) INTO V_EXISTING_REC_COUNT FROM PHM_APS_DATA  WHERE BATCH_NUM = V_BATCH_NUM;
            
         DBMS_OUTPUT.PUT_LINE('EXISTING RECORD COUNT IN PHM_APS_DATA table for BATCH_NUM: ' || V_BATCH_NUM || ' IS: ' || V_EXISTING_REC_COUNT);          
         IF V_EXISTING_REC_COUNT > 0 THEN
           DELETE FROM PHM_APS_DATA WHERE BATCH_NUM = V_BATCH_NUM;
           COMMIT;
         END IF;
         DBMS_OUTPUT.PUT_LINE('TOTAL RECORDS DELETED FROM PHM_APS_DATA table for the BATCH_NUM: ' || V_BATCH_NUM || ' IS: ' || V_EXISTING_REC_COUNT);
          
         -- STEP 5a : CHECK DATA EXISTS FOR BATCH AND RUN DATE IN THE ALGORITHM OUTPUT TABLE   , IF DATA EXISTS DELETE THE DATA FROM OUTPUT TABLE       
         SELECT COUNT(*) INTO V_EXISTING_REC_COUNT FROM PHM_ALG_OUTPUT  WHERE PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM AND BATCH_NUM = V_BATCH_NUM;
            
         DBMS_OUTPUT.PUT_LINE('EXISTING RECORD COUNT IN PHM_ALG_OUTPUT table for PHM_ALGORITHM_DEFINITIONS_SK: ' || V_ALG_NUM || ', BATCH_NUM: ' || V_BATCH_NUM || ' IS: ' || V_EXISTING_REC_COUNT);          
         IF V_EXISTING_REC_COUNT > 0 THEN
           DELETE FROM PHM_ALG_OUTPUT WHERE PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM AND BATCH_NUM = V_BATCH_NUM;
           COMMIT;
         END IF;
         DBMS_OUTPUT.PUT_LINE('TOTAL RECORDS DELETED FROM PHM_ALG_OUTPUT table for PHM_ALGORITHM_DEFINITIONS_SK: ' || V_ALG_NUM || ', BATCH_NUM: ' || V_BATCH_NUM || ' IS: ' || V_EXISTING_REC_COUNT);
          
          
    EXCEPTION
        WHEN OTHERS THEN
          V_PROCESS_STATUS := 'ERRORED';
          V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA OF PREVIOUS RUN FOR RUN_DATE ' || V_RUN_DATE || ' FOR BATCH_NUM ' || V_BATCH_NUM ||' DUE TO  : ' || SQLERRM;
          PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
    END;
    
    IF V_PROCESS_STATUS <>  'ERRORED' THEN
       V_PROCESS_STATUS := 'STARTED';
       PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID, V_PROD_FAMILY, V_PROCESS_TYPE, V_ROUTINE_TYPE
                , VALGNAME, V_ROUTINE_NAME, V_RUN_MODE, V_PROCESS_STATUS, V_ERROR_MESSAGE, V_RUN_DATE, SYSDATE, V_BATCH_NUM, V_UNIX_ID,V_ALG_NUM );

       V_INSERT_COUNT := 0;
       V_UPDATE_COUNT := 0;
       V_TOTAL_COUNT := 0;
       
       FOR X IN CURAPS_DEVICES_DATES
       LOOP
        -- DBMS_OUTPUT.PUT_LINE('SN: ' || X.SN || ', X.DT: ' || X.DT || ', DT_MAX: ' || X.DT_MAX);
        V_TOTAL_COUNT := V_TOTAL_COUNT + 1;
        FOR Y IN (SELECT DEVICE_ID,TRUNC(TIMESTAMP) DT, SN, DURATION, DESCRIPTION, ID, MAX(VALUE) MAX_VALUE, MIN(VALUE) MIN_VALUE
                   FROM APS_COUNTERS WHERE SN = X.SN AND TRUNC(TIMESTAMP) BETWEEN X.DT AND X.DT_MAX + 1 -- TRUNC(TIMESTAMP) BETWEEN X.DT AND X.DT_MAX
                        AND ID IN ('normal', 'priority','tubes','1','2','3','4','5','6','7','8')  AND DURATION IN ('YTD')
                        AND DESCRIPTION IN ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
                   GROUP BY DEVICE_ID, TRUNC(TIMESTAMP), SN, DURATION, DESCRIPTION, ID)
         LOOP
            SELECT COUNT(1) INTO V_COUNTERS_COUNT FROM PHM_APS_COUNTERS_TEMP
                  WHERE ID = Y.ID AND DURATION = Y.DURATION  AND DESCRIPTION = Y.DESCRIPTION AND SN = Y.SN AND TIMESTAMP = Y.DT;

            -- DBMS_OUTPUT.PUT_LINE('Y.SN: ' || Y.SN || ', Y.DURATION: ' || Y.DURATION || ', Y.DT: ' || Y.DT || ', Y.ID: ' || Y.ID || ', Y.DESCRIPTION: ' || Y.DESCRIPTION || ', V_COUNTERS_COUNT: ' || V_COUNTERS_COUNT);

            IF V_COUNTERS_COUNT > 0 THEN
              UPDATE PHM_APS_COUNTERS_TEMP SET MAX_VALUE = Y.MAX_VALUE, MIN_VALUE = Y.MIN_VALUE WHERE ID = Y.ID AND DURATION = Y.DURATION  AND DESCRIPTION = Y.DESCRIPTION AND SN = Y.SN AND TIMESTAMP = Y.DT;
              V_UPDATE_COUNT := V_UPDATE_COUNT + 1;
            ELSE
              INSERT INTO PHM_APS_COUNTERS_TEMP(DEVICE_ID, SN, TIMESTAMP, ID, DURATION, DESCRIPTION, MIN_VALUE, MAX_VALUE) 
                      VALUES(Y.DEVICE_ID, Y.SN, Y.DT, Y.ID, Y.DURATION, Y.DESCRIPTION, Y.MIN_VALUE, Y.MAX_VALUE);
              V_INSERT_COUNT := V_INSERT_COUNT + 1;
            END IF;
         END LOOP;
       END LOOP;
       
       COMMIT;
       
       DBMS_OUTPUT.PUT_LINE('Total instruments for the batch: ' || V_TOTAL_COUNT || ', Update/Insert count in PHM_APS_COUNTERS_TEMP -> V_UPDATE_COUNT: ' || V_UPDATE_COUNT 
                 || ', V_INSERT_COUNT: ' || V_INSERT_COUNT || ', TOTAL COUNT: ' || (V_UPDATE_COUNT + V_INSERT_COUNT));

       FOR TH IN ALL_THRESHOLDS (V_ALG_NUM)
       LOOP
             DBMS_OUTPUT.PUT_LINE('Pre-Processing PHM_PATTERNS_SK: ' || TH.PHM_PATTERNS_SK || ', PATTERN_NAME: ' || TH.PATTERN_NAME || ', ALGORITHM_TYPE: ' || TH.ALGORITHM_TYPE 
                        || ', PATTERN_TEXT: ' || TH.PATTERN_TEXT || ', ERROR_COUNT: ' || TH.ERROR_COUNT || ', IHN_LEVEL3_DESC: ' || TH.IHN_LEVEL3_DESC || ', THRESHOLD_DATA_DAYS: ' || TH.THRESHOLD_DATA_DAYS 
                        || ', THRESHOLDS_DAYS: ' || TH.THRESHOLDS_DAYS || ', THRESHOLD_DESCRIPTION: ' || TH.THRESHOLD_DESCRIPTION || ', THRESHOLD_TYPE: ' || TH.THRESHOLD_TYPE);    
             
             IF TH.ALGORITHM_TYPE = 'BCR_1'  THEN
               V_INSERT_COUNT := 0; 
               FOR X IN CURAPS_DEVICES_DATES
               LOOP
                  VSTARTDATE := X.DT - TH.THRESHOLD_DATA_DAYS;  -- last 30 days data
                  FOR Y IN CURAPS_ERRORS_BCR1 (X.SN, VSTARTDATE, X.DT_MAX)
                  LOOP
                    VCOUNT_NORMAL_MIN    := 0;
                    VCOUNT_NORMAL_MAX    := 0;
                    VCOUNT_PRIORITY_MIN  := 0;
                    VCOUNT_PRIORITY_MAX  := 0;
                    ERRORS_PER_DAY       := 0;
                    PER_ERROR_COUNT      := 0;
                    BEGIN
                       SELECT MAX_VALUE INTO VCOUNT_NORMAL_MIN FROM (SELECT *  FROM PHM_APS_COUNTERS_TEMP WHERE ID = 'normal' AND DURATION = 'YTD'
                            AND DESCRIPTION = 'InputTubeCounter' AND SN = Y.SN AND TIMESTAMP <= Y.DT  ORDER BY TIMESTAMP DESC) WHERE ROWNUM < 2;
                    EXCEPTION
                      WHEN OTHERS THEN
                       VCOUNT_NORMAL_MIN := 0; 
                       V_ERROR_MESSAGE := ' NOT ABLE TO FIND MAX COUNT FOR CONDITION - NORMAL - YTD - INPUTTBECOUNTER : FOR' || Y.SN || ' FOR DATE ' || Y.DT || ', WITH ERROR :' || SQLERRM;
                       --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;

                    BEGIN
                       SELECT MAX_VALUE INTO VCOUNT_NORMAL_MAX FROM PHM_APS_COUNTERS_TEMP WHERE ID =  'normal' AND DURATION = 'YTD' AND DESCRIPTION = 'InputTubeCounter' AND SN = Y.SN AND TIMESTAMP = Y.DT + 1;
                    EXCEPTION
                       WHEN OTHERS THEN
                        VCOUNT_NORMAL_MAX := 0;
                        V_ERROR_MESSAGE := 'NOT ABLE TO FIND MAX COUNT FOR CONDITION - NORMAL - YTD - INPUTTBECOUNTER for :' || Y.SN || ' FOR DATE ' || Y.DT || ', WITH ERROR :' || SQLERRM;
                        --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;

                    BEGIN
                      SELECT MAX_VALUE INTO VCOUNT_PRIORITY_MIN FROM ( SELECT *  FROM PHM_APS_COUNTERS_TEMP WHERE ID = 'priority' AND DURATION = 'YTD'
                            AND DESCRIPTION = 'InputTubeCounter' AND SN = Y.SN AND TIMESTAMP <= Y.DT  ORDER BY TIMESTAMP DESC) WHERE ROWNUM < 2;
                    EXCEPTION
                      WHEN OTHERS THEN
                       VCOUNT_PRIORITY_MIN := 0;
                       V_ERROR_MESSAGE := 'NOT ABLE TO FIND MAX COUNT FOR CONDITION - PRIORITY - YTD - INPUTTBECOUNTER :' || Y.SN || ' FOR DATE  : ' || Y.DT || ', WITH ERROR :' || SQLERRM;
                       --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;

                    BEGIN
                       SELECT MAX_VALUE INTO VCOUNT_PRIORITY_MAX FROM PHM_APS_COUNTERS_TEMP WHERE ID =  'priority' AND DURATION = 'YTD' AND DESCRIPTION = 'InputTubeCounter'
                            AND SN = Y.SN AND TIMESTAMP = Y.DT + 1;
                    EXCEPTION
                      WHEN OTHERS THEN
                       VCOUNT_PRIORITY_MAX := 0;
                       V_ERROR_MESSAGE := 'NOT ABLE TO FIND MAX COUNT FOR CONDITION - PRIORITY - YTD - INPUTTBECOUNTER :' || Y.SN || ' FOR DATE : ' || Y.DT || ', WITH ERROR :' || SQLERRM;
                       --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;
                    DBMS_OUTPUT.PUT_LINE('VCOUNT_NORMAL_MIN: ' || VCOUNT_NORMAL_MIN || ', VCOUNT_NORMAL_MAX: ' || VCOUNT_NORMAL_MAX  || ', VCOUNT_PRIORITY_MIN: ' || VCOUNT_PRIORITY_MIN 
                             || ', VCOUNT_PRIORITY_MAX: ' || VCOUNT_PRIORITY_MAX || ', Y.SN: ' || Y.SN || ', Y.DT: ' || Y.DT);
                    BEGIN
                       ERRORS_PER_DAY := VCOUNT_PRIORITY_MAX + VCOUNT_NORMAL_MAX - VCOUNT_PRIORITY_MIN - VCOUNT_NORMAL_MIN;
                       IF ERRORS_PER_DAY > 0 THEN
                          PER_ERROR_COUNT := (Y.PAT_ERRCOUNT * 100) / ERRORS_PER_DAY;
                       ELSE
                          PER_ERROR_COUNT := 0;
                       END IF;
                    EXCEPTION
                       WHEN OTHERS THEN
                          PER_ERROR_COUNT := 0; ERRORS_PER_DAY := 0;
                          V_ERROR_MESSAGE := 'NOT ABLE TO CALCULATE PER_DAY_ERROR_COUNT OR NOT ABLE TO INSERT DATA INTO PHM_APS_DATA FOR '||VALGNAME||', WITH ERROR :'|| SQLERRM;
                          --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG  (V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;
                    
                    DBMS_OUTPUT.PUT_LINE('Inserting BCR_1 data into PHM_APS_DATA for PL: ' || Y.PL || ', Y.SN: ' || Y.SN || ', PATTERN: ' || TH.PATTERN_TEXT 
                              || ', Y.FLG_DATE: ' || Y.FLG_DATE || ', ERRORS_PER_DAY: ' || ERRORS_PER_DAY || ', Y.PAT_ERRCOUNT: ' || Y.PAT_ERRCOUNT || ', PER_ERROR_COUNT: ' || PER_ERROR_COUNT);                      
                    
                    
                    INSERT INTO SVC_PHM_OWNER.PHM_APS_DATA (PHM_ALGORITHM_DEFINITIONS_SK, PHM_PATTERNS_SK, BATCH_NUM, RUN_DATE, DEVICE_ID, IOM_SN, PL, SN
                                  , TIMESTAMP, ALGORITHM_TYPE, TESTCOUNT, ERRORCOUNT, ERRORPCT, TIMESTAMP_MS, ADDED_BY, DATE_CREATED)
                          VALUES  (V_ALG_NUM, TH.PHM_PATTERNS_SK, V_BATCH_NUM, V_RUN_DATE, Y.DEVICE_ID, Y.SN, Y.PL, Y.SN
                                  , Y.FLG_DATE, TH.ALGORITHM_TYPE, ERRORS_PER_DAY, Y.PAT_ERRCOUNT, TRUNC(PER_ERROR_COUNT, 5), 0, VALGNAME, SYSDATE);
                    V_INSERT_COUNT := V_INSERT_COUNT + 1;
                    IF MOD(V_INSERT_COUNT,10000) = 0 THEN COMMIT; END IF;

                  END LOOP;
               END LOOP;
               COMMIT;
               DBMS_OUTPUT.PUT_LINE('Total records into PHM_APS_DATA table for BCR_1 algorithm: ' || V_INSERT_COUNT);
                
             END IF;
             
             IF TH.ALGORITHM_TYPE = 'BCR_2/3'  THEN
                V_INSERT_COUNT := 0;
                FOR X IN CURAPS_DEVICES_DATES
                LOOP
                  VSTARTDATE := X.DT - TH.THRESHOLDS_DAYS;
                  FOR Y IN CURAPS_ERRORS_BCR23 (X.SN, VSTARTDATE, X.DT_MAX, TH.PATTERN_TEXT)
                  LOOP
                    VCOUNT_1_MIN    := 0;
                    VCOUNT_1_MAX    := 0;
                    ERRORS_PER_DAY  := 0;
                    PER_ERROR_COUNT := 0;
                    BEGIN
                       SELECT MAX_VALUE INTO VCOUNT_1_MIN FROM (SELECT * FROM PHM_APS_COUNTERS_TEMP WHERE ID = 'tubes' AND DURATION = 'YTD' AND DESCRIPTION = 'CentrifugeCounter' 
                            AND SN = Y.IOM_SN AND TIMESTAMP <= Y.DT ORDER BY TIMESTAMP DESC) WHERE ROWNUM < 2;
                    EXCEPTION
                       WHEN NO_DATA_FOUND THEN
                          VCOUNT_1_MIN := 0;
                          V_ERROR_MESSAGE := 'NOT ABLE TO GET MAX VALUE FOR TUBES-YTD-CENTRIFUGECOUNTER FOR ' || Y.IOM_SN || ' FOR DATE ' || Y.DT || ', WITH ERROR :' || SQLERRM;
                          --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;
                    BEGIN
                       SELECT MAX_VALUE INTO VCOUNT_1_MAX FROM PHM_APS_COUNTERS_TEMP WHERE ID = 'tubes' AND DURATION = 'YTD' AND DESCRIPTION = 'CentrifugeCounter' AND SN = Y.IOM_SN AND TIMESTAMP = Y.DT + 1;
                    EXCEPTION
                       WHEN NO_DATA_FOUND THEN
                          VCOUNT_1_MAX := 0;
                          V_ERROR_MESSAGE := 'NOT ABLE TO GET MAX VALUE FOR TUBES-YTD-CENTRIFUGECOUNTER FOR ' || Y.IOM_SN || ' FOR DATE ' || Y.DT || ', WITH ERROR :' || SQLERRM;
                          --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;
                    DBMS_OUTPUT.PUT_LINE('VCOUNT_1_MIN: ' || VCOUNT_1_MIN || ', VCOUNT_1_MAX: ' || VCOUNT_1_MAX || ', Y.IOM_SN: ' || Y.IOM_SN || ', Y.DT: ' || Y.DT); 
                    BEGIN
                      ERRORS_PER_DAY := VCOUNT_1_MAX - VCOUNT_1_MIN;
                      IF ERRORS_PER_DAY > 0 THEN
                         PER_ERROR_COUNT := (Y.PAT_ERRCOUNT * 100) / ERRORS_PER_DAY;
                      ELSE
                         PER_ERROR_COUNT := 0;
                      END IF;
                    EXCEPTION
                      WHEN OTHERS THEN
                        PER_ERROR_COUNT := 0; ERRORS_PER_DAY := 0;
                        V_ERROR_MESSAGE := 'NOT ABLE TO CALCULATE PER_DAY_ERROR_COUNT OR NOT ABLE TO INSERT DATA INTO PHM_APS_DATA FOR ' || Y.SN || ' FOR DATE ' || Y.FLG_DATE
                                                   || ' FOR ' || VALGNAME || ', WITH ERROR :' || SQLERRM;
                        -- PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                    END;
                     
                    DBMS_OUTPUT.PUT_LINE('Inserting BCR_2/3 data into PHM_APS_DATA for PL: ' || Y.PL || ', IOM_SN: ' || Y.IOM_SN || ', Y.SN: ' || Y.SN || ', PATTERN: ' || TH.PATTERN_TEXT 
                              || ', Y.FLG_DATE: ' || Y.FLG_DATE || ', ERRORS_PER_DAY: ' || ERRORS_PER_DAY || ', Y.PAT_ERRCOUNT: ' || Y.PAT_ERRCOUNT || ', PER_ERROR_COUNT: ' || PER_ERROR_COUNT);                      
                          
                    INSERT INTO SVC_PHM_OWNER.PHM_APS_DATA (PHM_ALGORITHM_DEFINITIONS_SK, PHM_PATTERNS_SK, BATCH_NUM, RUN_DATE, DEVICE_ID, IOM_SN, PL, SN
                                , TIMESTAMP, ALGORITHM_TYPE, TESTCOUNT, ERRORCOUNT, ERRORPCT, TIMESTAMP_MS, ADDED_BY, DATE_CREATED)
                                         VALUES  (V_ALG_NUM, TH.PHM_PATTERNS_SK, V_BATCH_NUM, V_RUN_DATE, Y.DEVICE_ID, Y.IOM_SN, Y.PL, Y.SN
                                , Y.FLG_DATE, TH.ALGORITHM_TYPE, ERRORS_PER_DAY, Y.PAT_ERRCOUNT, TRUNC(PER_ERROR_COUNT, 5), 0, VALGNAME, SYSDATE);
                                        
                    V_INSERT_COUNT := V_INSERT_COUNT + 1;
                    IF MOD(V_INSERT_COUNT, 10000) = 0 THEN COMMIT; END IF;
                  END LOOP;                  
                END LOOP;
                COMMIT;
             END IF;

             IF TH.ALGORITHM_TYPE = 'LAS_205'  THEN
                V_INSERT_COUNT := 0;
                FOR Y IN CURAPS_DEVICES_DATES
                LOOP
                    VSTARTDATE := Y.DT - TH.THRESHOLDS_DAYS;
                    DBMS_OUTPUT.PUT_LINE('LAS_205, pattern text: ' || TH.PATTERN_TEXT || ', VSTARTDATE: ' || VSTARTDATE || ', Y.DT_MAX: ' || Y.DT_MAX || ', SN: ' || Y.SN);
                    FOR X IN CURAPS_DEVICE_LAS (Y.SN, VSTARTDATE, Y.DT_MAX, TH.PATTERN_TEXT)
                    LOOP
                      LASX_MIN_COUNT := 0; 
                      LASX_MAX_COUNT := 0;
                      ERRORS_PER_DAY := 0;
                      LASX_PERCENTAGE := 0; 

                      BEGIN
                         SELECT MAX_VALUE INTO LASX_MIN_COUNT FROM (SELECT * FROM PHM_APS_COUNTERS_TEMP WHERE ID = SUBSTR(TH.PATTERN_TEXT, 4, 4) AND DURATION = 'YTD' 
                            AND DESCRIPTION = 'InstrumentBufferCounter' AND SN = X.IOM_SN AND TIMESTAMP <= X.DT ORDER BY TIMESTAMP DESC) WHERE ROWNUM < 2;
                      EXCEPTION
                         WHEN NO_DATA_FOUND THEN
                            LASX_MIN_COUNT := 0;
                            V_ERROR_MESSAGE := 'NOT ABLE TO GET MAX VALUE FOR LAS ID#-YTD-InstrumentBufferCounter FOR ' || X.IOM_SN || ' FOR DATE ' || X.DT || ', WITH ERROR :' || SQLERRM;
                            --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                      END;
                      BEGIN
                         SELECT MAX_VALUE INTO LASX_MAX_COUNT FROM PHM_APS_COUNTERS_TEMP WHERE ID = SUBSTR(TH.PATTERN_TEXT, 4, 4) AND DURATION = 'YTD' 
                            AND DESCRIPTION = 'InstrumentBufferCounter' AND SN = X.IOM_SN AND TIMESTAMP = X.DT + 1;
                      EXCEPTION
                         WHEN NO_DATA_FOUND THEN
                            LASX_MAX_COUNT := 0;
                            V_ERROR_MESSAGE := 'NOT ABLE TO GET MAX VALUE FOR LAS ID#-YTD-InstrumentBufferCounter FOR ' || X.IOM_SN || ' FOR DATE ' || X.DT || ', WITH ERROR :' || SQLERRM;
                            --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                      END;
                      
                      DBMS_OUTPUT.PUT_LINE('LASX_MIN_COUNT: ' || LASX_MIN_COUNT || ', LASX_MAX_COUNT: ' || LASX_MAX_COUNT || ', X.IOM_SN: ' || X.IOM_SN || ', X.DT: ' || X.DT);
                      BEGIN
                         ERRORS_PER_DAY := LASX_MAX_COUNT - LASX_MIN_COUNT;
                         IF ERRORS_PER_DAY > 0 THEN
                            LASX_PERCENTAGE := (X.LAS_ERROR_COUNT * 100) / ERRORS_PER_DAY;
                         ELSE
                            LASX_PERCENTAGE := 0;
                         END IF;
                      EXCEPTION
                         WHEN OTHERS THEN
                             LASX_PERCENTAGE := 0; ERRORS_PER_DAY := 0;
                             V_ERROR_MESSAGE := 'NOT ABLE TO CALCULATE LAS PERCENTAGE OR NOT ABLE TO INSERT DATA INTO PHM_APS_DATA FOR ' || X.SN || ' FOR DATE ' 
                                                    || X.MAX_TIMESTAMP ||' FOR ' || VALGNAME || ', WITH ERROR :' || SQLERRM;
                            --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG  (V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, V_ERROR_MESSAGE, VALGNAME);
                      END;
                          
                      DBMS_OUTPUT.PUT_LINE('Inserting LAS_205 data into PHM_APS_DATA for PL: ' || X.PL || ', IOM_SN: ' || X.IOM_SN || ', X.SN: ' || X.SN || ', PATTERN: ' || TH.PATTERN_TEXT 
                            || ', X.MS_TIME: ' || X.MAX_TIMESTAMP || ', ERRORS_PER_DAY: ' || ERRORS_PER_DAY || ', X.LAS_ERROR_COUNT: ' || X.LAS_ERROR_COUNT || ', LASX_PERCENTAGE: ' || LASX_PERCENTAGE);                      
                          
                      INSERT INTO SVC_PHM_OWNER.PHM_APS_DATA (PHM_ALGORITHM_DEFINITIONS_SK, PHM_PATTERNS_SK, BATCH_NUM, RUN_DATE, DEVICE_ID, IOM_SN, PL, SN
                                , TIMESTAMP, ALGORITHM_TYPE, TESTCOUNT, ERRORCOUNT, ERRORPCT, TIMESTAMP_MS, ADDED_BY, DATE_CREATED)
                                VALUES  (V_ALG_NUM, TH.PHM_PATTERNS_SK, V_BATCH_NUM, V_RUN_DATE, X.DEVICE_ID, X.IOM_SN, X.PL, X.SN
                                , X.MAX_TIMESTAMP, TH.PATTERN_TEXT, ERRORS_PER_DAY, X.LAS_ERROR_COUNT, TRUNC(LASX_PERCENTAGE, 5), 0, VALGNAME, SYSDATE);
                          
                      V_INSERT_COUNT := V_INSERT_COUNT + 1;
                      IF MOD(V_INSERT_COUNT, 10000) = 0 THEN COMMIT; END IF;
                    END LOOP;
                END LOOP;
               COMMIT;
             END IF;

             IF TH.ALGORITHM_TYPE = 'COUNT' THEN
                V_INSERT_COUNT := 0;
                
               /* SELECT DEVICE_ID,SN,TRUNC(TIMESTAMP) FLAG_DATE, COUNT(FILEID) ERRORCOUNT,max(timestamp) MS_TIME
                          FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE
                          WHERE  TIMESTAMP BETWEEN (SELECT  MIN (TRUNC(TIMESTAMP)) - TH.THRESHOLD_DATA_DAYS FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS
                          WHERE RUN_DATE = V_RUN_DATE AND BATCH_NUM = V_BATCH_NUM AND SN =AE.SN) AND (SELECT  MAX (TIMESTAMP)
                          FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS WHERE RUN_DATE = V_RUN_DATE AND BATCH_NUM = V_BATCH_NUM AND SN = AE.SN and TRUNC(TIMESTAMP)<> TRUNC(SYSDATE))
                          AND MESSAGE LIKE TH.PATTERN_DESCRIPTION GROUP BY DEVICE_ID,SN,TRUNC(TIMESTAMP) ORDER BY SN,FLAG_DATE */
                
                FOR Y IN (SELECT DEVICE_ID, MAX(PSM.IOM_SN) IOM_SN, PSM.PL, PSM.SN MODULE_SN, TRUNC(TIMESTAMP) FLAG_DATE, COUNT(FILEID) ERRORCOUNT, max(timestamp) MS_TIME
                               FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM
                               WHERE TIMESTAMP BETWEEN 
                                        (SELECT  MIN (TRUNC(TIMESTAMP)) - TH.THRESHOLDS_DAYS FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS 
                                            WHERE RUN_DATE = V_RUN_DATE AND BATCH_NUM = V_BATCH_NUM AND SN = AE.SN) 
                                    AND (SELECT  MAX (TIMESTAMP) FROM SVC_PHM_ODS.PHM_ODS_APS_ERRORS 
                                            WHERE RUN_DATE = V_RUN_DATE AND BATCH_NUM = V_BATCH_NUM AND SN = AE.SN and TRUNC(TIMESTAMP) <> TRUNC(SYSDATE))
                                    AND AE.MESSAGE LIKE '%' || TH.PATTERN_TEXT  || '%' AND PSM.MESSAGE LIKE '%' || TH.PATTERN_TEXT  || '%' and AE.SN = PSM.IOM_SN 
                               GROUP BY DEVICE_ID, PSM.PL, PSM.SN, TRUNC(TIMESTAMP) ORDER BY IOM_SN, MODULE_SN, FLAG_DATE)
                  LOOP

                    BEGIN
                      DBMS_OUTPUT.PUT_LINE('Inserting COUNT data into PHM_APS_DATA for PL: ' || Y.PL || ', IOM_SN: ' || Y.IOM_SN || ', Y.SN: ' || Y.MODULE_SN || ', PATTERN: ' || TH.PATTERN_TEXT || ', Y.MS_TIME: ' || Y.MS_TIME); 
                      INSERT INTO SVC_PHM_OWNER.PHM_APS_DATA (PHM_ALGORITHM_DEFINITIONS_SK, PHM_PATTERNS_SK, BATCH_NUM, RUN_DATE, DEVICE_ID, IOM_SN, PL, SN
                                , TIMESTAMP, ALGORITHM_TYPE, TESTCOUNT, ERRORCOUNT, ERRORPCT, TIMESTAMP_MS, ADDED_BY, DATE_CREATED)
                                VALUES  (V_ALG_NUM, TH.PHM_PATTERNS_SK, V_BATCH_NUM, V_RUN_DATE, Y.DEVICE_ID, Y.IOM_SN, Y.PL, Y.MODULE_SN
                                , Y.MS_TIME, TH.ALGORITHM_TYPE, 0, Y.ERRORCOUNT, 0, 0, VALGNAME, SYSDATE);
                      
                    V_INSERT_COUNT := V_INSERT_COUNT + 1;
                    IF MOD(V_INSERT_COUNT, 10000) = 0 THEN COMMIT; END IF;
                    EXCEPTION
                      WHEN OTHERS THEN
                        V_ERROR_MESSAGE := 'NOT ABLE TO CALCULATE PER_DAY_ERROR_COUNT OR NOT ABLE TO INSERT DATA INTO PHM_APS_DATA FOR ' || Y.IOM_SN || ' FOR DATE '
                                           || Y.FLAG_DATE ||' FOR ' || VALGNAME || ', WITH ERROR :'|| SQLERRM;
                        PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG  (V_PROCESS_ID,V_ALG_NUM,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,VALGNAME);
                    END;

                  END LOOP;
                  COMMIT;
                  DBMS_OUTPUT.PUT_LINE(sysdate || ', inserted ' || V_INSERT_COUNT || ' records into PHM_APS_DATA table for the COUNT algorithm: ' || TH.PATTERN_NAME);  

             END IF;

             /*V_INSERT_COUNT := 0;
             FOR X IN CURAPS_DEVICES_DATES
             LOOP
                   FOR REM_DATES IN (SELECT DISTINCT TRUNC(DATE_ON_LOG_FILE) DT, SN, LFD.DEVICE_ID
                              FROM APS_LOG_FILE_DETAILS LFD, APS_LOG_FILES LF WHERE LF.ZIPID = LFD.ZIPID AND LF.DEVICE_ID = LFD.DEVICE_ID
                              AND LF.SN = X.SN AND TRUNC(DATE_ON_LOG_FILE) BETWEEN X.DT - TH.THRESHOLDS_DAYS AND X.DT_MAX)
                   LOOP
                      SELECT COUNT(*) INTO TH_DT_COUNT FROM PHM_APS_DATA WHERE PHM_PATTERNS_SK = TH.PHM_PATTERNS_SK AND IOM_SN = REM_DATES.SN
                                AND TRUNC(TIMESTAMP) = REM_DATES.DT AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE;
                      IF TH_DT_COUNT = 0 THEN
                         INSERT INTO SVC_PHM_OWNER.PHM_APS_DATA (PHM_ALGORITHM_DEFINITIONS_SK, PHM_PATTERNS_SK, BATCH_NUM, RUN_DATE, DEVICE_ID, IOM_SN, PL, SN
                               , TIMESTAMP, ALGORITHM_TYPE, TESTCOUNT, ERRORCOUNT, ERRORPCT, TIMESTAMP_MS, ADDED_BY, DATE_CREATED)
                            VALUES  (V_ALG_NUM, TH.PHM_PATTERNS_SK, V_BATCH_NUM, V_RUN_DATE, REM_DATES.DEVICE_ID, REM_DATES.SN, NULL, REM_DATES.SN
                               , TO_DATE(to_char(REM_DATES.DT,'YYYYMMDD') || '000001','YYYYMMDDHH24MISS'), TH.ALGORITHM_TYPE, 0, 0, 0, 0, VALGNAME || '-GEO', SYSDATE);                         
                         V_INSERT_COUNT := V_INSERT_COUNT + 1;
                         IF MOD(V_INSERT_COUNT,10000) = 0 THEN COMMIT; END IF;
                      END IF;

                   END LOOP;
             END LOOP;
             COMMIT;
             DBMS_OUTPUT.PUT_LINE(sysdate || ', inserted ' || V_INSERT_COUNT || ' records into PHM_APS_DATA table  with VALGNAME-GEO');*/
       END LOOP;

       FOR Z IN ALL_THRESHOLDS(V_ALG_NUM)
        LOOP

          IF Z.ALGORITHM_TYPE = 'BCR_1' THEN
             DBMS_OUTPUT.PUT_LINE('Processing the thresholds for BCR_1 algorithm ' || Z.PATTERN_TEXT || ', THRESHOLD_DESCRIPTION: ' || Z.THRESHOLD_DESCRIPTION);
             FOR Y IN CURAPS_DEVICES_DATES
              LOOP
                  FLAG := 'NO';
                  IHN_VALUE := NULL;
                  BEGIN
                  SELECT DISTINCT IL.PL, IL.CUSTOMER, IL.CUSTOMER_NUM, PC.AREAREGION, PC.COUNTRY 
                      INTO V_INST_PL, V_CUSTOMER, V_CUST_NUM, V_CITY, V_COUNTRY
                  FROM INSTRUMENTLISTING IL, PHM_APS_DATA PAT, PHM_COUNTRY PC WHERE PAT.SN = IL.SN
                      AND PAT.PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND  PC.COUNTRY_CODE = IL.COUNTRY_CODE
                      AND PAT.BATCH_NUM = V_BATCH_NUM AND PAT.RUN_DATE = V_RUN_DATE AND  PAT.SN = Y.SN
                      AND INST_STATUS = 'Active' AND CMS_STATUS = 'Active';
                  EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE(Y.SN || ', ERROR WHILE FETCHING DATA FOR BCR_1 algorithm:  ' || SQLERRM);
                        V_INST_PL := NULL; V_CUSTOMER := NULL; V_CUST_NUM := NULL; V_CITY := NULL; V_COUNTRY := NULL;
                  END;

                  IF V_CUST_NUM IS NOT NULL THEN
                    FOR D IN (SELECT * FROM PHM_APS_DATA WHERE SN = Y.SN AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK
                              AND TRUNC(TIMESTAMP) >= Y.DT AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE ORDER BY TIMESTAMP)
                    LOOP
                     BEGIN
                         TODAY_TEST_COUNT := 0;
                         ONE_SD_BELOW_MEAN_COUNT := 0;
                         V_30DAYS_MIN_DATE := NULL;
                         TODAY_ERRORPCT := 0;
                         PREV_DAY_ERRORPCT := 0;
                         SD15_ABOVE_30MEAN_ERRORPCT := 0;
                         FLAG := 'NO';
                         IHN_VALUE := NULL;
                         CONSEQ_COUNT := 0;
                         V_FLAGGED_PL := NULL;
                         V_FLAGGED_EXP_CODE := NULL;

                         -- Calculate 1.5 SD below 30 day mean 
                         SELECT TRUNC(ABS(AVG(TESTCOUNT) - STDDEV(TESTCOUNT)), 0), MIN(TIMESTAMP)  INTO ONE_SD_BELOW_MEAN_COUNT, V_30DAYS_MIN_DATE FROM 
                            (SELECT *  FROM PHM_APS_DATA WHERE SN = Y.SN AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND BATCH_NUM = V_BATCH_NUM
                                AND RUN_DATE = V_RUN_DATE AND TRUNC(TIMESTAMP) <= TRUNC(D.TIMESTAMP) ORDER BY TIMESTAMP DESC) 
                         WHERE ROWNUM <=  Z.THRESHOLD_DATA_DAYS;

                         -- Get current day's error count and error percentage
                         SELECT TESTCOUNT, NVL(ERRORPCT ,0) INTO TODAY_TEST_COUNT, TODAY_ERRORPCT FROM PHM_APS_DATA WHERE SN = Y.SN
                            AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE AND TRUNC(TIMESTAMP) = TRUNC(D.TIMESTAMP);

                         -- Exclude the days where IOM Volume (test count) is 1.5 SD below 30 day mean 
                         IF TODAY_TEST_COUNT >= ONE_SD_BELOW_MEAN_COUNT THEN
                           -- Calculate the percentage 1.5 SD above 30 day mean 
                           SELECT (STDDEV (ERRORPCT) * Z.ERROR_COUNT) + AVG (ERRORPCT) INTO SD15_ABOVE_30MEAN_ERRORPCT FROM PHM_APS_DATA T
                           WHERE SN = Y.SN AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE AND TESTCOUNT >= ONE_SD_BELOW_MEAN_COUNT 
                                    AND TRUNC(TIMESTAMP) BETWEEN V_30DAYS_MIN_DATE AND TRUNC(D.TIMESTAMP);
                           IF TODAY_ERRORPCT > SD15_ABOVE_30MEAN_ERRORPCT THEN
                               CONSEQ_COUNT := 0;
                               -- Check for error percentage match for consecutive days
                               FOR I IN (SELECT * FROM  (SELECT * FROM PHM_APS_DATA WHERE SN = Y.SN AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK
                                            AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE AND TESTCOUNT >= ONE_SD_BELOW_MEAN_COUNT AND TRUNC(TIMESTAMP) <= TRUNC(D.TIMESTAMP) 
                                          ORDER BY TIMESTAMP DESC) WHERE ROWNUM <=  Z.THRESHOLDS_DAYS ORDER BY TIMESTAMP DESC)
                               LOOP
                                    IF I.ERRORPCT >= SD15_ABOVE_30MEAN_ERRORPCT THEN
                                     CONSEQ_COUNT := CONSEQ_COUNT + 1;
                                    ELSE
                                     CONSEQ_COUNT := 0;
                                    END IF;
                               END LOOP;

                               IF CONSEQ_COUNT >= Z.THRESHOLDS_DAYS THEN
                                  FLAG := 'YES';
                                  -- IHN_VALUE :=   Z.THRESHOLD_ALERT;
                                  IHN_VALUE :=   Z.ISSUE_DESCRIPTION;
                                  PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Z.PHM_PATTERNS_SK, V_INST_PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                                ELSE
                                  FLAG := 'NO';
                                  IHN_VALUE := NULL;
                               END IF;

                           ELSE
                                FLAG := 'NO';
                                IHN_VALUE := NULL;
                           END IF;

                         ELSE
                            FLAG := 'NO';
                            IHN_VALUE := NULL;
                         END IF;
                     EXCEPTION
                       WHEN OTHERS THEN
                         VSQLERRORMSG :=  ' CALCULATION OF SD OF ERRORS HAS FAILED FOR PHM_PATTERNS_SK: ' || Z.PHM_PATTERNS_SK || ' FOR ' || Y.SN || ' FOR DATE ' || D.TIMESTAMP || ', ERROR :' || SQLERRM;
                         TODAY_ERRORPCT := 0;
                         FLAG := 'NO';
                         IHN_VALUE := NULL;
                         DBMS_OUTPUT.PUT_LINE(VSQLERRORMSG);
                         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID, V_ALG_NUM, V_RUN_DATE, V_BATCH_NUM, VSQLERRORMSG, VALGNAME);
                     END;

                     DBMS_OUTPUT.PUT_LINE('SN: ' || Y.SN || ', D.TIMESTAMP: ' || D.TIMESTAMP || ', ONE_SD_BELOW_MEAN_COUNT: ' ||  ONE_SD_BELOW_MEAN_COUNT || ', V_30DAYS_MIN_DATE: ' || V_30DAYS_MIN_DATE 
                            || ', TODAY_TEST_COUNT: ' || TODAY_TEST_COUNT || ', TODAY_ERRORPCT: ' || TODAY_ERRORPCT || ', SD15_ABOVE_30MEAN_ERRORPCT: ' || SD15_ABOVE_30MEAN_ERRORPCT 
                            || ', CONSEQ_COUNT: ' || CONSEQ_COUNT || ', Z.THRESHOLDS_DAYS: ' || Z.THRESHOLDS_DAYS || ', FLAG: ' || FLAG 
                            || ', IHN_VALUE: ' || IHN_VALUE || ', V_FLAGGED_PL: ' || V_FLAGGED_PL || ', V_FLAGGED_EXP_CODE: ' || V_FLAGGED_EXP_CODE);  
                     
                     PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(V_CUSTOMER, V_CUST_NUM, D.DEVICE_ID, Y.SN, V_COUNTRY, V_CITY, V_ALG_NUM
                            , Z.PHM_THRESHOLDS_SK, D.TIMESTAMP, TODAY_ERRORPCT, FLAG, IHN_VALUE, NULL, VALGNAME, V_PROD_FAMILY, V_BATCH_NUM, Z.PHM_PATTERNS_SK, V_RUN_DATE,V_PROCESS_ID, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                            
                     V_INSERT_COUNT := V_INSERT_COUNT + 1;
                     IF MOD(V_INSERT_COUNT, 10000) = 0 THEN COMMIT; END IF;
                    END LOOP;
                  END IF;
              END LOOP;
              COMMIT; 
          END IF;
          IF Z.ALGORITHM_TYPE = 'BCR_2/3' THEN
            DBMS_OUTPUT.PUT_LINE('Processing the thresholds for BCR_2/3 algorithm ' || Z.PATTERN_TEXT || ', THRESHOLD_DESCRIPTION: ' || Z.THRESHOLD_DESCRIPTION); 
            FOR Y IN (SELECT DISTINCT IL.PL, PAT.SN, PAT.DEVICE_ID, IL.CUSTOMER, IL.CUSTOMER_NUM, PC.AREAREGION, PC.COUNTRY
                            FROM INSTRUMENTLISTING IL, PHM_APS_DATA PAT, PHM_COUNTRY PC WHERE PAT.SN = IL.SN
                            AND   PAT.PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND  PC.COUNTRY_CODE = IL.COUNTRY_CODE
                            AND  PAT.BATCH_NUM = V_BATCH_NUM AND PAT.RUN_DATE = V_RUN_DATE
                            AND INST_STATUS = 'Active' AND CMS_STATUS = 'Active')
            LOOP

              CURR_DAY := NULL;
              PREV_DAY := NULL;
              CONSECUTIVE_DAYS := TRUE;
              TODAY_ERRORPCT := 0; 
              FLAGGING_DAYS := 0;
              FLAG := 'NO';
              IHN_VALUE := NULL;
              V_INSERT_COUNT := 0;

              FOR D IN  (SELECT * FROM PHM_APS_DATA WHERE  SN = Y.SN AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE ORDER BY TIMESTAMP)
              LOOP
                BEGIN
                  FLAG := 'NO';
                  IHN_VALUE := NULL;
                  V_FLAGGED_PL := NULL;
                  V_FLAGGED_EXP_CODE := NULL;  
                  -- Consecutive days scenario Ex: 10 errors/day for 2 consecutive days; 2 errors/day for 3 consecutive days
                  IF (Z.THRESHOLD_TYPE = 'CONSECUTIVE') THEN
                      CURR_DAY := TRUNC(D.TIMESTAMP);
                      TODAY_ERRORPCT := D.ERRORPCT;
                      -- check if next date in the cursor is one less then the previous day? - means consecutive or not
                      IF (PREV_DAY IS NOT NULL AND CURR_DAY <> (PREV_DAY + 1)) THEN
                        CONSECUTIVE_DAYS := FALSE;
                      END IF;
                      
                      IF (CONSECUTIVE_DAYS = TRUE AND (TODAY_ERRORPCT >= Z.ERROR_COUNT)) THEN
                        FLAGGING_DAYS := FLAGGING_DAYS + 1;
                      ELSE
                        FLAGGING_DAYS := 0;                        
                      END IF;  
                      
                      PREV_DAY := CURR_DAY;

                      IF (FLAGGING_DAYS >= Z.THRESHOLDS_DAYS) THEN
                         FLAG := 'YES';
                         IHN_VALUE :=  Z.ISSUE_DESCRIPTION;  --Z.THRESHOLD_ALERT;
                         PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Z.PHM_PATTERNS_SK, Y.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                      END IF;
                  END IF;                       
                EXCEPTION
                 WHEN OTHERS THEN
                    FLAG := 'NO'; IHN_VALUE := NULL; TODAY_ERRORPCT:= 0;
                    VSQLERRORMSG :=  ' PER CENT OF COUNT OF ERRORS HAS FAILED FOR PHM_PATTERNS_SK ' || Z.PHM_PATTERNS_SK || ' FOR '|| Y.SN ||' FOR DATE '|| D.TIMESTAMP || ', ERROR :' || SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG  (V_PROCESS_ID,V_ALG_NUM,V_RUN_DATE,V_BATCH_NUM,VSQLERRORMSG,VALGNAME);
                END;
                DBMS_OUTPUT.PUT_LINE('THRESHOLD_TYPE: ' || Z.THRESHOLD_TYPE || ', SN: ' || Y.SN || ', TIMESTAMP: ' ||  D.TIMESTAMP || ', CURR_DAY: ' || CURR_DAY || ', PREV_DAY: ' || PREV_DAY 
                          || ', CONSECUTIVE_DAYS: ' || (CASE WHEN (CONSECUTIVE_DAYS = TRUE) THEN 'TRUE'  ELSE 'FALSE' END) || ', TODAY_ERRORPCT: ' || TODAY_ERRORPCT || ', FLAG: ' || FLAG 
                          || ', IHN_VALUE: ' || IHN_VALUE || ', V_FLAGGED_PL: ' || V_FLAGGED_PL || ', V_FLAGGED_EXP_CODE: ' || V_FLAGGED_EXP_CODE);  
                PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(Y.CUSTOMER, Y.CUSTOMER_NUM, Y.DEVICE_ID, Y.SN, Y.COUNTRY, Y.AREAREGION, V_ALG_NUM
                         , Z.PHM_THRESHOLDS_SK, D.TIMESTAMP, TODAY_ERRORPCT, FLAG, IHN_VALUE, NULL, VALGNAME, V_PROD_FAMILY, V_BATCH_NUM, Z.PHM_PATTERNS_SK, V_RUN_DATE, V_PROCESS_ID, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                         
                V_INSERT_COUNT := V_INSERT_COUNT + 1;
                IF MOD(V_INSERT_COUNT, 10000) = 0 THEN COMMIT; END IF;
              END LOOP;
            END LOOP;
            COMMIT;
          END IF;

          IF Z.ALGORITHM_TYPE = 'LAS_205' THEN
            DBMS_OUTPUT.PUT_LINE('Processing the thresholds for LAS_205 algorithm: ' || Z.PATTERN_NAME || ', THRESHOLD_DESCRIPTION: ' || Z.THRESHOLD_DESCRIPTION);   
            -- Get all instruments for the BATCH_NUM and RUN_DATE and PHM_PATTERNS_SK (LAS1: Error 205 etc.,)
            FOR Y IN (SELECT DISTINCT IL.PL, IL.SN, PAT.DEVICE_ID, IL.CUSTOMER, IL.CUSTOMER_NUM, PC.AREAREGION, PC.COUNTRY
                        FROM INSTRUMENTLISTING IL,PHM_APS_DATA PAT, PHM_COUNTRY PC WHERE PAT.SN = IL.SN
                            AND PAT.PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND  PC.COUNTRY_CODE = IL.COUNTRY_CODE
                            AND PAT.BATCH_NUM = V_BATCH_NUM AND PAT.RUN_DATE = V_RUN_DATE
                            AND INST_STATUS = 'Active' AND CMS_STATUS = 'Active')

            LOOP
               CURR_DAY := NULL;
               PREV_DAY := NULL;
               CONSECUTIVE_DAYS := TRUE;
               TODAY_ERRORPCT := 0; 
               FLAGGING_DAYS := 0;
               FLAG := 'NO';
               IHN_VALUE := NULL;
               V_INSERT_COUNT := 0;
               
               FOR X IN (SELECT * FROM PHM_APS_DATA WHERE SN = Y.SN AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE ORDER BY TIMESTAMP)
               LOOP
                BEGIN
                  FLAG := 'NO';
                  IHN_VALUE := NULL;
                  V_FLAGGED_PL := NULL;
                  V_FLAGGED_EXP_CODE := NULL;  
                  -- Consecutive days scenario Ex: 10 errors/day for 2 consecutive days; 2 errors/day for 3 consecutive days
                  IF (Z.THRESHOLD_TYPE = 'CONSECUTIVE') THEN
                      CURR_DAY := TRUNC(X.TIMESTAMP);
                      TODAY_ERRORPCT := X.ERRORPCT;
                      -- check if next date in the cursor is one less then the previous day? - means consecutive or not
                      IF (PREV_DAY IS NOT NULL AND CURR_DAY <> (PREV_DAY + 1)) THEN
                        CONSECUTIVE_DAYS := FALSE;
                      END IF;
                      
                      IF (CONSECUTIVE_DAYS = TRUE AND (TODAY_ERRORPCT >= Z.ERROR_COUNT)) THEN
                        FLAGGING_DAYS := FLAGGING_DAYS + 1;
                      ELSE
                        FLAGGING_DAYS := 0;                        
                      END IF;  
                      
                      PREV_DAY := CURR_DAY;

                      IF (FLAGGING_DAYS >= Z.THRESHOLDS_DAYS) THEN
                         FLAG := 'YES';
                         IHN_VALUE :=  Z.ISSUE_DESCRIPTION;  --Z.THRESHOLD_ALERT;
                         PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Z.PHM_PATTERNS_SK, Y.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                      END IF;
                  END IF;  
                  
                EXCEPTION WHEN OTHERS THEN
                   FLAG := 'NO'; IHN_VALUE := NULL; TODAY_ERRORPCT:= 0; 
                   VSQLERRORMSG :=  ' CALCULATION OF LAS_205 ERRORS HAS FAILED FOR ' || Z.PHM_PATTERNS_SK || ' FOR ' || Y.SN || ' FOR DATE '|| X.TIMESTAMP || ', ERROR :'|| SQLERRM;
                   DBMS_OUTPUT.PUT_LINE(SQLERRM);
                   PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID,V_ALG_NUM,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,VALGNAME);
                END;
               DBMS_OUTPUT.PUT_LINE('THRESHOLD_TYPE: ' || Z.THRESHOLD_TYPE || ', SN: ' || Y.SN || ', TIMESTAMP: ' ||  X.TIMESTAMP || ', CURR_DAY: ' || CURR_DAY || ', PREV_DAY: ' || PREV_DAY 
                         || ', CONSECUTIVE_DAYS: ' || (CASE WHEN (CONSECUTIVE_DAYS = TRUE) THEN 'TRUE'  ELSE 'FALSE' END) || ', TODAY_ERRORPCT: ' || TODAY_ERRORPCT || ', FLAG: ' || FLAG 
                         || ', IHN_VALUE: ' || IHN_VALUE || ', V_FLAGGED_PL: ' || V_FLAGGED_PL || ', V_FLAGGED_EXP_CODE: ' || V_FLAGGED_EXP_CODE);  
               PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(Y.CUSTOMER, Y.CUSTOMER_NUM, Y.DEVICE_ID, Y.SN, Y.COUNTRY, Y.AREAREGION, V_ALG_NUM
                        , Z.PHM_THRESHOLDS_SK, X.TIMESTAMP, TODAY_ERRORPCT, FLAG, IHN_VALUE, NULL, VALGNAME, V_PROD_FAMILY, V_BATCH_NUM, Z.PHM_PATTERNS_SK, V_RUN_DATE, V_PROCESS_ID, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);

               V_INSERT_COUNT := V_INSERT_COUNT + 1;
               IF MOD(V_INSERT_COUNT, 10000) = 0 THEN COMMIT; END IF;
             END LOOP;
            END LOOP;
            COMMIT;
          END IF;

          IF Z.ALGORITHM_TYPE = 'COUNT' THEN
             DBMS_OUTPUT.PUT_LINE('Processing the thresholds for COUNT algorithm ' || Z.PATTERN_TEXT || ', THRESHOLD_DESCRIPTION: ' || Z.THRESHOLD_DESCRIPTION);   
             FOR Y IN (SELECT DISTINCT IL.PL, IL.SN, PAT.DEVICE_ID, IL.CUSTOMER, IL.CUSTOMER_NUM, PC.AREAREGION, PC.COUNTRY
                           FROM INSTRUMENTLISTING IL, PHM_APS_DATA PAT, PHM_COUNTRY PC WHERE PAT.PL = IL.PL and PAT.SN = IL.SN
                           AND PAT.PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND  PC.COUNTRY_CODE = IL.COUNTRY_CODE
                           AND PAT.BATCH_NUM = V_BATCH_NUM AND PAT.RUN_DATE = V_RUN_DATE AND INST_STATUS = 'Active' AND CMS_STATUS = 'Active')
            LOOP
               CURR_DAY := NULL;
               PREV_DAY := NULL;
               CONSECUTIVE_DAYS := TRUE;
               CURR_DAY_ERRCOUNT := 0; 
               FLAGGING_DAYS := 0;
               FLAG := 'NO';
               IHN_VALUE := NULL;
               V_INSERT_COUNT := 0;
               TOTAL_ERROR_COUNT := 0;
                
               FOR X IN (SELECT * FROM PHM_APS_DATA WHERE SN = Y.SN AND PHM_PATTERNS_SK = Z.PHM_PATTERNS_SK AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE ORDER BY TIMESTAMP)
               LOOP
                BEGIN
                  FLAG := 'NO';
                  IHN_VALUE := NULL;
                  V_FLAGGED_PL := NULL;
                  V_FLAGGED_EXP_CODE := NULL;                  
                  -- Consecutive days scenario Ex: 10 errors/day for 2 consecutive days; 2 errors/day for 3 consecutive days
                  IF (Z.THRESHOLD_TYPE = 'CONSECUTIVE') THEN
                      CURR_DAY := TRUNC(X.TIMESTAMP);
                      CURR_DAY_ERRCOUNT := X.ERRORCOUNT;
                      -- check if next date in the cursor is one less then the previous day? - means consecutive or not
                      IF (PREV_DAY IS NOT NULL AND CURR_DAY <> (PREV_DAY + 1)) THEN
                        CONSECUTIVE_DAYS := FALSE;
                      END IF;
                      
                      IF (CONSECUTIVE_DAYS = TRUE AND (CURR_DAY_ERRCOUNT >= Z.ERROR_COUNT)) THEN
                        FLAGGING_DAYS := FLAGGING_DAYS + 1;
                      ELSE
                        FLAGGING_DAYS := 0;                        
                      END IF;  
                      
                      PREV_DAY := CURR_DAY;

                      IF (FLAGGING_DAYS >= Z.THRESHOLDS_DAYS) THEN
                         FLAG := 'YES';
                         IHN_VALUE :=  Z.ISSUE_DESCRIPTION;  --Z.THRESHOLD_ALERT;
                         PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Z.PHM_PATTERNS_SK, Y.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                      END IF;
                      DBMS_OUTPUT.PUT_LINE('THRESHOLD_TYPE: ' || Z.THRESHOLD_TYPE || ', SN: ' || Y.SN || ', TIMESTAMP: ' ||  X.TIMESTAMP || ', CURR_DAY: ' || CURR_DAY || ', PREV_DAY: ' || PREV_DAY 
                                || ', CONSECUTIVE_DAYS: ' || (CASE WHEN (CONSECUTIVE_DAYS = TRUE) THEN 'TRUE'  ELSE 'FALSE' END) || ', ERROR_COUNT: ' || CURR_DAY_ERRCOUNT || ', FLAG: ' || FLAG 
                                || ', IHN_VALUE: ' || IHN_VALUE || ', V_FLAGGED_PL: ' || V_FLAGGED_PL || ', V_FLAGGED_EXP_CODE: ' || V_FLAGGED_EXP_CODE);  
                  -- count based scenario Ex: 3 errors in a week, 1 error per day
                  ELSIF (Z.THRESHOLD_TYPE = 'COUNT') THEN
                      CURR_DAY_ERRCOUNT := X.ERRORCOUNT;
                      TOTAL_ERROR_COUNT := TOTAL_ERROR_COUNT + CURR_DAY_ERRCOUNT;
                      IF (TOTAL_ERROR_COUNT >= Z.ERROR_COUNT) THEN
                         FLAG := 'YES';
                         IHN_VALUE :=  Z.ISSUE_DESCRIPTION;  --Z.THRESHOLD_ALERT;
                         PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Z.PHM_PATTERNS_SK, Y.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                      END IF;
                      DBMS_OUTPUT.PUT_LINE('THRESHOLD_TYPE: ' || Z.THRESHOLD_TYPE || ', SN: ' || Y.SN || ', TIMESTAMP: ' ||  X.TIMESTAMP || ', X.ERRORCOUNT: ' || X.ERRORCOUNT || ', TOTAL_ERROR_COUNT: ' || TOTAL_ERROR_COUNT 
                                || ', FLAG: ' || FLAG || ', IHN_VALUE: ' || IHN_VALUE || ', V_FLAGGED_PL: ' || V_FLAGGED_PL || ', V_FLAGGED_EXP_CODE: ' || V_FLAGGED_EXP_CODE);  

                  -- discreate days count scenario Ex: 1/day 3 days with errors in one week
                  ELSIF (Z.THRESHOLD_TYPE = 'DISCRETE') THEN
                      CURR_DAY_ERRCOUNT := X.ERRORCOUNT;
                      IF (CURR_DAY_ERRCOUNT >= Z.ERROR_COUNT) THEN
                        FLAGGING_DAYS := FLAGGING_DAYS + 1;
                      END IF;
                        
                      IF (FLAGGING_DAYS >= Z.THRESHOLD_DATA_DAYS) THEN
                         FLAG := 'YES';
                         IHN_VALUE :=  Z.ISSUE_DESCRIPTION;  --Z.THRESHOLD_ALERT;
                         PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Z.PHM_PATTERNS_SK, Y.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                      END IF;
                      DBMS_OUTPUT.PUT_LINE('THRESHOLD_TYPE: ' || Z.THRESHOLD_TYPE || ', SN: ' || Y.SN || ', TIMESTAMP: ' ||  X.TIMESTAMP || ', CURR_DAY_ERRCOUNT: ' || CURR_DAY_ERRCOUNT || ', FLAGGING_DAYS: ' || FLAGGING_DAYS 
                                || ', FLAG: ' || FLAG || ', IHN_VALUE: ' || IHN_VALUE || ', V_FLAGGED_PL: ' || V_FLAGGED_PL || ', V_FLAGGED_EXP_CODE: ' || V_FLAGGED_EXP_CODE);  
                      
                  END IF;  
                EXCEPTION WHEN OTHERS THEN
                   CURR_DAY_ERRCOUNT := 0; FLAG := 'NO'; IHN_VALUE := NULL; V_FLAGGED_PL := NULL; V_FLAGGED_EXP_CODE := NULL;
                   VSQLERRORMSG :=  ' CALCULATION OF COUNT OF ERRORS HAS FAILED FOR PHM_PATTERNS_SK: ' || Z.PHM_PATTERNS_SK || ' FOR ' || Y.SN || ' FOR DATE ' || X.TIMESTAMP || ', ERROR :'|| SQLERRM;
                   DBMS_OUTPUT.PUT_LINE(SQLERRM);
                   PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG(V_PROCESS_ID,V_ALG_NUM,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,VALGNAME);
                END;
                PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(Y.CUSTOMER, Y.CUSTOMER_NUM, Y.DEVICE_ID, Y.SN, Y.COUNTRY, Y.AREAREGION, V_ALG_NUM
                           , Z.PHM_THRESHOLDS_SK, X.TIMESTAMP, CURR_DAY_ERRCOUNT, FLAG, IHN_VALUE, NULL, VALGNAME, V_PROD_FAMILY, V_BATCH_NUM, Z.PHM_PATTERNS_SK, V_RUN_DATE, V_PROCESS_ID, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                               
                V_INSERT_COUNT := V_INSERT_COUNT + 1;
                IF MOD(V_INSERT_COUNT, 10000) = 0 THEN COMMIT; END IF;
               END LOOP;
            END LOOP;
            COMMIT;
          END IF;
          
       END LOOP;  -- END of loop for Thresholds
    END IF;

  ELSE
      V_ERROR_MESSAGE := 'NOT ABLE TO GET THE ALGORITHM DETAILS DUE TO '||SQLERRM;
      V_PROCESS_STATUS := 'ERRORED';
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,'',V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID ,V_ALG_NUM);
  END IF;

  IF V_PROCESS_STATUS NOT IN ( 'ERRORED') THEN
     V_PROCESS_STATUS := 'COMPLETED';
     V_ERROR_MESSAGE  := '';
     PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,'',V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID ,V_ALG_NUM);
  END IF;

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('000 '||SQLERRM);
     V_ERROR_MESSAGE := 'NOT ABLE TO EXECUTE THE ALGORITHM DETAILS';
     PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,'',V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID ,V_ALG_NUM);
END PHM_APS;
/