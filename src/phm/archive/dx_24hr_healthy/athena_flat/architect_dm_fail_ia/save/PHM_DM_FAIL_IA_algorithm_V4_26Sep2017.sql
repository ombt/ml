CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_DM_FAIL_IA_algorithm(V_ALG_NUM NUMBER,V_RUN_DATE DATE,V_BATCH_NUM VARCHAR2, V_UNIX_ID VARCHAR2)
IS

--  AUDIT LOG VARIABLE   

V_PROCESS_TYPE  VARCHAR2(25);
V_PROCESS_STATUS      VARCHAR2(25):= 'STARTED';
V_PROCESS_ID NUMBER(15);
V_PROD_FAMILY  VARCHAR2(25);
V_RUN_MODE VARCHAR2(10);
V_ROUTINE_NAME VARCHAR(35);
V_ROUTINE_TYPE VARCHAR(35);
V_ERROR_MESSAGE       VARCHAR(4000);


-- ALGORITHM PARAMETER VARAIBLES TO HANDLE THE PROCESS FLOW 


-- ALGORITHM LOCAL VARAIBLES TO HANDLE THE PROCESS FLOW 

V_EXISTING_REC_CNT  NUMBER;
V_FLAG              VARCHAR(5);
V_REC_COUNT         NUMBER := 0;
V_REC_INS_COUNT     NUMBER := 0;
V_FLAG_DATE_TIME    DATE;
V_RES_COUNT         NUMBER;
V_FLAG_COUNT        NUMBER;
VALGNAME            VARCHAR(25);
V_ALG_DFN_SK        NUMBER;
V_IHN_LEVEL3_DESC  VARCHAR2(200);
V_IHN_LEVEL3_DESC_VAL  VARCHAR2(200);
V_FLAGGED_PL       VARCHAR2(10);
V_FLAGGED_EXP_CODE VARCHAR2(10);

--Variables to hold values
V_N_DAYS_WITH_FAIL  NUMBER; --number of unique days with a failure event
V_N_PATIENT_SINCE_FAIL NUMBER; --number of patient tests run since the first falure event
V_N_DAYS_WITH_PATIENT NUMBER; --number of unique days with patient samples reported
V_N_DAYS_SINCE_LAST_PAT NUMBER; --number of days since the last patient sample reported (under failure condition)

--The threshold parameters
T_N_DAYS_WITH_FAIL  NUMBER; --number of unique days with a failure event
T_N_PATIENT_SINCE_FAIL NUMBER; --number of patient tests run since the first falure event
T_N_DAYS_WITH_PATIENT NUMBER; --number of unique days with patient samples reported
T_N_DAYS_SINCE_LAST_PAT NUMBER; --number of days since the last patient sample reported (under failure condition)

--Create a table to store FLAG_LIST in -- use BULK COLLECT so operation only has to be carried out once (instead of cursor loop)
TYPE TAB IS RECORD (
DEVICEID VARCHAR(10),
MODULESNDRM VARCHAR(10),
RESULT_TR VARCHAR(10),
FIRST_FAIL DATE,
LAST_FAIL DATE,
N_FAIL NUMBER,
N_DAYS_WITH_FAIL NUMBER,
N_DAYS_SINCE_FIRST_FAIL NUMBER);

TYPE TBL IS TABLE OF TAB;

FLG_TBL TBL;


    
    

-- Cursor to identify all instruments available in IDA during batch (taken from PHM_ODS_RESULTS_CC)
CURSOR DEVICE_SN_LIST
IS
    SELECT
          IA.DEVICEID,
          UPPER(IA.MODULESNDRM) SERIAL_NUM,
          MAX (IL.PL) PL,
          MAX (IL.CUSTOMER_NUM) CUSTOMER_NUMBER,
          MAX (IL.CUSTOMER) CUSTOMER_NAME,
          MAX (PC.COUNTRY) COUNTRY_NAME,
          MAX (PC.AREAREGION) AREA,
          MAX (IL.CITY) CITY,
          MAX (COMPLETIONDATE) MAX_COMPLETION_DATE,
          COUNT (*) DEVICE_SN_CNT
    FROM 
          SVC_PHM_ODS.PHM_ODS_RESULTS_IA IA,
          INSTRUMENTLISTING IL,
          PHM_COUNTRY PC
    WHERE 
          IA.BATCH_NUM = V_BATCH_NUM AND 
          IA.RUN_DATE = V_RUN_DATE AND 
          UPPER (IA.MODULESNDRM) = UPPER (IL.SN) AND
          PC.COUNTRY_CODE = IL.COUNTRY_CODE
    GROUP BY IA.DEVICEID, IA.MODULESNDRM;   
                          
                           
BEGIN

                    SELECT *
                    BULK COLLECT INTO FLG_TBL FROM
                      (SELECT
                      FINALDAT.DEVICEID,
                      FINALDAT.MODULESNDRM,
                      FINALDAT.RESULT_TR,
                      FINALDAT.MINDATE AS FIRST_FAIL,
                      FINALDAT.MAXDATE AS LAST_FAIL,
                      FINALDAT.CNT AS N_FAIL,
                      FINALDAT.N_DAYS_WITH_FAIL,
                      TRUNC(SYSDATE,'DDD') - TRUNC(FINALDAT.MINDATE,'DDD') AS N_DAYS_SINCE_FIRST_FAIL
                    FROM
                      (SELECT
                        DAT.DEVICEID,
                        DAT.MODULESNDRM,
                        DAT.RESULT_TR,
                        DAT.MINDATE,
                        DAT.MAXDATE,
                        DAT.CNT,
                        DAT.N_DAYS_WITH_FAIL,
                        ROW_NUMBER() OVER (PARTITION BY DAT.MODULESNDRM ORDER BY DAT.MAXDATE DESC) AS RN
                      FROM
                        (SELECT
                          RESULTS.DEVICEID,
                          RESULTS.MODULESNDRM,
                          RESULTS.RESULT_TR,
                          MIN(RESULTS.COMPLETIONDATE) AS MINDATE,
                          MAX(RESULTS.COMPLETIONDATE) AS MAXDATE,
                          COUNT(DISTINCT(TRUNC(RESULTS.COMPLETIONDATE,'DDD'))) AS N_DAYS_WITH_FAIL,
                          COUNT(*) AS CNT
                        FROM
                          (SELECT
                            M.*,
                            ROW_NUMBER() OVER (ORDER BY M.MODULESNDRM,M.COMPLETIONDATE DESC) AS A,
                            ROW_NUMBER() OVER (PARTITION BY M.RESULT_TR ORDER BY M.MODULESNDRM,M.COMPLETIONDATE DESC) AS B,
                            (ROW_NUMBER() OVER (ORDER BY M.MODULESNDRM,M.COMPLETIONDATE DESC) - 
                            ROW_NUMBER() OVER (PARTITION BY M.RESULT_TR ORDER BY M.MODULESNDRM,M.COMPLETIONDATE DESC)) AS DI
                           FROM
                            (SELECT 
                              *
                              FROM
                              SVC_PHM_ODS.PHM_ODS_DM_FAIL_IA MA
                     
                            ORDER BY MA.MODULESNDRM,MA.COMPLETIONDATE DESC) M

                          ORDER BY M.MODULESNDRM,M.COMPLETIONDATE DESC
                          ) RESULTS

                        GROUP BY
                          RESULTS.DEVICEID,
                          RESULTS.MODULESNDRM,
                          RESULTS.RESULT_TR,
                          RESULTS.DI

                        ORDER BY RESULTS.MODULESNDRM) DAT) FINALDAT

                      WHERE FINALDAT.RN = 1 AND FINALDAT.RESULT_TR = 'Failed' AND FINALDAT.N_DAYS_WITH_FAIL > 1);
                      
  -- STEP 1   :PURPOSE TO GET PROCESSID OF CURRENT EXECUTION
  
   V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
   V_PROCESS_STATUS := 'STARTED';
   
   --DBMS_OUTPUT.PUT_LINE('DM_FAIL_IA_algorithm EXECUTION STARTED FOR : V_BATCH_NUM: ' || V_BATCH_NUM || ', V_RUN_DATE: ' || V_RUN_DATE);

  -- STEP 2  :   PURPOSE TO GET THE REQUIRED ALGORITHM INFORMATION FROM CONFIGURATION TABLES
 
  SELECT AR.ROUTINE_NAME, AR.ROUTINE_TYPE,AR.RUN_MODE,AR.ROUTINE_INVOKE_COMMAND,PF.PRODUCT_FAMILY_NAME
  INTO VALGNAME,V_PROCESS_TYPE,V_RUN_MODE ,V_ROUTINE_NAME,V_PROD_FAMILY
  FROM PHM_ALGORITHM_ROUTINES AR,PHM_PATTERNS PP , PHM_PRODUCT_FAMILY PF 
  WHERE AR.PHM_PATTERNS_SK = V_ALG_NUM   AND PP.PHM_PATTERNS_SK = AR.PHM_PATTERNS_SK
  AND PP.PHM_PROD_FAMILY_SK = PF.PHM_PROD_FAMILY_SK;
  
  --DBMS_OUTPUT.PUT_LINE('VALGNAME: ' || VALGNAME || ', V_PROCESS_TYPE: ' || V_PROCESS_TYPE || ', V_RUN_MODE: ' || V_RUN_MODE || ', V_ROUTINE_NAME: ' || V_ROUTINE_NAME || ', V_PROD_FAMILY: ' || V_PROD_FAMILY);
  
  
  -- GET ALGORITHM_DEFINITION_SK
  SELECT PP.PHM_ALGORITHM_DEFINITIONS_SK INTO V_ALG_DFN_SK from PHM_PATTERNS PP, PHM_ALGORITHM_DEFINITIONS PAD 
    WHERE PP.PHM_ALGORITHM_DEFINITIONS_SK =  PAD.PHM_ALGORITHM_DEFINITIONS_SK and PP.PHM_PATTERNS_SK = V_ALG_NUM
    AND ALGORITHM_NAME IN (SELECT ROUTINE_NAME FROM PHM_ALGORITHM_ROUTINES WHERE PHM_PATTERNS_SK = V_ALG_NUM);
  
  --DBMS_OUTPUT.PUT_LINE('V_ALG_DFN_SK: ' || V_ALG_DFN_SK);
  
   -- Ex: 12941    ARCHITECT IA    ALG Oracle Procedure    Oracle Procedure    FEP    PHM_FE_PRESSURE    Batch    9/8/2016 10:20:36 PM        STARTED        9/8/2016    9/8/2016 10:20:36.000000 PM    BTH2200    NULL        1003
   PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID ,V_ALG_NUM);
   
   --DBMS_OUTPUT.PUT_LINE('V_PROCESS_ID: ' || V_PROCESS_ID || ', VALGNAME: ' || VALGNAME || ', V_ROUTINE_NAME: ' || V_ROUTINE_NAME);
   
  --  STEP 3 : PURPOSE - TO GET THE ALL THE PARAMETERS THAT WERE DEFINED IN THE ALGORITHM SCREEN
  
   FOR I IN  ( SELECT PARAMETER_VALUES,PARAMETER_NAME,PHM_PATTERNS_SK FROM PHM_THRESHOLD_PARAMETER WHERE PHM_PATTERNS_SK = V_ALG_NUM and NVL(DELETE_FLAG,'N') <> 'Y')
   LOOP
        
   -- PURPOSE - IN CASE OF NEW PERAMETER DEFEINED IN ALGORITHM DEFINITION -  WRITE CODE WITH A NEW IF CONDITION TO GET NEW PARAMETER VALUE 
     -- <CHANGE >
     --IF I.PARAMETER_NAME = 'THRESHOLDS_COUNT'                   THEN T_N_DAYS_WITH_FAIL             :=  I.PARAMETER_VALUES;  END IF;     
     IF I.PARAMETER_NAME = 'DM_FAIL_DAYS_WITH_FAIL'             THEN T_N_DAYS_WITH_FAIL             :=  I.PARAMETER_VALUES;  END IF;
     IF I.PARAMETER_NAME = 'DM_FAIL_N_PATIENT_SINCE_FAIL'       THEN T_N_PATIENT_SINCE_FAIL         :=  I.PARAMETER_VALUES;  END IF;
     IF I.PARAMETER_NAME = 'DM_FAIL_DAYS_WITH_PATIENT'          THEN T_N_DAYS_WITH_PATIENT          :=  I.PARAMETER_VALUES;  END IF;
     IF I.PARAMETER_NAME = 'DM_FAIL_DAYS_SINCE_LAST_PAT'        THEN T_N_DAYS_SINCE_LAST_PAT        :=  I.PARAMETER_VALUES;  END IF;
     IF I.PARAMETER_NAME = 'IHN_LEVEL3_DESC'                    THEN V_IHN_LEVEL3_DESC              :=  I.PARAMETER_VALUES;  END IF;
     
     
     
     -- < CHANGE>
   END LOOP;
   
   

  -- PURPOSE :  TO CONFIRM THE AVALIABILITY OF ODS  BASIC DETAILS  
  IF VALGNAME IS NOT NULL THEN
    
         -- STEP 5a : CHECK DATA EXISTS FOR BATCH AND RUN DATE IN THE ALGORITHM OUTPUT TABLE   , IF DATA EXISTS DELETE THE DATA FROM OUTPUT TABLE       
         SELECT COUNT(*) INTO V_EXISTING_REC_CNT FROM PHM_ALG_OUTPUT  WHERE  BATCH_NUM = V_BATCH_NUM 
            AND RUN_DATE = V_RUN_DATE AND PHM_PATTERNS_SK = V_ALG_NUM; --AND ROWNUM < 5;
            
         --DBMS_OUTPUT.PUT_LINE('EXISTING RECORD COUNT IN PHM_ALG_OUTPUT: ' || V_EXISTING_REC_CNT);          
         IF V_EXISTING_REC_CNT > 0 THEN
           DELETE FROM PHM_ALG_OUTPUT  WHERE  BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE AND PHM_PATTERNS_SK = V_ALG_NUM;
           COMMIT;
         END IF;
         --DBMS_OUTPUT.PUT_LINE('TOTAL RECORDS DELETED FROM PHM_ALG_OUTPUT: ' || V_EXISTING_REC_CNT);
         
             
         -- STEP 5b : CHECK DATA EXISTS FOR BATCH AND RUN DATE IN THE ALGORITHM CHART OUTPUT  TABLE   , IF DATA EXISTS DELETE THE DATA FROM OUTPUT TABLE       
         SELECT COUNT(*) INTO V_EXISTING_REC_CNT FROM PHM_ALG_CHART_OUTPUT  WHERE  BATCH_NUM = V_BATCH_NUM 
            AND RUN_DATE = V_RUN_DATE AND PHM_PATTERN_SK = V_ALG_NUM; --AND ROWNUM < 5;
            
         --DBMS_OUTPUT.PUT_LINE('EXISTING RECORD COUNT IN PHM_ALG_CHART_OUTPUT: ' || V_EXISTING_REC_CNT);          
         IF V_EXISTING_REC_CNT > 0 THEN
           DELETE FROM PHM_ALG_CHART_OUTPUT  WHERE  BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE AND PHM_PATTERN_SK = V_ALG_NUM;
           COMMIT;
         END IF;
         --DBMS_OUTPUT.PUT_LINE('TOTAL RECORDS DELETED FROM PHM_ALG_CHART_OUTPUT: ' || V_EXISTING_REC_CNT);
  
        -- STEP 6 : ALGORIOTHM SPECIFIC CODE - TO WRITE INTO COMMON OUTPUT TABLE>
        
        -- FOR EACH OF THE INSTRUMENTS COMING IN THE BATCH, IDENTIFY IF IT IS IN THE FLAGGED LIST,
        -- IF YES, it is flagged, otherwise it is unflagged
       
        V_FLAG_COUNT := 0;
        FOR DL IN  DEVICE_SN_LIST
        LOOP
            BEGIN
                V_FLAG := 'NO';
                V_IHN_LEVEL3_DESC_VAL := NULL;
                V_FLAG_DATE_TIME := V_RUN_DATE;
                V_RES_COUNT := 0;
                V_FLAGGED_PL := NULL;
                V_FLAGGED_EXP_CODE := NULL;

                FOR indx IN 1 .. FLG_TBL.COUNT
                LOOP
                    IF FLG_TBL (indx).MODULESNDRM = DL.SERIAL_NUM THEN
                        SELECT
                            COUNT(*) AS N_PATIENT_SINCE_FAIL,
                            COUNT(DISTINCT(TRUNC(IA.COMPLETIONDATE,'DDD'))) AS N_DAYS_WITH_PATIENT,
                            TRUNC(SYSDATE,'DDD') - TRUNC(MAX(IA.COMPLETIONDATE),'DDD') AS N_DAYS_SINCE_LAST_PAT
                            INTO V_N_PATIENT_SINCE_FAIL, V_N_DAYS_WITH_PATIENT, V_N_DAYS_SINCE_LAST_PAT
                        FROM
                            SVC_PHM_ODS.PHM_ODS_RESULTS_IA IA
                        WHERE
                            IA.MODULESNDRM = FLG_TBL (indx).MODULESNDRM AND
                            IA.SAMPLETYPE = 'PATIENT' AND
                            IA.COMPLETIONDATE >= FLG_TBL (indx).FIRST_FAIL;
                    
                        IF  V_N_PATIENT_SINCE_FAIL >= T_N_PATIENT_SINCE_FAIL AND 
                            V_N_DAYS_WITH_PATIENT >= T_N_DAYS_WITH_PATIENT AND 
                            V_N_DAYS_SINCE_LAST_PAT <= T_N_DAYS_SINCE_LAST_PAT
                        THEN
                            V_FLAG := 'YES';
                            V_IHN_LEVEL3_DESC_VAL := V_IHN_LEVEL3_DESC;
                            V_RES_COUNT := 1;
                            V_FLAG_COUNT := V_FLAG_COUNT + 1;
                            -- Get the PL and experience code for the flagged instrument
                            PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(V_ALG_NUM, DL.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                        END IF;
                    END IF;
                END LOOP;

            --  INSERT THE DATA INTO COMMON RESULT OUTPUT TABLE 
            PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(DL.CUSTOMER_NAME, DL.CUSTOMER_NUMBER, DL.DEVICEID
                , DL.SERIAL_NUM, DL.COUNTRY_NAME, DL.AREA, V_ALG_DFN_SK, -1, V_FLAG_DATE_TIME, V_RES_COUNT
                , V_FLAG, V_IHN_LEVEL3_DESC_VAL, NULL, VALGNAME, NULL, V_BATCH_NUM, V_ALG_NUM, V_RUN_DATE, V_PROCESS_ID, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
            
            --  INSERT THE DATA INTO COMMON CHART OUTPUT TABLE 
            PHM_ALGORITHM_UTILITIES_1.PHM_ALG_CHART_INSERT(DL.DEVICEID, DL.PL, DL.SERIAL_NUM, DL.COUNTRY_NAME, DL.AREA
                , V_ALG_NUM, NULL, NULL, V_FLAG_DATE_TIME, V_RES_COUNT, TO_CHAR(GET_MS_FROM_DATE(V_FLAG_DATE_TIME))
                , VALGNAME, SYSDATE, V_BATCH_NUM, V_RUN_DATE, V_ALG_DFN_SK);                
 
            V_REC_COUNT := V_REC_COUNT + 1;
            IF V_REC_COUNT > 5000
             THEN
               V_REC_COUNT := 0;
               COMMIT;
            END IF;
            V_REC_INS_COUNT := V_REC_INS_COUNT + 1;  -- DL%ROWCOUNT
                
            EXCEPTION
            --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS 
            WHEN OTHERS THEN
               V_ERROR_MESSAGE :=  ' PHM_DM_FAIL_IA_algorithm EXECUTION HAS FAILED FOR '||V_ALG_NUM||' FOR '||DL.SERIAL_NUM||' FOR DATE '||V_FLAG_DATE_TIME||
                                       ', ERROR :'|| SQLERRM;
               V_PROCESS_STATUS := 'ERRORED';
               PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID ,V_ALG_NUM);
               EXIT;
          END;
        END LOOP;
       -- </  CHANGE  >
       --DBMS_OUTPUT.PUT_LINE('PHM_DM_FAIL_CC_algorithm Execution COMPLETED Successfully. Total records inserted: ' || V_REC_INS_COUNT || ', Flagged Count: ' || V_FLAG_COUNT);
        
     -- STEP 7 PURPOSE - TO UPDATED THE PROCESS WITH COMPLETED STATUS IN THE AUDIT TABLES 
       V_PROCESS_STATUS := 'COMPLETED';
       V_ERROR_MESSAGE  := '';
       PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_ALG_NUM );
      COMMIT;

  ELSE 
       V_ERROR_MESSAGE := ' NOT ABLE FIND BASIC INFORMATION OF ALGORITHM '||V_ALG_NUM||' WITH ERROR ' || SQLERRM;
       V_PROCESS_STATUS := 'ERRORED';
       PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_ALG_NUM );
    
  END IF;
EXCEPTION
   --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS 
    WHEN OTHERS THEN
       V_PROCESS_STATUS := 'ERRORED';
       V_ERROR_MESSAGE  := 'ALGORITHM EXECUTION FAILED FOR DM_FAIL_IA_algorithm, DUE TO: ' || SQLERRM;
       PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_ALG_NUM );
       COMMIT;
END PHM_DM_FAIL_IA_algorithm;

grant execute on SVC_PHM_OWNER.PHM_DM_FAIL_IA_algorithm to SVC_PHM_CONNECT;