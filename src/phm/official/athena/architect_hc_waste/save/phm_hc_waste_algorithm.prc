CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_HC_WASTE_algorithm(V_ALG_NUM NUMBER,
                                                                 V_RUN_DATE DATE,
                                                                 V_BATCH_NUM VARCHAR2, 
                                                                 V_UNIX_ID VARCHAR2)
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
    V_IHN_LEVEL3_DESC  VARCHAR2(200);
    V_IHN_LEVEL3_DESC_VAL  VARCHAR2(200); 
    V_FLAGGED_PL       VARCHAR2(10);
    V_FLAGGED_EXP_CODE VARCHAR2(10);

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

    -- Cursor to identify flagged instruments. Written as a function
    CURSOR FLAG_LIST(V_MODULESNDRM VARCHAR2, 
                     V_DEVICEID NUMBER)
    IS
        SELECT
            W.MODULESNDRM AS MODULE_SN,
            W.DEVICEID AS DEVICE_ID
        FROM (
            SELECT
                HCW.MODULESNDRM,
                HCW.COMPLETIONDATE,
                HCW.DEVICEID,
                CASE WHEN HCW.READ29/NULLIF(HCW.READ30,0) - GREATEST(HCW.READ31/NULLIF(HCW.READ32,0), HCW.READ32/NULLIF(HCW.READ33,0))  < -0.03
                     THEN 1
                     ELSE 0
                     END AS SLOPE_DIFF_FLAG
                FROM 
                    SVC_PHM_ODS.PHM_ODS_HC_WASTE HCW
        ) W
        WHERE 
            W.MODULESNDRM = V_MODULESNDRM 
        AND 
            W.DEVICEID = V_DEVICEID 
        AND 
            W.COMPLETIONDATE >= SYSDATE - 1
        GROUP BY
            W.MODULESNDRM,
            W.DEVICEID
        HAVING
            COUNT(*) >= 10 
        AND
            AVG(SLOPE_DIFF_FLAG) >=0.5;

    -- Curstor to identify all instruments available in IDA during batch (taken from PHM_ODS_RESULTS_CC)
    CURSOR DEVICE_SN_LIST
    IS
        SELECT 
            CC.DEVICEID,
            UPPER(CC.MODULESNDRM) SERIAL_NUM,
            MAX (IL.PL) PL,
            MAX (IL.CUSTOMER_NUM) CUSTOMER_NUMBER,
            MAX (IL.CUSTOMER) CUSTOMER_NAME,
            MAX (PC.COUNTRY) COUNTRY_NAME,
            MAX (PC.AREAREGION) AREA,
            MAX (IL.CITY) CITY,
            MAX (COMPLETIONDATE) MAX_COMPLETION_DATE,
            COUNT (*) DEVICE_SN_CNT
        FROM 
            SVC_PHM_ODS.PHM_ODS_RESULTS_CC CC,
            INSTRUMENTLISTING IL,
            PHM_COUNTRY PC
        WHERE 
            CC.BATCH_NUM = V_BATCH_NUM 
        AND 
            CC.RUN_DATE = V_RUN_DATE 
        AND 
            UPPER(CC.MODULESNDRM) = UPPER (IL.SN) 
        AND
            PC.COUNTRY_CODE = IL.COUNTRY_CODE 
        and 
            IL.INST_STATUS='Active'
        GROUP BY 
            CC.DEVICEID, 
            CC.MODULESNDRM;   


    BEGIN

        -- STEP 1   :PURPOSE TO GET PROCESSID OF CURRENT EXECUTION

        V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
        V_PROCESS_STATUS := 'STARTED';

        -- STEP 2  :   PURPOSE TO GET THE REQUIRED ALGORITHM INFORMATION FROM CONFIGURATION TABLES

        SELECT 
            AR.ROUTINE_NAME, 
            AR.ROUTINE_TYPE,
            AR.RUN_MODE,
            AR.ROUTINE_INVOKE_COMMAND,
            PF.PRODUCT_FAMILY_NAME
        INTO 
            VALGNAME,
            V_PROCESS_TYPE,
            V_RUN_MODE ,
            V_ROUTINE_NAME,
            V_PROD_FAMILY
        FROM 
            PHM_ALGORITHM_ROUTINES AR,
            PHM_PATTERNS PP , 
            PHM_PRODUCT_FAMILY PF 
        WHERE 
            AR.PHM_PATTERNS_SK = V_ALG_NUM   
        AND 
            PP.PHM_PATTERNS_SK = AR.PHM_PATTERNS_SK
        AND 
            PP.PHM_PROD_FAMILY_SK = PF.PHM_PROD_FAMILY_SK;

        -- GET ALGORITHM_DEFINITION_SK
        SELECT 
            PP.PHM_ALGORITHM_DEFINITIONS_SK 
        INTO 
            V_ALG_DFN_SK 
        from 
            PHM_PATTERNS PP, 
            PHM_ALGORITHM_DEFINITIONS PAD 
        WHERE 
            PP.PHM_ALGORITHM_DEFINITIONS_SK =  PAD.PHM_ALGORITHM_DEFINITIONS_SK 
        and 
            PP.PHM_PATTERNS_SK = V_ALG_NUM
        AND 
            ALGORITHM_NAME IN (
                SELECT 
                    ROUTINE_NAME 
                FROM 
                    PHM_ALGORITHM_ROUTINES 
                WHERE 
                    PHM_PATTERNS_SK = V_ALG_NUM);

        -- Ex: 12941    ARCHITECT IA    ALG Oracle Procedure    Oracle Procedure    FEP    PHM_FE_PRESSURE    Batch    9/8/2016 10:20:36 PM        STARTED        9/8/2016    9/8/2016 10:20:36.000000 PM    BTH2200    NULL        1003
        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                        V_PROD_FAMILY,
                                                        V_PROCESS_TYPE,
                                                        V_ROUTINE_TYPE,
                                                        VALGNAME,
                                                        V_ROUTINE_NAME,
                                                        V_RUN_MODE,
                                                        V_PROCESS_STATUS,
                                                        V_ERROR_MESSAGE,
                                                        V_RUN_DATE ,
                                                        SYSDATE,
                                                        V_BATCH_NUM ,
                                                        V_UNIX_ID ,
                                                        V_ALG_NUM);

        --  STEP 3 : PURPOSE - TO GET THE ALL THE PARAMETERS THAT WERE DEFINED IN THE ALGORITHM SCREEN

        FOR I IN (
            SELECT 
                PARAMETER_VALUES,
                PARAMETER_NAME,
                PHM_PATTERNS_SK 
            FROM 
                PHM_THRESHOLD_PARAMETER 
            WHERE 
                PHM_PATTERNS_SK = V_ALG_NUM 
            and 
                NVL(DELETE_FLAG,'N') <> 'Y')
        LOOP
            IF I.PARAMETER_NAME = 'IHN_LEVEL3_DESC' 
            THEN 
                V_IHN_LEVEL3_DESC := I.PARAMETER_VALUES;  
            END IF;
        END LOOP;

        -- PURPOSE :  TO CONFIRM THE AVALIABILITY OF ODS  BASIC DETAILS  
        IF VALGNAME IS NOT NULL 
        THEN
            -- STEP 5a : CHECK DATA EXISTS FOR BATCH AND RUN DATE IN THE ALGORITHM OUTPUT TABLE   , IF DATA EXISTS DELETE THE DATA FROM OUTPUT TABLE       
            SELECT 
                COUNT(*) 
            INTO 
                V_EXISTING_REC_CNT 
            FROM 
                PHM_ALG_OUTPUT  
            WHERE  
                BATCH_NUM = V_BATCH_NUM 
            AND 
                RUN_DATE = V_RUN_DATE 
            AND 
                PHM_PATTERNS_SK = V_ALG_NUM; --AND ROWNUM < 5;

            IF V_EXISTING_REC_CNT > 0 
            THEN
                DELETE FROM 
                    PHM_ALG_OUTPUT  
                WHERE  
                    BATCH_NUM = V_BATCH_NUM 
                AND 
                    RUN_DATE = V_RUN_DATE 
                AND 
                    PHM_PATTERNS_SK = V_ALG_NUM;
                COMMIT;
            END IF;

            -- STEP 5b : CHECK DATA EXISTS FOR BATCH AND RUN DATE IN THE ALGORITHM CHART OUTPUT  TABLE   , IF DATA EXISTS DELETE THE DATA FROM OUTPUT TABLE       
            SELECT 
                COUNT(*) 
            INTO 
                V_EXISTING_REC_CNT 
            FROM 
                PHM_ALG_CHART_OUTPUT  
            WHERE  
                BATCH_NUM = V_BATCH_NUM 
            AND 
                RUN_DATE = V_RUN_DATE 
            AND 
                PHM_PATTERN_SK = V_ALG_NUM; --AND ROWNUM < 5;

            IF V_EXISTING_REC_CNT > 0 
            THEN
                DELETE FROM 
                    PHM_ALG_CHART_OUTPUT  
                WHERE  
                    BATCH_NUM = V_BATCH_NUM 
                AND 
                    RUN_DATE = V_RUN_DATE 
                AND 
                    PHM_PATTERN_SK = V_ALG_NUM;
                COMMIT;
            END IF;

            -- STEP 6 : ALGORIOTHM SPECIFIC CODE - TO WRITE INTO COMMON OUTPUT TABLE>

            -- FOR EACH OF THE INSTRUMENTS COMING IN THE BATCH, IDENTIFY IF IT IS IN THE FLAGGED LIST,
            -- IF YES, it is flagged, otherwise it is unflagged
            V_FLAG_COUNT := 0;
            FOR DL IN DEVICE_SN_LIST
            LOOP
                BEGIN
                    V_FLAG := 'NO';
                    V_IHN_LEVEL3_DESC_VAL := NULL;
                    V_FLAG_DATE_TIME := V_RUN_DATE;
                    V_RES_COUNT := 0;
                    V_FLAGGED_PL := NULL;
                    V_FLAGGED_EXP_CODE := NULL;

                    FOR x IN FLAG_LIST(DL.SERIAL_NUM, 
                                       DL.DEVICEID)
                    LOOP
                        V_FLAG := 'YES';
                        V_IHN_LEVEL3_DESC_VAL := V_IHN_LEVEL3_DESC;
                        V_RES_COUNT := 1;
                        V_FLAG_COUNT := V_FLAG_COUNT + 1;
                        -- Get the PL and experience code for the flagged instrument
                        PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(V_ALG_NUM, 
                                                                      DL.PL, 
                                                                      NULL, 
                                                                      V_FLAGGED_PL, 
                                                                      V_FLAGGED_EXP_CODE);
                    END LOOP;


                    --  INSERT THE DATA INTO COMMON RESULT OUTPUT TABLE 
                    PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(DL.CUSTOMER_NAME, 
                                                                             DL.CUSTOMER_NUMBER, 
                                                                             DL.DEVICEID , 
                                                                             DL.SERIAL_NUM, 
                                                                             DL.COUNTRY_NAME, 
                                                                             DL.AREA, 
                                                                             V_ALG_DFN_SK, 
                                                                             -1, 
                                                                             V_FLAG_DATE_TIME, 
                                                                             V_RES_COUNT , 
                                                                             V_FLAG, 
                                                                             V_IHN_LEVEL3_DESC_VAL, 
                                                                             NULL, 
                                                                             VALGNAME, 
                                                                             NULL, 
                                                                             V_BATCH_NUM, 
                                                                             V_ALG_NUM, 
                                                                             V_RUN_DATE, 
                                                                             V_PROCESS_ID, 
                                                                             V_FLAGGED_PL, 
                                                                             V_FLAGGED_EXP_CODE);

                    --  INSERT THE DATA INTO COMMON CHART OUTPUT TABLE 
                    PHM_ALGORITHM_UTILITIES_1.PHM_ALG_CHART_INSERT(DL.DEVICEID, 
                                                                   DL.PL, 
                                                                   DL.SERIAL_NUM, 
                                                                   DL.COUNTRY_NAME, 
                                                                   DL.AREA , 
                                                                   V_ALG_NUM, 
                                                                   NULL, 
                                                                   NULL, 
                                                                   V_FLAG_DATE_TIME, 
                                                                   V_RES_COUNT, 
                                                                   TO_CHAR(GET_MS_FROM_DATE(V_FLAG_DATE_TIME)) , 
                                                                   VALGNAME, 
                                                                   SYSDATE, 
                                                                   V_BATCH_NUM, 
                                                                   V_RUN_DATE, 
                                                                   V_ALG_DFN_SK);                

                    V_REC_COUNT := V_REC_COUNT + 1;
                    IF V_REC_COUNT > 5000
                    THEN
                        V_REC_COUNT := 0;
                        COMMIT;
                    END IF;
                    V_REC_INS_COUNT := V_REC_INS_COUNT + 1;  -- DL%ROWCOUNT

                --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS 
                EXCEPTION
                WHEN OTHERS THEN
                    V_ERROR_MESSAGE :=  ' PHM_HC_WASTE_algorithm EXECUTION HAS FAILED FOR '||V_ALG_NUM||' FOR '||DL.SERIAL_NUM||' FOR DATE '||V_FLAG_DATE_TIME|| ', ERROR :'|| SQLERRM;
                    V_PROCESS_STATUS := 'ERRORED';
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                                    V_PROD_FAMILY,
                                                                    V_PROCESS_TYPE,
                                                                    V_ROUTINE_TYPE,
                                                                    VALGNAME,
                                                                    V_ROUTINE_NAME,
                                                                    V_RUN_MODE,
                                                                    V_PROCESS_STATUS,
                                                                    V_ERROR_MESSAGE,
                                                                    V_RUN_DATE ,
                                                                    SYSDATE,
                                                                    V_BATCH_NUM ,
                                                                    V_UNIX_ID ,
                                                                    V_ALG_NUM);
                    EXIT;
                END;
            END LOOP;

            -- </  CHANGE  >

            -- STEP 7 PURPOSE - TO UPDATED THE PROCESS WITH COMPLETED STATUS IN THE AUDIT TABLES 
            V_PROCESS_STATUS := 'COMPLETED';
            V_ERROR_MESSAGE  := '';
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                            V_PROD_FAMILY,
                                                            V_PROCESS_TYPE,
                                                            V_ROUTINE_TYPE,
                                                            VALGNAME,
                                                            V_ROUTINE_NAME,
                                                            V_RUN_MODE,
                                                            V_PROCESS_STATUS,
                                                            V_ERROR_MESSAGE,
                                                            V_RUN_DATE ,
                                                            SYSDATE,
                                                            V_BATCH_NUM ,
                                                            V_UNIX_ID,V_ALG_NUM );
            COMMIT;
        ELSE 
            V_ERROR_MESSAGE := ' NOT ABLE FIND BASIC INFORMATION OF ALGORITHM '||V_ALG_NUM||' WITH ERROR ' || SQLERRM;
            V_PROCESS_STATUS := 'ERRORED';
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                            V_PROD_FAMILY,
                                                            V_PROCESS_TYPE,
                                                            V_ROUTINE_TYPE,
                                                            VALGNAME,
                                                            V_ROUTINE_NAME,
                                                            V_RUN_MODE,
                                                            V_PROCESS_STATUS,
                                                            V_ERROR_MESSAGE,
                                                            V_RUN_DATE ,
                                                            SYSDATE,
                                                            V_BATCH_NUM ,
                                                            V_UNIX_ID,V_ALG_NUM );
        END IF;

    --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS 
    EXCEPTION
    WHEN OTHERS THEN
        V_PROCESS_STATUS := 'ERRORED';
        V_ERROR_MESSAGE  := 'ALGORITHM EXECUTION FAILED FOR PHM_HC_WASTE_algorithm, DUE TO: ' || SQLERRM;
        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                        V_PROD_FAMILY,
                                                        V_PROCESS_TYPE,
                                                        V_ROUTINE_TYPE,
                                                        VALGNAME,
                                                        V_ROUTINE_NAME,
                                                        V_RUN_MODE,
                                                        V_PROCESS_STATUS,
                                                        V_ERROR_MESSAGE,
                                                        V_RUN_DATE ,
                                                        SYSDATE,
                                                        V_BATCH_NUM ,
                                                        V_UNIX_ID,V_ALG_NUM );
    COMMIT;
END PHM_HC_WASTE_algorithm;


-- Assigning the grants to SVC_PHM_CONNECT user (ETL process requires this)
grant execute on SVC_PHM_OWNER.PHM_HC_WASTE_algorithm to SVC_PHM_CONNECT;
