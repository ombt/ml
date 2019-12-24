CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_DARK_COUNT(V_ALG_NUM NUMBER,
                                                         V_RUN_DATE DATE,
                                                         V_BATCH_NUM VARCHAR2, 
                                                         V_UNIX_ID VARCHAR2)
IS

/******************************************************************************
   NAME:       PHM_DARK_COUNT
   PURPOSE:   DARK COUNT DATA WILL COLLECTED AND VERIFIED AGAINST THE THRESHOLD VALUES

   REVISIONS:
   Ver        Date        Author            Description
   ---------  ----------  ---------------  ------------------------------------
   1.0         07/24/2015   Sivakrishna Angadi            Procedure Created  
   2.0        07/12/2016   Sivakrishna Angadi            Procedure updated for sun approach and batch run
   3.0        10/03/2017  Siva Chandaluri               Updated the procedure to use instrumentlisting for instrument/customer/country info and populate PL/EC Code in PHM_ALG_OUTPUT table   

******************************************************************************/

CURSOR DEVICE_STATS (V_MODULESNDRM VARCHAR2,
                     V_START_DATE DATE,
                     DAYS_NUM NUMBER,
                     V_END_DATE DATE)
IS
    SELECT 
        R.DEVICEID DEVICE_ID, 
        R.MODULESNDRM MODULE_SN,
        TRUNC (R.COMPLETIONDATE) TEST_COMPLETION_DATE, 
        MAX(R.COMPLETIONDATE) MAX_COMP_DATE,
        MAX(TO_NUMBER(R.DARKCOUNT)) MAX_DARK_COUNT_BY_DAY, 
        STDDEV (R.DARKCOUNT) STD, 
        COUNT (R.DARKCOUNT) COUNT_DARK_COUNT_BY_DAY,
        AVG (TO_NUMBER (R.DARKCOUNT)) AVG_DARK_COUNT 
    FROM 
        SVC_PHM_ODS.PHM_ODS_RESULTS_IA R 
    WHERE 
        MODULESNDRM = V_MODULESNDRM 
    AND 
        DARKCOUNT IS NOT NULL
    AND 
        COMPLETIONDATE BETWEEN 
            V_START_DATE - DAYS_NUM 
        AND 
            V_END_DATE 
    GROUP BY 
        DEVICEID,
        MODULESNDRM,
        TRUNC(COMPLETIONDATE) 
    ORDER BY 1,2;

--CURSOR DEVICE_AND_DATE_LIST
--IS 
--SELECT ODS.MODULESNDRM,IMI.COUNTRYNAME,IMI.AREAREGION,IMI.CUSTOMERNAME,IMI.CUSTOMERNUMBER,IMI.MODULETYPE,
--MIN(TRUNC(ODS.COMPLETIONDATE)) CUR_MIN_DATE, MAX(ODS.COMPLETIONDATE) CUR_MAX_DATE
--FROM SVC_PHM_ODS.PHM_ODS_RESULTS_IA ODS, IDAOWNER.IDAMODULEINFORMATION IMI 
--WHERE RUN_DATE = V_RUN_DATE AND BATCH_NUM = V_BATCH_NUM AND IMI.MODULESN = ODS.MODULESNDRM AND IMI.CUSTOMERNUMBER IS NOT NULL 
--AND IMI.MODULETYPE LIKE 'I%' AND IMI.EFFECTIVEFROMDATE <= SYSDATE AND IMI.EFFECTIVETODATE >= SYSDATE 
--GROUP BY ODS.MODULESNDRM,IMI.COUNTRYNAME,IMI.AREAREGION,IMI.CUSTOMERNAME,IMI.CUSTOMERNUMBER,IMI.MODULETYPE
--ORDER BY 1,5;

CURSOR DEVICE_AND_DATE_LIST
IS 
    SELECT  
        IA.DEVICEID, 
        IA.MODULESNDRM, 
        MAX (IL.PL) PL, 
        MAX (IL.CUSTOMER_NUM) CUSTOMERNUMBER,
        MAX (IL.CUSTOMER) CUSTOMERNAME, 
        MAX (PC.COUNTRY) COUNTRYNAME, 
        MAX (PC.AREAREGION) AREA, 
        MAX (IL.CITY) CITY, 
        MIN(TRUNC(IA.COMPLETIONDATE)) CUR_MIN_DATE, 
        MAX(IA.COMPLETIONDATE) CUR_MAX_DATE,
        CASE WHEN IA.MODULESNDRM like 'I1SR%' 
             then 
                 'I1SR'
             WHEN IA.MODULESNDRM like 'ISR%' 
             then 
                 'I2SR'
             WHEN IA.MODULESNDRM like 'I20%' 
             then 
                 'I2000'
             ElSE 
                 IA.MODULESNDRM 
             END MODULETYPE
    FROM 
        SVC_PHM_ODS.PHM_ODS_RESULTS_IA IA, 
        INSTRUMENTLISTING IL, 
        PHM_COUNTRY PC
    WHERE 
        IA.COMPLETIONDATE > TRUNC(SYSDATE) - 3 
    AND 
        IA.RUN_DATE = V_RUN_DATE 
    AND 
        IA.BATCH_NUM = V_BATCH_NUM 
    AND 
        IL.SN = IA.MODULESNDRM 
    AND 
        PC.COUNTRY_CODE = IL.COUNTRY_CODE

    GROUP BY 
        IA.DEVICEID, 
        IA.MODULESNDRM
    ORDER BY 2;  

CURSOR ALL_THRESHOLDS (VALGNUM NUMBER)
IS 
    SELECT 
        PTT.PHM_THRESHOLDS_SK,
        PTT.THRESHOLD_NUMBER,
        PTT.THRESHOLD_NUMBER_UNIT,
        PTT.THRESHOLD_NUMBER_DESC,
        PP.PHM_PATTERNS_SK,
        PP.PATTERN_DESCRIPTION,
        PTT.THRESHOLD_ALERT,
        PTT.THRESHOLD_TYPE, 
        PTT.THRESHOLD_DATA_DAYS
    FROM 
        PHM_PATTERNS PP, 
        PHM_THRESHOLDS PTT 
    WHERE 
        PP.PHM_PATTERNS_SK = PTT.PHM_PATTERNS_SK
    AND 
        PP.PHM_ALGORITHM_DEFINITIONS_SK = PTT.PHM_ALGORITHM_DEFINITIONS_SK 
    AND 
        PP.PHM_ALGORITHM_DEFINITIONS_SK = VALGNUM;

VCOUNTRY    IDAOWNER.IDAMODULEINFORMATION.COUNTRYNAME%TYPE ;
VCITY       IDAOWNER.IDAMODULEINFORMATION.CITY%TYPE ;
VCUSTNAME   IDAOWNER.IDAMODULEINFORMATION.CUSTOMERNAME%TYPE ; 
VCUST_NUM   IDAOWNER.IDAMODULEINFORMATION.CUSTOMERNUMBER%TYPE ;
VTYPE       IDAOWNER.IDAMODULEINFORMATION.MODULETYPE%TYPE ;

FLAG                VARCHAR(5);
NUM_DAYS            NUMBER(2);
VALGNAME            VARCHAR(25);
V_ERROR_MESSAGE     VARCHAR(2000);
VIHN4_CALL_MESSAGE  VARCHAR(150);
VMAX_THRESHOLD_UNIT NUMBER(3);
V_PROCESS_TYPE      VARCHAR(25); 
V_PROCESS_STATUS    VARCHAR2(25) := 'STARTED';
V_PROCESS_ID        NUMBER(15);
V_PROD_FAMILY       VARCHAR2(25);
V_RUN_MODE          VARCHAR2(10);
V_ROUTINE_NAME      VARCHAR(35);
V_ROUTINE_TYPE      VARCHAR(35);
V_ALG_TYPE          VARCHAR2(10);
V_INSERT_COUNT      NUMBER(25);
V_FLAGGED_PL        VARCHAR2(10);
V_FLAGGED_EXP_CODE  VARCHAR2(10);

V_DATE DATE;

BEGIN
    /* TO GET ALL TEH BASIC INFO FOR TEH ALGORITHM NUMBER PROVIDED */
    PHM_ALGORITHM_UTILITIES_1.PHM_GET_ALG_DETAILS(V_ALG_NUM, 
                                                  VALGNAME,
                                                  V_PROCESS_TYPE,
                                                  V_ROUTINE_NAME,
                                                  V_RUN_MODE,
                                                  V_PROD_FAMILY);

    V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
  
    IF VALGNAME IS NOT NULL THEN
    BEGIN
        DELETE FROM PHM_DC_DATA WHERE BATCH_NUM = V_BATCH_NUM;
        DELETE FROM PHM_ALG_OUTPUT WHERE PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM AND  BATCH_NUM = V_BATCH_NUM ;

        COMMIT;

        EXCEPTION
        WHEN OTHERS THEN /* TO CATCH ALL EXCEPTIONS WHILE DELETING THE DATA */
            V_PROCESS_STATUS := 'ERRORED';
            V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA OF PREVIOUS RUN FOR RUN_DATE '||V_RUN_DATE||' FOR BATCH_NUM ' ||V_BATCH_NUM ||' DUE TO  : '||SQLERRM;
            PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,
                                                            V_ALG_NUM,
                                                            V_RUN_DATE,
                                                            V_BATCH_NUM,
                                                            V_ERROR_MESSAGE,
                                                            VALGNAME); 
    END;

    IF V_PROCESS_STATUS <>  'ERRORED' THEN
        V_PROCESS_STATUS := 'STARTED'; /* TO INSERT AUDIT RECORDS AS PROCESS STARTED */
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
        
    --DBMS_OUTPUT.PUT_LINE('V_PROCESS_ID: ' || V_PROCESS_ID || ', VALGNAME: ' || VALGNAME || ', V_ROUTINE_NAME: ' || V_ROUTINE_NAME);
        
    FOR AP IN ALL_THRESHOLDS (V_ALG_NUM) /* GET ALL THE THRESHOLDS */
    LOOP
        FOR DD IN DEVICE_AND_DATE_LIST /*GET ALL THE DEVICE LIST */
        LOOP
            --DBMS_OUTPUT.PUT_LINE('MODULESNDRM: ' || DD.MODULESNDRM || ', DD.CUR_MIN_DATE: ' || DD.CUR_MIN_DATE || ', DD.CUR_MAX_DATE: ' || DD.CUR_MAX_DATE || ', AP.THRESHOLD_DATA_DAYS: ' || AP.THRESHOLD_DATA_DAYS); 
            FOR DS IN  DEVICE_STATS (DD.MODULESNDRM,
                                     DD.CUR_MIN_DATE,
                                     AP.THRESHOLD_DATA_DAYS,
                                     DD.CUR_MAX_DATE) /* EACH DEVICE STATICS TO PHM_DC_DATA*/
            LOOP
                BEGIN
                --DBMS_OUTPUT.PUT_LINE('MODULE_SN: ' || DS.MODULE_SN || ', AP.THRESHOLD_TYPE: ' || AP.THRESHOLD_TYPE || ', DD.CUR_MAX_DATE: ' || DD.CUR_MAX_DATE); 
                IF AP.THRESHOLD_TYPE = 'SD' THEN /* INSERT SD INTO DEVICE_VALUE COLUMN*/
                    INSERT INTO PHM_DC_DATA VALUES (V_RUN_DATE,
                                                    V_BATCH_NUM,
                                                    AP.PHM_THRESHOLDS_SK,
                                                    DS.DEVICE_ID,
                                                    DS.MODULE_SN, 
                                                    AP.THRESHOLD_TYPE,
                                                    DS.MAX_COMP_DATE,
                                                    DS.STD,
                                                    0,
                                                    VALGNAME, 
                                                    SYSDATE, 
                                                    V_UNIX_ID);
                END IF;
                    
                IF AP.THRESHOLD_TYPE = 'AVG' THEN /* INSERT AVG INTO DEVICE_VALUE COLUMN*/
                    INSERT INTO 
                        PHM_DC_DATA 
                    VALUES (
                        V_RUN_DATE,
                        V_BATCH_NUM,
                        AP.PHM_THRESHOLDS_SK,
                        DS.DEVICE_ID,
                        DS.MODULE_SN, 
                        AP.THRESHOLD_TYPE,
                        DS.MAX_COMP_DATE,
                        DS.AVG_DARK_COUNT,
                        0,
                        VALGNAME, 
                        SYSDATE, 
                        V_UNIX_ID);
                END IF;   

                V_INSERT_COUNT := V_INSERT_COUNT + 1;
                IF MOD(V_INSERT_COUNT,10000) = 0 THEN 
                    COMMIT; 
                END IF;                          

                EXCEPTION WHEN OTHERS THEN /* TO CATCH AL TEH EXCPETIONS WHILE GATHERING THE DEVICE INFO */
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE := 'NOT ABLE TO INSERT DATA INTO PHM_DC_DATA FOR SN '|| DS.MODULE_SN||' DATE:'||DS.MAX_COMP_DATE||' FOR THRESHOLD_SK'||AP.PHM_THRESHOLDS_SK||' THE BATCH_NUM' || V_BATCH_NUM||', RUN_DATE ' ||V_RUN_DATE ||'- DUE TO ' || SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_ALG_NUM,'',V_PROCESS_TYPE,V_ROUTINE_TYPE,VALGNAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID ,V_ALG_NUM);
                END;                                 
            END LOOP;
        END LOOP;
    END LOOP;
       
    COMMIT;

    FOR AP IN ALL_THRESHOLDS(V_ALG_NUM)/* GET ALL THRESHOLD VALUES */
    LOOP
        FOR DD IN DEVICE_AND_DATE_LIST/* GET ALL THE DEVICES */
        LOOP
            VIHN4_CALL_MESSAGE := '';
            FLAG := 'NO';
            NUM_DAYS := 0;
            V_FLAGGED_PL := NULL;
            V_FLAGGED_EXP_CODE := NULL;
            BEGIN  /* PROCESS ERROR CONDITIONS FOR EACH DEVICE */
                FOR X IN (
                    SELECT 
                        * 
                    FROM 
                        PHM_DC_DATA 
                    WHERE 
                        BATCH_NUM = V_BATCH_NUM 
                    AND 
                        RUN_DATE = V_RUN_DATE 
                    AND 
                        MODULESNDRM = DD.MODULESNDRM 
                    AND 
                       PHM_THRESHOLDS_SK = AP.PHM_THRESHOLDS_SK 
                    ORDER BY 
                    COMPLETIONDATE )
               LOOP
                   V_DATE := X.COMPLETIONDATE;
                   IF X.DEVICE_VALUE >= AP.THRESHOLD_NUMBER THEN /* VERYFING THE NUMBER OF ERRORS AGAINST THRESHOLD ERROR COUNT */
                       NUM_DAYS := NUM_DAYS + 1;
                       IF NUM_DAYS >= AP.THRESHOLD_NUMBER_UNIT THEN /* VERYFING THE NUMBER OF DAYS AGAINST THRESHOLD ERROR DAYS */
                           FLAG := 'YES';
                           VIHN4_CALL_MESSAGE := LOWER(SUBSTR(DD.MODULETYPE,1,2)) || 
                                                 'SR ' || 
                                                 AP.THRESHOLD_ALERT;
                          --DBMS_OUTPUT.PUT_LINE('FLAGGED: ' || FLAG || ', VIHN4_CALL_MESSAGE: ' || VIHN4_CALL_MESSAGE || ', AP.PHM_PATTERNS_SK: ' || AP.PHM_PATTERNS_SK || ', PL: ' || DD.PL);
                          -- Get the PL and experience code for the flagged instrument
                          PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(AP.PHM_PATTERNS_SK, DD.PL, VIHN4_CALL_MESSAGE, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);
                          --DBMS_OUTPUT.PUT_LINE('V_FLAGGED_PL: ' || V_FLAGGED_EXP_CODE || ', V_FLAGGED_EXP_CODE: ' || ', AP.PHM_PATTERNS_SK: ' || AP.PHM_PATTERNS_SK || ', PL: ' || DD.PL);
                       ELSE
                           FLAG := 'NO';
                           VIHN4_CALL_MESSAGE := '';
                       END IF;
                   ELSE
                       FLAG := 'NO';     
                       NUM_DAYS := 0;
                       VIHN4_CALL_MESSAGE := '';
                   END IF; 
                   /* INSERT DATA INTO OUTPUT TABLES WITH FLAGGING DETAILS.  */
                   PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(DD.CUSTOMERNAME,
                                                                            DD.CUSTOMERNUMBER,
                                                                            X.DEVICEID,
                                                                            X.MODULESNDRM,
                                                                            DD.COUNTRYNAME,
                                                                            DD.AREA,
                                                                            V_ALG_NUM,
                                                                            AP.PHM_THRESHOLDS_SK,
                                                                            X.COMPLETIONDATE,
                                                                            X.DEVICE_VALUE,
                                                                            FLAG,
                                                                            VIHN4_CALL_MESSAGE,
                                                                            '',
                                                                            VALGNAME,
                                                                            '',
                                                                            V_BATCH_NUM,
                                                                            AP.PHM_PATTERNS_SK,
                                                                            V_RUN_DATE,
                                                                            V_PROCESS_ID, 
                                                                            V_FLAGGED_PL, 
                                                                            V_FLAGGED_EXP_CODE);
                    V_INSERT_COUNT := V_INSERT_COUNT + 1;
                    IF MOD(V_INSERT_COUNT,10000) = 0 THEN 
                        COMMIT; 
                    END IF;                   
                END LOOP;
                EXCEPTION 
                WHEN OTHERS THEN  /* TO CATCH ANY ERROR THAT HAPPENS DURING THE ALGORITHMS CALCULATIONS AND MAKING THE RUN AS ERRORD IN AUDIT TABLES  */
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE := 'NOT ABLE TO PROCES THE THRESHOLD CONDITION FOR SN ' ||
                                       DD.MODULESNDRM || 
                                       ' FOR DATE ' ||
                                       V_DATE ||
                                       ' FOR THRESHOLDS_SK ' ||
                                       AP.PHM_THRESHOLDS_SK ||
                                       ' FOR THE BATCH_NUM ' || 
                                       V_BATCH_NUM || 
                                       ', RUN_DATE ' ||
                                       V_RUN_DATE || 
                                       '- DUE TO ' || 
                                       SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_ALG_NUM,
                                                                    '',
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
                END;                                 
            END LOOP;
        END LOOP;
    END IF;
    ELSE  /* LOGGING THE ERROR MESSAGE IF BASIC ALGORITHM DETAILS NOT FOUND  */
        V_PROCESS_STATUS := 'ERRORED';
        V_ERROR_MESSAGE := 'NOT ABLE TO FIND BASIC DETAILS OF ALGORITHM '||V_ALG_NUM||' DUT TO '||SQLERRM;
        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_ALG_NUM,
                                                        '',
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
                                                        V_UNIX_ID,
                                                        V_ALG_NUM );
    END IF;
        
    IF V_PROCESS_STATUS <> 'ERRORED' THEN /* UPDATING THE ALGORITHM STATUS TO COMPLETED IF NOT ERROR OCCURS  */
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
                                                        V_UNIX_ID,
                                                        V_ALG_NUM );
    END IF;        
     
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        V_PROCESS_STATUS := 'ERRORED';
        V_ERROR_MESSAGE := 'ALGORITHM PROCES HAS FAILED FOR ALGORITHM ' ||
                           V_ALG_NUM ||
                           ' FOR THE BATCH_NUM ' || 
                           V_BATCH_NUM || 
                           ', RUN_DATE ' ||
                           V_RUN_DATE || 
                           '- DUE TO ' || 
                           SQLERRM;
        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_ALG_NUM,
                                                        '',
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
                                                        V_UNIX_ID,
                                                        V_ALG_NUM );
END PHM_DARK_COUNT;

CREATE OR REPLACE PUBLIC SYNONYM PHM_DARK_COUNT FOR SVC_PHM_OWNER.PHM_DARK_COUNT;

GRANT EXECUTE ON  SVC_PHM_OWNER.PHM_DARK_COUNT  TO SVC_PHM_CONNECT;

GRANT EXECUTE ON  SVC_PHM_OWNER.PHM_DARK_COUNT  TO SVC_PHM_CONNECT_ROLE;


