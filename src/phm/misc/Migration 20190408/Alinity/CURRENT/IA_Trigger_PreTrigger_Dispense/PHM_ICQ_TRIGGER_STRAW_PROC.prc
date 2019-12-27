CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_ICQ_Trigger_Straw_PROC (
   V_ALG_NUM      NUMBER,
   V_RUN_DATE     DATE,
   V_BATCH_NUM    VARCHAR2,
   V_UNIX_ID      VARCHAR2)
IS
   --  AUDIT LOG VARIABLE

   V_PROCESS_TYPE          VARCHAR2 (25);
   V_PROCESS_STATUS        VARCHAR2 (25) := 'STARTED';
   V_PROCESS_ID            NUMBER (15);
   V_PROD_FAMILY           VARCHAR2 (25);
   V_RUN_MODE              VARCHAR2 (10);
   V_ROUTINE_NAME          VARCHAR (35);
   V_ROUTINE_TYPE          VARCHAR (35);
   V_ERROR_MESSAGE         VARCHAR (4000);


   -- ALGORITHM PARAMETER VARAIBLES TO HANDLE THE PROCESS FLOW
   V_IHN_LEVEL3_DESC       VARCHAR2 (200);
   V_IHN_LEVEL3_DESC_VAL   VARCHAR2 (200);
   V_FLAGGED_PL            VARCHAR2 (10);
   V_FLAGGED_EXP_CODE      VARCHAR2 (10);

   -- ALGORITHM LOCAL VARAIBLES TO HANDLE THE PROCESS FLOW

   V_EXISTING_REC_CNT      NUMBER;
   V_FLAG                  VARCHAR (5);
   V_REC_COUNT             NUMBER := 0;
   V_REC_INS_COUNT         NUMBER := 0;
   V_FLAG_DATE_TIME        DATE;
   V_RES_COUNT             NUMBER;
   V_FLAG_COUNT            NUMBER;
   VALGNAME                VARCHAR (25);
   V_ALG_DFN_SK            NUMBER;

   -- Cursor to identify flagged instruments. Written as a function
   CURSOR FLAG_LIST (
      V_MODULESNDRM    VARCHAR2,
      V_DEVICEID       NUMBER)
   IS
    SELECT   MODULESN,DeviceId 
        FROM
        (SELECT
        DeviceId, MODULESN, TO_CHAR(LOGDATE_LOCAL, 'YYYY-MM-DD') AS date_only,
        (CASE
            WHEN CORRECTEDCOUNT <= 30 AND peak_adj_signal / dark_adj_signal <= 0.44 THEN 1
            WHEN CORRECTEDCOUNT <= 50 AND peak_adj_signal / dark_adj_signal <= 0.40 THEN 1
            WHEN CORRECTEDCOUNT <= 70 AND peak_adj_signal / dark_adj_signal <= 0.35 THEN 1
            ELSE 0
        END) AS shapeflag
        FROM
        (SELECT
            i.DeviceId, i.MODULESN, i.LOGDATE_LOCAL, i.CORRECTEDCOUNT,
            SUM(CASE WHEN r.signal > i.DARKAVERAGE AND r.time IN (4, 5, 6, 7)
            THEN r.signal - i.DARKAVERAGE ELSE 0 END) AS peak_adj_signal,
            SUM(CASE WHEN r.signal > i.DARKAVERAGE
            THEN r.signal - i.DARKAVERAGE ELSE 0 END) AS dark_adj_signal
        FROM SVC_PHM_ODS.PHM_ODS_ICQ_RESULTS i
            --IDAQOWNER.ICQ_RESULTS i
        INNER JOIN
            (SELECT *
            FROM SVC_PHM_ODS.PHM_ODS_ICQ_RESULTS_READS  --IDAQOWNER.ICQ_RESULTS_READS
            UNPIVOT (signal FOR time IN
            (S01 AS  1, S02 AS  2, S03 AS  3, S04 AS  4, S05 AS  5, S06 AS  6,
             S07 AS  7, S08 AS  8, S09 AS  9, S10 AS 10, S11 AS 11, S12 AS 12,
             S13 AS 13, S14 AS 14, S15 AS 15, S16 AS 16, S17 AS 17, S18 AS 18,
             S19 AS 19, S20 AS 20, S21 AS 21, S22 AS 22, S23 AS 23, S24 AS 24,
             S25 AS 25, S26 AS 26, S27 AS 27, S28 AS 28, S29 AS 29, S30 AS 30))
            ) r
        ON
            i.ID = r.ICQ_RESULTS_ID
        WHERE i.MODULESN = V_MODULESNDRM
                    AND i.DEVICEID = V_DEVICEID                     AND
            TRUNC(i.LOGDATE_LOCAL) = TRUNC(SYSDATE) - 1     AND
            TRUNC(r.LOGDATE_LOCAL) = TRUNC(SYSDATE) - 1     AND
            LOWER(i.SAMPLEID) NOT LIKE  '%saline%'         AND
            LOWER(i.SAMPLEID) NOT LIKE  '%buf%'            AND
            LOWER(i.OPERATORID) NOT LIKE 'fse'             AND
            ASSAYNUMBER NOT LIKE  '%213%'                  AND
            ASSAYNUMBER NOT LIKE  '%216%'            
        GROUP BY
            i.DeviceId, i.MODULESN, i.LOGDATE_LOCAL, i.CORRECTEDCOUNT
        )
        )
    GROUP BY
        DeviceId, MODULESN, date_only
    HAVING
        COUNT(*) >= 50 AND
        AVG(shapeflag) > 0.01;

   -- Curstor to identify all instruments available in IDA during batch (taken from PHM_ODS_RESULTS_CC)
   CURSOR DEVICE_SN_LIST
   IS
        SELECT CC.DEVICEID,
               UPPER (CC.MODULESN) SERIAL_NUM,
               MAX (IL.PL) PL,
               MAX (IL.CUSTOMER_NUM) CUSTOMER_NUMBER,
               MAX (IL.CUSTOMER) CUSTOMER_NAME,
               MAX (PC.COUNTRY) COUNTRY_NAME,
               MAX (PC.AREAREGION) AREA,
               MAX (IL.CITY) CITY,
               COUNT (*) DEVICE_SN_CNT
          FROM --SVC_PHM_ODS.PHM_ODS_RESULTS_CC CC,
               SVC_PHM_ODS.PHM_ODS_ICQ_RESULTS CC,
               INSTRUMENTLISTING IL,
               PHM_COUNTRY PC
         WHERE     CC.BATCH_NUM = V_BATCH_NUM
               AND CC.RUN_DATE = V_RUN_DATE
               AND UPPER (CC.MODULESN) = UPPER (IL.SN)
               AND PC.COUNTRY_CODE = IL.COUNTRY_CODE
               AND IL.INST_STATUS = 'Active'
      GROUP BY CC.DEVICEID, CC.MODULESN;
BEGIN
   -- STEP 1   :PURPOSE TO GET PROCESSID OF CURRENT EXECUTION

   V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID ();
   V_PROCESS_STATUS := 'STARTED';

   -- STEP 2  :   PURPOSE TO GET THE REQUIRED ALGORITHM INFORMATION FROM CONFIGURATION TABLES

   SELECT AR.ROUTINE_NAME,
          AR.ROUTINE_TYPE,
          AR.RUN_MODE,
          AR.ROUTINE_INVOKE_COMMAND,
          PF.PRODUCT_FAMILY_NAME
     INTO VALGNAME,
          V_PROCESS_TYPE,
          V_RUN_MODE,
          V_ROUTINE_NAME,
          V_PROD_FAMILY
     FROM PHM_ALGORITHM_ROUTINES AR, PHM_PATTERNS PP, PHM_PRODUCT_FAMILY PF
    WHERE     AR.PHM_PATTERNS_SK = V_ALG_NUM
          AND PP.PHM_PATTERNS_SK = AR.PHM_PATTERNS_SK
          AND PP.PHM_PROD_FAMILY_SK = PF.PHM_PROD_FAMILY_SK;

   -- GET ALGORITHM_DEFINITION_SK
   SELECT PP.PHM_ALGORITHM_DEFINITIONS_SK
     INTO V_ALG_DFN_SK
     FROM PHM_PATTERNS PP, PHM_ALGORITHM_DEFINITIONS PAD
    WHERE     PP.PHM_ALGORITHM_DEFINITIONS_SK =
                 PAD.PHM_ALGORITHM_DEFINITIONS_SK
          AND PP.PHM_PATTERNS_SK = V_ALG_NUM
          AND ALGORITHM_NAME IN (SELECT ROUTINE_NAME
                                   FROM PHM_ALGORITHM_ROUTINES
                                  WHERE PHM_PATTERNS_SK = V_ALG_NUM);

   -- Ex: 12941    ARCHITECT IA    ALG Oracle Procedure    Oracle Procedure    FEP    PHM_FE_PRESSURE    Batch    9/8/2016 10:20:36 PM        STARTED        9/8/2016    9/8/2016 10:20:36.000000 PM    BTH2200    NULL        1003
   PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (V_PROCESS_ID,
                                                    V_PROD_FAMILY,
                                                    V_PROCESS_TYPE,
                                                    V_ROUTINE_TYPE,
                                                    VALGNAME,
                                                    V_ROUTINE_NAME,
                                                    V_RUN_MODE,
                                                    V_PROCESS_STATUS,
                                                    V_ERROR_MESSAGE,
                                                    V_RUN_DATE,
                                                    SYSDATE,
                                                    V_BATCH_NUM,
                                                    V_UNIX_ID,
                                                    V_ALG_NUM);

   --  STEP 3 : PURPOSE - TO GET THE ALL THE PARAMETERS THAT WERE DEFINED IN THE ALGORITHM SCREEN

   FOR I
      IN (SELECT PARAMETER_VALUES, PARAMETER_NAME, PHM_PATTERNS_SK
            FROM PHM_THRESHOLD_PARAMETER
           WHERE     PHM_PATTERNS_SK = V_ALG_NUM
                 AND NVL (DELETE_FLAG, 'N') <> 'Y')
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
      SELECT COUNT (*)
        INTO V_EXISTING_REC_CNT
        FROM PHM_ALG_OUTPUT
       WHERE     BATCH_NUM = V_BATCH_NUM
             AND RUN_DATE = V_RUN_DATE
             AND PHM_PATTERNS_SK = V_ALG_NUM;                --AND ROWNUM < 5;

      IF V_EXISTING_REC_CNT > 0
      THEN
         DELETE FROM PHM_ALG_OUTPUT
               WHERE     BATCH_NUM = V_BATCH_NUM
                     AND RUN_DATE = V_RUN_DATE
                     AND PHM_PATTERNS_SK = V_ALG_NUM;

         COMMIT;
      END IF;


      -- STEP 5b : CHECK DATA EXISTS FOR BATCH AND RUN DATE IN THE ALGORITHM CHART OUTPUT  TABLE   , IF DATA EXISTS DELETE THE DATA FROM OUTPUT TABLE
      SELECT COUNT (*)
        INTO V_EXISTING_REC_CNT
        FROM PHM_ALG_CHART_OUTPUT
       WHERE     BATCH_NUM = V_BATCH_NUM
             AND RUN_DATE = V_RUN_DATE
             AND PHM_PATTERN_SK = V_ALG_NUM;                 --AND ROWNUM < 5;

      IF V_EXISTING_REC_CNT > 0
      THEN
         DELETE FROM PHM_ALG_CHART_OUTPUT
               WHERE     BATCH_NUM = V_BATCH_NUM
                     AND RUN_DATE = V_RUN_DATE
                     AND PHM_PATTERN_SK = V_ALG_NUM;

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

            FOR x IN FLAG_LIST (DL.SERIAL_NUM, DL.DEVICEID)
            LOOP
               V_FLAG := 'YES';
               V_IHN_LEVEL3_DESC_VAL := V_IHN_LEVEL3_DESC;
               V_RES_COUNT := 1;
               V_FLAG_COUNT := V_FLAG_COUNT + 1;
               -- Get the PL and experience code for the flagged instrument
               PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE (
                  V_ALG_NUM,
                  DL.PL,
                  NULL,
                  V_FLAGGED_PL,
                  V_FLAGGED_EXP_CODE);
            END LOOP;


            --  INSERT THE DATA INTO COMMON RESULT OUTPUT TABLE
            PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL (
               DL.CUSTOMER_NAME,
               DL.CUSTOMER_NUMBER,
               DL.DEVICEID,
               DL.SERIAL_NUM,
               DL.COUNTRY_NAME,
               DL.AREA,
               V_ALG_DFN_SK,
               -1,
               V_FLAG_DATE_TIME,
               V_RES_COUNT,
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
            PHM_ALGORITHM_UTILITIES_1.PHM_ALG_CHART_INSERT (
               DL.DEVICEID,
               DL.PL,
               DL.SERIAL_NUM,
               DL.COUNTRY_NAME,
               DL.AREA,
               V_ALG_NUM,
               NULL,
               NULL,
               V_FLAG_DATE_TIME,
               V_RES_COUNT,
               TO_CHAR (GET_MS_FROM_DATE (V_FLAG_DATE_TIME)),
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

            V_REC_INS_COUNT := V_REC_INS_COUNT + 1;             -- DL%ROWCOUNT
         EXCEPTION
            --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS
            WHEN OTHERS
            THEN
               V_ERROR_MESSAGE :=
                     ' PHM_ICQ_Trigger_Straw_PROC EXECUTION HAS FAILED FOR '
                  || V_ALG_NUM
                  || ' FOR '
                  || DL.SERIAL_NUM
                  || ' FOR DATE '
                  || V_FLAG_DATE_TIME
                  || ', ERROR :'
                  || SQLERRM;
               V_PROCESS_STATUS := 'ERRORED';
               PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (
                  V_PROCESS_ID,
                  V_PROD_FAMILY,
                  V_PROCESS_TYPE,
                  V_ROUTINE_TYPE,
                  VALGNAME,
                  V_ROUTINE_NAME,
                  V_RUN_MODE,
                  V_PROCESS_STATUS,
                  V_ERROR_MESSAGE,
                  V_RUN_DATE,
                  SYSDATE,
                  V_BATCH_NUM,
                  V_UNIX_ID,
                  V_ALG_NUM);
               EXIT;
         END;
      END LOOP;

      -- </  CHANGE  >

      -- STEP 7 PURPOSE - TO UPDATED THE PROCESS WITH COMPLETED STATUS IN THE AUDIT TABLES
      V_PROCESS_STATUS := 'COMPLETED';
      V_ERROR_MESSAGE := '';
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (V_PROCESS_ID,
                                                       V_PROD_FAMILY,
                                                       V_PROCESS_TYPE,
                                                       V_ROUTINE_TYPE,
                                                       VALGNAME,
                                                       V_ROUTINE_NAME,
                                                       V_RUN_MODE,
                                                       V_PROCESS_STATUS,
                                                       V_ERROR_MESSAGE,
                                                       V_RUN_DATE,
                                                       SYSDATE,
                                                       V_BATCH_NUM,
                                                       V_UNIX_ID,
                                                       V_ALG_NUM);
      COMMIT;
   ELSE
      V_ERROR_MESSAGE :=
            ' NOT ABLE FIND BASIC INFORMATION OF ALGORITHM '
         || V_ALG_NUM
         || ' WITH ERROR '
         || SQLERRM;
      V_PROCESS_STATUS := 'ERRORED';
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (V_PROCESS_ID,
                                                       V_PROD_FAMILY,
                                                       V_PROCESS_TYPE,
                                                       V_ROUTINE_TYPE,
                                                       VALGNAME,
                                                       V_ROUTINE_NAME,
                                                       V_RUN_MODE,
                                                       V_PROCESS_STATUS,
                                                       V_ERROR_MESSAGE,
                                                       V_RUN_DATE,
                                                       SYSDATE,
                                                       V_BATCH_NUM,
                                                       V_UNIX_ID,
                                                       V_ALG_NUM);
   END IF;
EXCEPTION
   --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS
   WHEN OTHERS
   THEN
      V_PROCESS_STATUS := 'ERRORED';
      V_ERROR_MESSAGE :=
            'ALGORITHM EXECUTION FAILED FOR PHM_ICQ_Trigger_Straw, DUE TO: '
         || SQLERRM;
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (V_PROCESS_ID,
                                                       V_PROD_FAMILY,
                                                       V_PROCESS_TYPE,
                                                       V_ROUTINE_TYPE,
                                                       VALGNAME,
                                                       V_ROUTINE_NAME,
                                                       V_RUN_MODE,
                                                       V_PROCESS_STATUS,
                                                       V_ERROR_MESSAGE,
                                                       V_RUN_DATE,
                                                       SYSDATE,
                                                       V_BATCH_NUM,
                                                       V_UNIX_ID,
                                                       V_ALG_NUM);
      COMMIT;
END PHM_ICQ_Trigger_Straw_PROC;
/