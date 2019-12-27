CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_ICQ_RSM_Pick_Load_PROC (
   V_ALG_NUM      NUMBER,
   V_RUN_DATE     DATE,
   V_BATCH_NUM    VARCHAR2,
   V_UNIX_ID      VARCHAR2)
IS

   --  AUDIT LOG VARIABLE

   V_PROCESS_TYPE       VARCHAR2 (25);
   V_PROCESS_STATUS     VARCHAR2 (25) := 'STARTED';
   V_PROCESS_ID         NUMBER (15);
   V_PROD_FAMILY        VARCHAR2 (25);
   V_RUN_MODE           VARCHAR2 (10);
   V_ROUTINE_NAME       VARCHAR (35);
   V_ROUTINE_TYPE       VARCHAR (35);
   V_ERROR_MESSAGE      VARCHAR (4000);


   -- ALGORITHM LOCAL VARAIBLES TO HANDLE THE PROCESS FLOW
   -- < CHANGE >

   V_FLAG               VARCHAR (5);
   V_REC_COUNT          NUMBER := 0;
   V_REC_INS_COUNT      NUMBER := 0;
   V_FLAG_DATE_TIME     DATE;
   V_FLAG_COUNT         NUMBER;
   V_RES_COUNT          NUMBER;
   V_IHN_LEVEL3_DESC_VAL  VARCHAR (500);
   VALGNAME             VARCHAR (200);
   V_ALG_DFN_SK         NUMBER;


   -- ALGORITHM PARAMETER VARAIBLES TO HANDLE THE PROCESS FLOW
   -- < CHANGE >
   
   V_IHN_LEVEL3_DESC    	 VARCHAR2(200);   
   V_FLAGGED_PL            	 VARCHAR2 (10);
   V_FLAGGED_EXP_CODE      	 VARCHAR2 (10);




   -- </ CHANGE >

   vcFlag               INTEGER := 0;
   
   
   
   
   --Create a table to store FLAG_LIST in -- use BULK COLLECT so operation only has to be carried out once (instead of cursor loop)
   TYPE TAB IS RECORD
   (
      DEVICEID                  NUMBER,
      MODULESNDRM               VARCHAR (10)
   );

   TYPE TBL IS TABLE OF TAB;

   FLG_TBL                   TBL;



   -- Cursor to identify all instruments available in IDA during batch (taken from PHM_ODS_RESULTS_CC)
   CURSOR DEVICE_SN_LIST
   IS
        SELECT IA.DEVICEID,
               UPPER (IA.SYSTEMSN) SERIAL_NUM,
               MAX (IL.PL) PL,
               MAX (IL.CUSTOMER_NUM) CUSTOMER_NUMBER,
               MAX (IL.CUSTOMER) CUSTOMER_NAME,
               MAX (PC.COUNTRY) COUNTRY_NAME,
               MAX (PC.AREAREGION) AREA,
               MAX (IL.CITY) CITY,
               COUNT (*) DEVICE_SN_CNT
          FROM SVC_PHM_ODS.PHM_ODS_CI_SCM_INSTACTIVITY IA,
               INSTRUMENTLISTING IL,
               PHM_COUNTRY PC
         WHERE     IA.BATCH_NUM = V_BATCH_NUM
               AND IA.RUN_DATE = V_RUN_DATE
               AND UPPER (IA.SYSTEMSN) = UPPER (IL.SN)
               AND PC.COUNTRY_CODE = IL.COUNTRY_CODE
               AND IL.PL = '214'  -- PL for Alinity i
      GROUP BY IA.DEVICEID, IA.SYSTEMSN;
      
      
      V_EXISTING_REC_CNT        NUMBER;
BEGIN

                  
   -- STEP 1   :PURPOSE TO GET PROCESSID OF CURRENT EXECUTION

   V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID ();
   --V_PROCESS_ID := -1;
   V_PROCESS_STATUS := 'STARTED';
   

   /*
   DBMS_OUTPUT.PUT_LINE (
         'PHM_CC_CUVETTE_PROC EXECUTION STARTED FOR : V_BATCH_NUM: '
      || V_BATCH_NUM
      || ', V_RUN_DATE: '
      || V_RUN_DATE);
   */

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

/*
   DBMS_OUTPUT.PUT_LINE (
         'VALGNAME: '
      || VALGNAME
      || ', V_PROCESS_TYPE: '
      || V_PROCESS_TYPE
      || ', V_RUN_MODE: '
      || V_RUN_MODE
      || ', V_ROUTINE_NAME: '
      || V_ROUTINE_NAME
      || ', V_PROD_FAMILY: '
      || V_PROD_FAMILY);

*/

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

   --DBMS_OUTPUT.PUT_LINE ('V_ALG_DFN_SK: ' || V_ALG_DFN_SK);

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

   --DBMS_OUTPUT.PUT_LINE ( 'V_PROCESS_ID: '|| V_PROCESS_ID || ', VALGNAME: ' || VALGNAME || ', V_ROUTINE_NAME: '|| V_ROUTINE_NAME);

   --  STEP 3 : PURPOSE - TO GET THE ALL THE PARAMETERS THAT WERE DEFINED IN THE ALGORITHM SCREEN

   FOR I IN (SELECT PARAMETER_VALUES, PARAMETER_NAME, PHM_PATTERNS_SK
               FROM PHM_THRESHOLD_PARAMETER
              WHERE PHM_PATTERNS_SK = V_ALG_NUM ) --AND DELETE_FLAG IS NULL)
   LOOP
      -- PURPOSE - IN CASE OF NEW PERAMETER DEFEINED IN ALGORITHM DEFINITION -  WRITE CODE WITH A NEW IF CONDITION TO GET NEW PARAMETER VALUE
      -- <CHANGE >
      
      IF I.PARAMETER_NAME = 'IHN_LEVEL3_DESC'
      THEN
         V_IHN_LEVEL3_DESC := I.PARAMETER_VALUES;
      END IF;  
 
 /*
      IF I.PARAMETER_NAME = 'MIN_VALUE'
      THEN
         V_ONEDAYGROUP := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;

      IF I.PARAMETER_NAME = 'MAX_VALUE'
      THEN
         V_SIXDAYGROUP := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;
*/
      
   -- < CHANGE>
   END LOOP;

   --DBMS_OUTPUT.PUT_LINE ( 'V_IHN_LEVEL3_DESC= ' || V_IHN_LEVEL3_DESC || ', V_INTEGRATEDVacuum_Sensor_MAX= '|| V_INTEGRATEDVacuum_Sensor_MAX || ', V_INTEGRATEDVacuum_Sensor_SD= ' || V_INTEGRATEDVacuum_Sensor_SD);

   SELECT *
     BULK COLLECT INTO FLG_TBL
     FROM (SELECT DEVICEID, Instrument
FROM
       (SELECT DEVICEID, Instrument,
              (CASE WHEN Num_Recover > 0 THEN Num_Recover / Num_RSM_Move
                     ELSE 0 END) AS Frac_Recover,
              (CASE WHEN Num_Engage  > 0 THEN Num_Engage  / Num_RSM_Move
                     ELSE 0 END) AS Frac_Engage,
              (CASE WHEN Num_Recover > 0 THEN Num_Recover / Num_Days
                     ELSE 0 END) AS PerDay_Recover,
              (CASE WHEN Num_Engage  > 0 THEN Num_Engage  / Num_Days
                     ELSE 0 END) AS PerDay_Engage
       FROM
              (SELECT DEVICEID, Instrument, COUNT(Day) AS Num_Days,
                     SUM(Num_Retry - 2*Num_Exceed) AS Num_Recover,
                     SUM(Num_Engage) AS Num_Engage,
                     SUM(Num_Scans + Num_Retry - Num_Exceed) AS Num_RSM_Move
              FROM
                     (SELECT TRUNC(LOGDATE_LOCAL) AS Day, DEVICEID, SYSTEMSN AS Instrument,
                           SUM(CASE WHEN COMPONENT = 'CarrierScheduler: CarrierScanned'
                                  THEN 1 ELSE 0 END) AS Num_Scans,
                           SUM(CASE WHEN COMPONENT LIKE '%Load%Pick%' AND ACTIVITY LIKE 'Retry%'
                                  THEN 1 ELSE 0 END) AS Num_Retry,
                           SUM(CASE WHEN COMPONENT LIKE '%Load%Pick%' AND ACTIVITY LIKE 'Exceed%'
                                  THEN 1 ELSE 0 END) AS Num_Exceed,
                           SUM(CASE WHEN COMPONENT LIKE '%Load%Pick%' AND ACTIVITY LIKE '%engagement%'
                                  THEN 1 ELSE 0 END) AS Num_Engage
                     FROM SVC_PHM_ODS.PHM_ODS_CI_SCM_INSTACTIVITY --IDAQOWNER.ICQ_INSTRUMENTACTIVITY
                     WHERE TRUNC(LOGDATE_LOCAL) >= TRUNC(SYSDATE) - 7 AND
                           TRUNC(LOGDATE_LOCAL) <= TRUNC(SYSDATE) - 1 AND
                           SYSTEMSN LIKE 'SCM%'
                     GROUP BY  TRUNC(LOGDATE_LOCAL), DEVICEID, SYSTEMSN
                     )
              GROUP BY  DEVICEID, Instrument
              )
       )
	WHERE 2.3 * Frac_Recover + 2.6 * Frac_Engage + 0.68 * PerDay_Recover + 0.85 * PerDay_Engage >= 3.97
	ORDER BY DEVICEID, Instrument

	  );
                  

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

      --DBMS_OUTPUT.PUT_LINE('EXISTING RECORD COUNT IN PHM_ALG_OUTPUT: ' || V_EXISTING_REC_CNT);
      IF V_EXISTING_REC_CNT > 0
      THEN
         DELETE FROM PHM_ALG_OUTPUT
               WHERE     BATCH_NUM = V_BATCH_NUM
                     AND RUN_DATE = V_RUN_DATE
                     AND PHM_PATTERNS_SK = V_ALG_NUM;

         COMMIT;
      END IF;

      --DBMS_OUTPUT.PUT_LINE('TOTAL RECORDS DELETED FROM PHM_ALG_OUTPUT: ' || V_EXISTING_REC_CNT);


      -- STEP 5b : CHECK DATA EXISTS FOR BATCH AND RUN DATE IN THE ALGORITHM CHART OUTPUT  TABLE   , IF DATA EXISTS DELETE THE DATA FROM OUTPUT TABLE
      SELECT COUNT (*)
        INTO V_EXISTING_REC_CNT
        FROM PHM_ALG_CHART_OUTPUT
       WHERE     BATCH_NUM = V_BATCH_NUM
             AND RUN_DATE = V_RUN_DATE
             AND PHM_PATTERN_SK = V_ALG_NUM;                 --AND ROWNUM < 5;

      --DBMS_OUTPUT.PUT_LINE('EXISTING RECORD COUNT IN PHM_ALG_CHART_OUTPUT: ' || V_EXISTING_REC_CNT);
      IF V_EXISTING_REC_CNT > 0
      THEN
         DELETE FROM PHM_ALG_CHART_OUTPUT
               WHERE     BATCH_NUM = V_BATCH_NUM
                     AND RUN_DATE = V_RUN_DATE
                     AND PHM_PATTERN_SK = V_ALG_NUM;

         COMMIT;
      END IF;

      --DBMS_OUTPUT.PUT_LINE('TOTAL RECORDS DELETED FROM PHM_ALG_CHART_OUTPUT: ' || V_EXISTING_REC_CNT);

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

            FOR indx IN 1 .. FLG_TBL.COUNT
            LOOP
               IF FLG_TBL (indx).MODULESNDRM = DL.SERIAL_NUM
               THEN

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
               END IF;
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
                     ' PHM_ICQ_RSM_Pick_Load_PROC EXECUTION HAS FAILED FOR '
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
      --DBMS_OUTPUT.PUT_LINE('PHM_ICQ_RSM_Pick_Load_PROC Execution COMPLETED Successfully. Total records inserted: ' || V_REC_INS_COUNT || ', Flagged Count: ' || V_FLAG_COUNT);

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
            'ALGORITHM EXECUTION FAILED FOR PHM_ICQ_RSM_Pick_Load_PROC, DUE TO: '
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


END PHM_ICQ_RSM_Pick_Load_PROC;
/