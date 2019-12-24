/* Formatted on 8/5/2019 9:05:17 AM (QP5 v5.269.14213.34769) */
CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_A3600_1 (
   V_ALG_NUM      NUMBER,
   V_RUN_DATE     DATE,
   V_BATCH_NUM    VARCHAR2,
   V_UNIX_ID      VARCHAR2)
AS
   --  AUDIT LOG VARIABLE

   V_PROCESS_TYPE       VARCHAR2 (250);
   V_PROCESS_STATUS     VARCHAR2 (25) := 'STARTED';
   V_PROCESS_ID         NUMBER (15);
   V_PROD_FAMILY        VARCHAR2 (250);
   V_RUN_MODE           VARCHAR2 (10);
   V_ROUTINE_NAME       VARCHAR (350);
   V_ROUTINE_TYPE       VARCHAR (350);
   V_ERROR_MESSAGE      VARCHAR (4000);


   -- ALGORITHM LOCAL VARAIBLES TO HANDLE THE PROCESS FLOW
   -- < CHANGE >

   V_EXISTING_REC_CNT   NUMBER;
   V_FLAG               VARCHAR (5);
   V_REC_COUNT          NUMBER := 0;
   V_REC_INS_COUNT      NUMBER := 0;
   V_FLAG_DATE_TIME     DATE;
   V_RES_COUNT          NUMBER;
   V_FLAGGED_PL         VARCHAR2 (10);
   V_FLAGGED_EXP_CODE   VARCHAR2 (10);

   VALGNAME             VARCHAR (500);
   V_ALG_DFN_SK         NUMBER;


   VALGEXEC_SEQ         VARCHAR2 (20);
   V_PREV_DATE          DATE;
   VSTARTDATE           DATE;
   VENDDATE             DATE;
   V_DATE_30TH          DATE;
   COUNT_FLAG           NUMBER (10);
   V_REQ_START_DATE     DATE;

   CURR_DAY             DATE;
   PREV_DAY             DATE;
   CONSECUTIVE_DAYS     BOOLEAN;
   CURR_DAY_ERRCOUNT    NUMBER;
   FLAGGING_DAYS        NUMBER;
   TOTAL_ERROR_COUNT    NUMBER;


   PREV_DAY_ERRCOUNT    NUMBER (10) := 0;
   TODAY_ERRCOUNT       NUMBER (10) := 0;

   PREV_DAY_ERRORPCT    NUMBER (10, 5);
   TODAY_ERRORPCT       NUMBER (10, 5);

   TODAY_TEST_COUNT     NUMBER (10) := 0;
   FILTER_COUNT         NUMBER (10) := 0;
   V_THRESHOLD_LIMIT    NUMBER (15, 8) := 100000;

   CONSEQ_COUNT         NUMBER (2) := 0;
   FLAG                 VARCHAR (25) := 'NO';
   IHN_VALUE            VARCHAR (100) := '';
   V_RERUN_PREV_DATE    DATE;



   V_CUSTOMERNAME       VARCHAR (100);
   V_CUSTOMERNUMBER     VARCHAR (100);
   V_DEVICEID           VARCHAR (100);
   V_SYSTEMSN           VARCHAR (100);
   V_COUNTRYNAME        VARCHAR (100);
   V_AREAREGION         VARCHAR (100);

   V_SAMP_ID_CHK1       VARCHAR2 (10) := ' ';
   V_SAMP_ID_CHK2       VARCHAR2 (10) := '%';
   V_INSERT_COUNT       NUMBER (25);


   VPRODLINE            PHM_INSTRUMENT_FAMILY_ASSOC.PRODUCTLINE%TYPE;

   CURSOR ALL_THRESHOLDS (
      vn_ALG_NUM    NUMBER)
   IS
        SELECT phm_patterns_sk PHM_THRESHOLDS_SK,
               CASE
                  WHEN THRESHOLD_NUMBER IS NULL THEN 9999
                  ELSE TO_NUMBER (THRESHOLD_NUMBER)
               END
                  THRESHOLD_NUMBER,
               CASE
                  WHEN THRESHOLD_NUMBER_UNIT IS NULL THEN 9999
                  ELSE TO_NUMBER (THRESHOLD_NUMBER_UNIT)
               END
                  THRESHOLD_NUMBER_UNIT,
               THRESHOLD_NUMBER_DESC,
               phm_patterns_sk PHM_PATTERNS_SK,
               PHM_THRESHOLDS_SK AS THRESHOLDS_SK_VAL,
               PATTERN_DESCRIPTION,
               THRESHOLD_ALERT,
               ALGORITHM_TYPE AS ALGORITHM_TYPE,
               CASE
                  WHEN THRESHOLD_DATA_DAYS IS NULL THEN 9999
                  ELSE TO_NUMBER (THRESHOLD_DATA_DAYS)
               END
                  THRESHOLD_DATA_DAYS,
               CASE WHEN MODULE_TYPE = 'ALL' THEN '%' ELSE MODULE_TYPE END
                  MODULE_TYPE
          FROM (SELECT tp.phm_patterns_sk,
                       p.pattern_name AS pattern_name,
                       thr.phm_thresholds_sk,
                       ihn.issue_description,
                       tp.parameter_name,
                       tp.parameter_values
                  FROM phm_threshold_parameter tp,
                       phm_patterns p,
                       (SELECT phm_patterns_sk, issue_description
                          FROM phm_algorithm_ihns pai
                         WHERE pai.phm_algorithm_definitions_sk = vn_ALG_NUM)
                       ihn,
                       (SELECT phm_patterns_sk, phm_thresholds_sk
                          FROM phm_thresholds pt
                         WHERE pt.phm_algorithm_definitions_sk = vn_ALG_NUM)
                       thr
                 WHERE     tp.phm_patterns_sk = p.phm_patterns_sk
                       AND NVL (tp.delete_flag, 'N') <> 'Y'
                       AND tp.phm_d_algorithm_definitions_sk = vn_ALG_NUM
                       AND p.phm_patterns_sk = ihn.phm_patterns_sk
                       AND p.phm_patterns_sk = thr.phm_patterns_sk) PIVOT (MAX (
                                                                              parameter_values)
                                                                       FOR parameter_name
                                                                       IN ('ALGORITHM_TYPE' AS ALGORITHM_TYPE,
                                                                          'ERROR_CODE_VALUE' AS PATTERN_DESCRIPTION,
                                                                          'ERROR_COUNT' AS THRESHOLD_NUMBER,
                                                                          'IHN_LEVEL3_DESC' AS THRESHOLD_ALERT,
                                                                          'THRESHOLD_DATA_DAYS' AS THRESHOLD_DATA_DAYS,
                                                                          'THRESHOLDS_DAYS' AS THRESHOLD_NUMBER_UNIT,
                                                                          'THRESHOLD_DESCRIPTION' AS THRESHOLD_NUMBER_DESC,
                                                                          'MODULE' AS MODULE_TYPE))
      ORDER BY algorithm_type, pattern_name;



   CURSOR THRESHOLD_COUNTS (
      V_SN1             VARCHAR2,
      V_NODETYPE1       VARCHAR2,
      V_ERRORCODE1      VARCHAR2,
      V_START_DATE      DATE,
      V_END_DATE        DATE,
      V_DATA_DAYS       NUMBER,
      V_SAMP_ID_CHK1    VARCHAR2,
      V_SAMP_ID_CHK2    VARCHAR2)
   IS
        SELECT ASI.DEVICEID,
               ASI.SYSTEMSN,
               N.PL,
               N.SN,
               TRUNC (COMPLETIONDATE) FLAG_DATE,
               AE.NODETYPE,
               AE.ERRORCODE,
               AE.NODEID,
               AE.INSTANCEID,
               AC.TUBES_TODAY,
               MAX (AE.COMPLETIONDATE) MAX_COMPL_DATE,
               COUNT (AE.ERRORCODE) ERROR_COUNT,
               TRUNC ( (COUNT (AE.ERRORCODE) * 100 / AC.TUBES_TODAY), 2)
                  ERROR_PERCENTAGE
          FROM SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
               A3600_LAYOUT_NODES_PL_SN N,
               IDAOWNER.A3600SYSTEMINFORMATION ASI,
               IDAOWNER.A3600_COUNTERS AC
         WHERE     AE.LAYOUT_NODES_ID = N.LAYOUT_NODES_ID
               AND N.CANID = AE.NODEID
               AND N.NODETYPE = AE.NODETYPE
               AND LOWER (N.SN) = LOWER (V_SN1)
               AND ASI.SYSTEMINFOID = N.SYSTEMINFOID
               AND AC.LAYOUT_NODES_ID = N.LAYOUT_NODES_ID
               AND AC.NODETYPE = AE.NODETYPE
               AND AC.COUNTER_DATE = TRUNC (AE.COMPLETIONDATE)
               AND AC.NODEID = AE.NODEID
               AND AC.INSTANCEID = AE.INSTANCEID
               AND N.SYSTEMINFOID = ASI.SYSTEMINFOID
               AND ASI.CURRENT_ROW = 'Y'
               AND AC.TUBES_TODAY <> 0
               AND (   (V_NODETYPE1 != '%' AND AE.NODETYPE = V_NODETYPE1)
                    OR (V_NODETYPE1 = '%' AND AE.NODETYPE LIKE V_NODETYPE1))
               AND AE.ERRORCODE = V_ERRORCODE1
               AND NVL (AE.SAMPLEID, V_SAMP_ID_CHK1) LIKE
                      NVL (V_SAMP_ID_CHK2, AE.SAMPLEID)
               AND AE.COMPLETIONDATE BETWEEN V_START_DATE - V_DATA_DAYS + 1
                                         AND V_END_DATE
      GROUP BY ASI.DEVICEID,
               ASI.SYSTEMSN,
               N.PL,
               N.SN,
               TRUNC (COMPLETIONDATE),
               AE.NODETYPE,
               AE.ERRORCODE,
               AE.NODEID,
               AE.INSTANCEID,
               AC.TUBES_TODAY;



   CURSOR DEVICE_AND_DATES (
      V_NODETYPE1     VARCHAR2,
      V_ERRORCODE1    VARCHAR2)
   IS
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
               AND AE.ERRORCODE = V_ERRORCODE1
               AND AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
               AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
               AND ALN.SN IS NOT NULL
               AND ALN.CANID = AE.NODEID
               AND ASI.CURRENT_ROW = 'Y'
               AND (   (V_NODETYPE1 != '%' AND AE.NODETYPE = V_NODETYPE1)
                    OR (V_NODETYPE1 = '%' AND AE.NODETYPE LIKE V_NODETYPE1))
      GROUP BY ASI.PRODUCTLINEREF,
               ASI.DEVICEID,
               ASI.SYSTEMSN,
               ALN.SN,
               AE.NODETYPE,
               AE.ERRORCODE
      ORDER BY ASI.SYSTEMSN, AE.NODETYPE, AE.ERRORCODE;
BEGIN
   PHM_ALGORITHM_UTILITIES_1.PHM_GET_ALG_DETAILS (V_ALG_NUM,
                                                  VALGNAME,
                                                  V_PROCESS_TYPE,
                                                  V_ROUTINE_NAME,
                                                  V_RUN_MODE,
                                                  V_PROD_FAMILY);
   V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID ();



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



   BEGIN
      DELETE FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
            WHERE    (BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE)
                  OR DATE_CREATED < SYSDATE - 35;

      DELETE FROM PHM_ALG_OUTPUT
            WHERE     PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM
                  AND BATCH_NUM = V_BATCH_NUM;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         V_PROCESS_STATUS := 'ERRORED';
         V_ERROR_MESSAGE :=
               'NOT ABLE TO DELETE THE DATA OF PREVIOUS RUN FOR RUN_DATE '
            || V_RUN_DATE
            || ' FOR BATCH_NUM '
            || V_BATCH_NUM
            || ' DUE TO  : '
            || SQLERRM;
         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,
                                                         V_ALG_NUM,
                                                         V_RUN_DATE,
                                                         V_BATCH_NUM,
                                                         V_ERROR_MESSAGE,
                                                         VALGNAME);
   END;


   FOR TH IN ALL_THRESHOLDS (V_ALG_NUM)
   LOOP
      IF    (TH.PATTERN_DESCRIPTION = '0405' AND TH.MODULE_TYPE = 'IOM')
         OR (TH.PATTERN_DESCRIPTION = '0605' AND TH.MODULE_TYPE = 'CM')
      THEN
         V_SAMP_ID_CHK1 := NULL;
         V_SAMP_ID_CHK2 := 'amp;U__';
      ELSE
         IF TH.PATTERN_DESCRIPTION = '5015' AND TH.MODULE_TYPE = 'ISR'
         THEN
            V_SAMP_ID_CHK1 := NULL;
            V_SAMP_ID_CHK2 := '%';
         ELSE
            V_SAMP_ID_CHK1 := ' ';
            V_SAMP_ID_CHK2 := '%';
         END IF;
      END IF;



      FOR DD IN DEVICE_AND_DATES (TH.MODULE_TYPE, TH.PATTERN_DESCRIPTION)
      LOOP
         FOR TE IN THRESHOLD_COUNTS (DD.SN,
                                     TH.MODULE_TYPE,
                                     TH.PATTERN_DESCRIPTION,
                                     DD.MIN_COMPL_DATE,
                                     DD.MAX_COMPL_DATE,
                                     TH.THRESHOLD_NUMBER_UNIT,
                                     V_SAMP_ID_CHK1,
                                     V_SAMP_ID_CHK2)
         LOOP
            BEGIN
               IF (TE.PL IS NOT NULL AND TE.SN IS NOT NULL)
               THEN
                  INSERT INTO SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                       VALUES (V_PROD_FAMILY,
                               V_RUN_DATE,
                               V_BATCH_NUM,
                               V_PROCESS_ID,
                               V_ALG_NUM,
                               TH.PHM_THRESHOLDS_SK,
                               TE.DEVICEID,
                               TE.SN,
                               TE.NODETYPE,
                               TE.ERRORCODE,
                               TE.NODEID,
                               TE.INSTANCEID,
                               TE.FLAG_DATE,
                               TE.TUBES_TODAY,
                               TE.ERROR_COUNT,
                               TE.ERROR_PERCENTAGE,
                               TE.MAX_COMPL_DATE,
                               0,
                               VALGNAME,
                               SYSDATE,
                               V_UNIX_ID,
                               TE.PL);

                  V_INSERT_COUNT := V_INSERT_COUNT + 1;

                  IF MOD (V_INSERT_COUNT, 10000) = 0
                  THEN
                     COMMIT;
                  END IF;
               END IF;

               COMMIT;
            EXCEPTION
               WHEN OTHERS
               THEN
                  V_ERROR_MESSAGE :=
                        ' INSERTING DATA INTO PHM_A3600_TEMP_DATA HAS FAILED FOR '
                     || V_ALG_NUM
                     || ' FOR '
                     || TE.DEVICEID
                     || ' FOR DATE '
                     || TE.FLAG_DATE
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
            END;
         END LOOP;
      END LOOP;



      -- Healthy Instruments
      FOR DD
         IN (  SELECT ALN.PL,
                      ALN.SN,
                      ASI.DEVICEID,
                      MAX (AE.COMPLETIONDATE) MAX_COMPL_DATE,
                      TRUNC (MIN (AE.COMPLETIONDATE)) MIN_COMPL_DATE
                 FROM SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
                      A3600_LAYOUT_NODES_PL_SN ALN,
                      IDAOWNER.A3600SYSTEMINFORMATION ASI
                WHERE     AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
                      AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
                      AND ALN.CANID = AE.NODEID
                      AND ASI.CURRENT_ROW = 'Y'
                      AND ALN.PL IS NOT NULL
                      AND ALN.SN IS NOT NULL
                      AND BATCH_NUM = V_BATCH_NUM
                      AND RUN_DATE = V_RUN_DATE
             GROUP BY ALN.PL, ALN.SN, ASI.DEVICEID)
      LOOP
         FOR AEC
            IN (SELECT DISTINCT ASI.DEVICEID,
                                ALN.SN,
                                AC.COUNTER_DATE,
                                AC.NODETYPE,
                                AC.NODEID,
                                AC.INSTANCEID
                  FROM A3600_LAYOUT_NODES_PL_SN ALN,
                       IDAOWNER.A3600SYSTEMINFORMATION ASI,
                       IDAOWNER.A3600_COUNTERS AC
                 WHERE     ALN.SN = DD.SN
                       AND AC.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
                       AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
                       AND ASI.CURRENT_ROW = 'Y'    -- AND AC.TUBES_TODAY <> 0
                       AND AC.COUNTER_DATE BETWEEN   DD.MIN_COMPL_DATE
                                                   - TH.THRESHOLD_NUMBER_UNIT
                                               AND DD.MAX_COMPL_DATE)
         LOOP
            SELECT COUNT (1)
              INTO COUNT_FLAG
              FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
             WHERE     FLAG_DATE = TRUNC (AEC.COUNTER_DATE)
                   AND MODULE_SN = UPPER (AEC.SN)
                   AND BATCH_NUM = V_BATCH_NUM
                   AND RUN_DATE = V_RUN_DATE;

            IF COUNT_FLAG = 0
            THEN
               BEGIN
                  INSERT INTO SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                          VALUES (
                                    V_PROD_FAMILY,
                                    V_RUN_DATE,
                                    V_BATCH_NUM,
                                    V_PROCESS_ID,
                                    V_ALG_NUM,
                                    TH.PHM_THRESHOLDS_SK,
                                    AEC.DEVICEID,
                                    AEC.SN,
                                    AEC.NODETYPE,
                                    TH.PATTERN_DESCRIPTION,
                                    AEC.NODEID,
                                    AEC.INSTANCEID,
                                    AEC.COUNTER_DATE,
                                    0,
                                    0,
                                    0,
                                    TO_DATE (
                                          TO_CHAR (AEC.COUNTER_DATE,
                                                   'DDMMYYYY')
                                       || '000001',
                                       'DDMMYYYYHH24MISS'),
                                    0,
                                    'A3600-SYS',
                                    SYSDATE,
                                    V_UNIX_ID,
                                    DD.PL);

                  V_INSERT_COUNT := V_INSERT_COUNT + 1;

                  IF MOD (V_INSERT_COUNT, 10000) = 0
                  THEN
                     COMMIT;
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     V_ERROR_MESSAGE :=
                           ' INSERTING DATA FOR ZERO COUNT PHM_A3600_TEMP_DATA HAS FAILED FOR '
                        || V_ALG_NUM
                        || ' FOR '
                        || AEC.SN
                        || ' FOR DATE '
                        || AEC.COUNTER_DATE
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
               END;
            END IF;

            COMMIT;
         END LOOP;

         COMMIT;
      END LOOP;
   END LOOP;                                          --ALL_THRESHOLDS CURSORS



   FOR Z IN ALL_THRESHOLDS (V_ALG_NUM)
   LOOP
      DBMS_OUTPUT.PUT_LINE (
            'ALGORITHM_TYPE ='
         || Z.ALGORITHM_TYPE
         || ', Z.PATTERN_DESCRIPTION='
         || Z.PATTERN_DESCRIPTION);

      --Error type
      IF Z.ALGORITHM_TYPE = 'ERROR_COUNT'
      THEN
         DBMS_OUTPUT.PUT_LINE (
               'Z.PATTERN_DESCRIPTION ='
            || '  '
            || Z.PATTERN_DESCRIPTION
            || ',Z.PHM_THRESHOLDS_SK='
            || Z.PHM_THRESHOLDS_SK
            || ', V_BATCH_NUM='
            || V_BATCH_NUM
            || ',V_RUN_DATE='
            || V_RUN_DATE
            || ',Z.THRESHOLD_NUMBER='
            || Z.THRESHOLD_NUMBER
            || ',Z.THRESHOLD_NUMBER_UNIT='
            || Z.THRESHOLD_NUMBER_UNIT);

         FOR Y
            IN (SELECT DISTINCT E.MODULE_SN,
                                E.DEVICE_ID,
                                I.CUSTOMER_NAME CUSTOMERNAME,
                                I.CUSTOMER_NUMBER CUSTOMERNUMBER,
                                I.CITY,
                                I.COUNTRY_CODE COUNTRYCODE,
                                PC.COUNTRY COUNTRYNAME,
                                PC.AREAREGION AREA,
                                PC.AREAREGION,
                                E.PL
                  FROM (  SELECT MAX (SN) SN,
                                 MAX (PL) PL,
                                 MAX (CUSTOMER_NUM) CUSTOMER_NUMBER,
                                 MAX (CUSTOMER) CUSTOMER_NAME,
                                 MAX (CITY) City,
                                 MAX (COUNTRY_CODE) COUNTRY_CODE
                            FROM INSTRUMENTLISTING
                        GROUP BY sn) I,
                       SVC_PHM_ODS.PHM_A3600_TEMP_ERROR E,
                       PHM_COUNTRY PC
                 WHERE     UPPER (I.SN) = E.MODULE_SN
                       AND E.BATCH_NUM = V_BATCH_NUM
                       AND E.RUN_DATE = V_RUN_DATE
                       AND I.PL = E.PL
                       AND PC.COUNTRY_CODE = I.COUNTRY_CODE
                       AND E.PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK)
         LOOP
            FOR P
               IN (SELECT DISTINCT MODULE_SN,
                                   PHM_ALGORITHM_DEFINITIONS_SK,
                                   NODETYPE,
                                   ERRORCODE,
                                   NODEID,
                                   INSTANCEID
                     FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                    WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                          AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                          AND BATCH_NUM = V_BATCH_NUM
                          AND RUN_DATE = V_RUN_DATE)
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


               FOR X
                  IN (  SELECT *
                          FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                         WHERE     MODULE_SN = UPPER (p.MODULE_SN)
                               AND ERRORCODE = P.ERRORCODE
                               AND BATCH_NUM = V_BATCH_NUM
                               AND RUN_DATE = V_RUN_DATE
                               AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                               AND NODETYPE = P.NODETYPE
                               AND INSTANCEID = P.INSTANCEID
                               AND NODEID = P.NODEID
                      ORDER BY FLAG_DATE)
               LOOP
                  BEGIN
                     FLAG := 'NO';
                     IHN_VALUE := NULL;
                     V_FLAGGED_PL := NULL;
                     V_FLAGGED_EXP_CODE := NULL;

                     CURR_DAY := X.FLAG_DATE;
                     CURR_DAY_ERRCOUNT := X.ERRORCOUNT;

                     -- check if next date in the cursor is one less then the previous day? - means consecutive or not
                     IF (PREV_DAY IS NOT NULL AND CURR_DAY <> (PREV_DAY + 1))
                     THEN
                        CONSECUTIVE_DAYS := FALSE;
                     END IF;

                     IF (    CONSECUTIVE_DAYS = TRUE
                         AND (CURR_DAY_ERRCOUNT >= Z.THRESHOLD_NUMBER))
                     THEN
                        FLAGGING_DAYS := FLAGGING_DAYS + 1;
                        PREV_DAY := CURR_DAY;
                     END IF;

                     IF (CURR_DAY_ERRCOUNT < Z.THRESHOLD_NUMBER)
                     THEN
                        FLAGGING_DAYS := 0;
                        PREV_DAY := NULL;
                        CONSECUTIVE_DAYS := TRUE;
                     END IF;


                     IF (    FLAGGING_DAYS >= Z.THRESHOLD_NUMBER_UNIT
                         AND (CURR_DAY_ERRCOUNT >= Z.THRESHOLD_NUMBER))
                     THEN
                        FLAG := 'YES';
                        IHN_VALUE := Z.THRESHOLD_ALERT;


                        V_FLAGGED_PL := NULL;
                        V_FLAGGED_EXP_CODE := NULL;

                        PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE (
                           Z.PHM_THRESHOLDS_SK,
                           Y.PL,
                           NULL,
                           V_FLAGGED_PL,
                           V_FLAGGED_EXP_CODE);
                     END IF;

                     /*

                                DBMS_OUTPUT.PUT_LINE (

                                   'TYPE = COUNT, SN: '
                                   || Y.MODULE_SN
                                   || ', FLAG_DATE: '
                                   || X.FLAG_DATE
                                   || ', CURR_DAY: '
                                   || CURR_DAY
                                   || ', PREV_DAY: '
                                   || PREV_DAY
                                   || ', CONSECUTIVE_DAYS: '
                                   || (CASE
                                      WHEN (CONSECUTIVE_DAYS = TRUE)
                                      THEN
                                     'TRUE'
                                      ELSE
                                     'FALSE'
                                   END)
                                   || ', ERROR_COUNT: '
                                   || CURR_DAY_ERRCOUNT
                                   || ', FLAG: '
                                   || FLAG
                                   || ', IHN_VALUE: '
                                   || IHN_VALUE
                                   || ', V_FLAGGED_PL: '
                                   || V_FLAGGED_PL
                                   || ', V_FLAGGED_EXP_CODE: '
                                  || V_FLAGGED_EXP_CODE);

                     */

                     --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_INSERT
                     PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL (
                        Y.CUSTOMERNAME,
                        Y.CUSTOMERNUMBER,
                        Y.DEVICE_ID,
                        Y.MODULE_SN,
                        Y.COUNTRYNAME,
                        Y.AREAREGION,
                        V_ALG_NUM,
                        Z.THRESHOLDS_SK_VAL,
                        X.MAX_DATE_VALUE,
                        X.ERRORCOUNT,
                        FLAG,
                        IHN_VALUE,
                        TO_CHAR (P.NODEID) || ',' || TO_CHAR (P.INSTANCEID),
                        VALGNAME,
                        V_PROD_FAMILY,
                        V_BATCH_NUM,
                        Z.PHM_THRESHOLDS_SK,
                        V_RUN_DATE,
                        V_PROCESS_ID,
                        V_FLAGGED_PL,
                        V_FLAGGED_EXP_CODE);

                     V_INSERT_COUNT := V_INSERT_COUNT + 1;

                     IF MOD (V_INSERT_COUNT, 10000) = 0
                     THEN
                        COMMIT;
                     END IF;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        V_ERROR_MESSAGE :=
                              ' CALCULATION OF COUNT OF ERRORS HAS FAILED FOR '
                           || Z.PHM_THRESHOLDS_SK
                           || ' FOR '
                           || Y.MODULE_SN
                           || ' FOR DATE '
                           || X.FLAG_DATE
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
                  END;
               END LOOP;
            END LOOP;
         END LOOP;
      END IF;



      IF Z.ALGORITHM_TYPE = 'PERCENTAGE'
      THEN
         DBMS_OUTPUT.PUT_LINE ('PERCENTAGE=' || '  ' || Z.THRESHOLD_NUMBER);


         FOR Y
            IN (SELECT DISTINCT E.MODULE_SN,
                                E.DEVICE_ID,
                                I.CUSTOMER_NAME CUSTOMERNAME,
                                I.CUSTOMER_NUMBER CUSTOMERNUMBER,
                                I.CITY,
                                I.COUNTRY_CODE COUNTRYCODE,
                                PC.COUNTRY COUNTRYNAME,
                                PC.AREAREGION AREA,
                                PC.AREAREGION,
                                E.PL
                  FROM (  SELECT MAX (SN) SN,
                                 MAX (PL) PL,
                                 MAX (CUSTOMER_NUM) CUSTOMER_NUMBER,
                                 MAX (CUSTOMER) CUSTOMER_NAME,
                                 MAX (CITY) City,
                                 MAX (COUNTRY_CODE) COUNTRY_CODE
                            FROM INSTRUMENTLISTING
                        GROUP BY sn) I,
                       SVC_PHM_ODS.PHM_A3600_TEMP_ERROR E,
                       PHM_COUNTRY PC
                 WHERE     UPPER (I.SN) = E.MODULE_SN
                       AND E.BATCH_NUM = V_BATCH_NUM
                       AND E.RUN_DATE = V_RUN_DATE
                       AND I.PL = E.PL
                       AND PC.COUNTRY_CODE = I.COUNTRY_CODE
                       AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK)
         LOOP
            FOR P
               IN (SELECT DISTINCT MODULE_SN,
                                   PHM_THRESHOLDS_SK,
                                   NODETYPE,
                                   ERRORCODE,
                                   NODEID,
                                   INSTANCEID
                     FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                    WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                          AND BATCH_NUM = V_BATCH_NUM
                          AND RUN_DATE = V_RUN_DATE
                          AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK)
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



               FOR D
                  IN (  SELECT *
                          FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                         WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                               AND BATCH_NUM = V_BATCH_NUM
                               AND RUN_DATE = V_RUN_DATE
                               AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                               AND NODETYPE = P.NODETYPE
                               AND ERRORCODE = P.ERRORCODE
                               AND INSTANCEID = P.INSTANCEID
                               AND NODEID = P.NODEID
                      ORDER BY FLAG_DATE)
               LOOP
                  BEGIN
                     FLAG := 'NO';
                     IHN_VALUE := NULL;
                     V_FLAGGED_PL := NULL;
                     V_FLAGGED_EXP_CODE := NULL;


                     CURR_DAY := D.FLAG_DATE;
                     CURR_DAY_ERRCOUNT := D.ERRORPCT;

                     -- check if next date in the cursor is one less then the previous day? - means consecutive or not
                     IF (PREV_DAY IS NOT NULL AND CURR_DAY <> (PREV_DAY + 1))
                     THEN
                        CONSECUTIVE_DAYS := FALSE;
                     END IF;

                     IF (    CONSECUTIVE_DAYS = TRUE
                         AND (CURR_DAY_ERRCOUNT >= Z.THRESHOLD_NUMBER))
                     THEN
                        FLAGGING_DAYS := FLAGGING_DAYS + 1;
                     END IF;

                     PREV_DAY := CURR_DAY;


                     IF (CURR_DAY_ERRCOUNT < Z.THRESHOLD_NUMBER)
                     THEN
                        FLAGGING_DAYS := 0;
                        PREV_DAY := NULL;
                        CONSECUTIVE_DAYS := TRUE;
                     END IF;


                     IF (    FLAGGING_DAYS >= Z.THRESHOLD_NUMBER_UNIT
                         AND (CURR_DAY_ERRCOUNT >= Z.THRESHOLD_NUMBER))
                     THEN
                        FLAG := 'YES';
                        IHN_VALUE := Z.THRESHOLD_ALERT;


                        V_FLAGGED_PL := NULL;
                        V_FLAGGED_EXP_CODE := NULL;

                        PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE (
                           Z.PHM_THRESHOLDS_SK,
                           Y.PL,
                           NULL,
                           V_FLAGGED_PL,
                           V_FLAGGED_EXP_CODE);
                     END IF;

                     /*
                                                 DBMS_OUTPUT.PUT_LINE (

                                                    'TYPE=Percentage, SN: '
                                                    || Y.MODULE_SN
                                                    || ', FLAG_DATE: '
                                                    || D.FLAG_DATE
                                                    || ', CURR_DAY: '
                                                    || CURR_DAY
                                                    || ', PREV_DAY: '
                                                    || PREV_DAY
                                                    || ', CONSECUTIVE_DAYS: '
                                                    || (CASE
                                                           WHEN (CONSECUTIVE_DAYS = TRUE)
                                                           THEN
                                                              'TRUE'
                                                           ELSE
                                                              'FALSE'
                                                        END)
                                                    || ', ERROR_COUNT: '
                                                    || CURR_DAY_ERRCOUNT
                                                    || ', FLAG: '
                                                    || FLAG
                                                    || ', IHN_VALUE: '
                                                    || IHN_VALUE
                                                    || ', V_FLAGGED_PL: '
                                                    || V_FLAGGED_PL
                                                    || ', V_FLAGGED_EXP_CODE: '
                                                   || V_FLAGGED_EXP_CODE);
                     */

                     --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_INSERT
                     PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL (
                        Y.CUSTOMERNAME,
                        Y.CUSTOMERNUMBER,
                        Y.DEVICE_ID,
                        Y.MODULE_SN,
                        Y.COUNTRYNAME,
                        Y.AREAREGION,
                        V_ALG_NUM,
                        Z.THRESHOLDS_SK_VAL,
                        D.MAX_DATE_VALUE,
                        D.ERRORPCT,
                        FLAG,
                        IHN_VALUE,
                        TO_CHAR (P.NODEID) || ',' || TO_CHAR (P.INSTANCEID),
                        VALGNAME,
                        V_PROD_FAMILY,
                        V_BATCH_NUM,
                        Z.PHM_THRESHOLDS_SK,
                        V_RUN_DATE,
                        V_PROCESS_ID,
                        V_FLAGGED_PL,
                        V_FLAGGED_EXP_CODE);
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        V_ERROR_MESSAGE :=
                              ' PER CENT OF COUNT OF ERRORS HAS FAILED FOR '
                           || Z.PHM_THRESHOLDS_SK
                           || ' FOR '
                           || Y.MODULE_SN
                           || ' FOR DATE '
                           || D.FLAG_DATE
                           || ', ERROR :'
                           || SQLERRM;
                        V_PROCESS_STATUS := 'ERRORED';
                        TODAY_ERRORPCT := 0;
                        FLAG := 'NO';
                        IHN_VALUE := '';
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
                  END;


                  V_INSERT_COUNT := V_INSERT_COUNT + 1;

                  IF MOD (V_INSERT_COUNT, 10000) = 0
                  THEN
                     COMMIT;
                  END IF;
               END LOOP;
            END LOOP;
         END LOOP;
      END IF;



      IF Z.ALGORITHM_TYPE = 'SD_HIGH_VOLUME'
      THEN
         DBMS_OUTPUT.PUT_LINE ('SD_LOWVOLUME=' || '  ' || Z.THRESHOLD_NUMBER);

         FOR Y
            IN (SELECT DISTINCT E.MODULE_SN,
                                E.DEVICE_ID,
                                I.CUSTOMER_NAME CUSTOMERNAME,
                                I.CUSTOMER_NUMBER CUSTOMERNUMBER,
                                I.CITY,
                                I.COUNTRY_CODE COUNTRYCODE,
                                PC.COUNTRY COUNTRYNAME,
                                PC.AREAREGION AREA,
                                PC.AREAREGION,
                                E.PL
                  FROM (  SELECT MAX (SN) SN,
                                 MAX (PL) PL,
                                 MAX (CUSTOMER_NUM) CUSTOMER_NUMBER,
                                 MAX (CUSTOMER) CUSTOMER_NAME,
                                 MAX (CITY) City,
                                 MAX (COUNTRY_CODE) COUNTRY_CODE
                            FROM INSTRUMENTLISTING
                        GROUP BY sn) I,
                       SVC_PHM_ODS.PHM_A3600_TEMP_ERROR E,
                       PHM_COUNTRY PC
                 WHERE     UPPER (I.SN) = E.MODULE_SN
                       AND E.BATCH_NUM = V_BATCH_NUM
                       AND E.RUN_DATE = V_RUN_DATE
                       AND I.PL = E.PL
                       AND PC.COUNTRY_CODE = I.COUNTRY_CODE
                       AND E.PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM)
         LOOP
            FOR P
               IN (SELECT DISTINCT MODULE_SN,
                                   PHM_THRESHOLDS_SK,
                                   NODETYPE,
                                   ERRORCODE,
                                   NODEID,
                                   INSTANCEID
                     FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                    WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                          AND BATCH_NUM = V_BATCH_NUM
                          AND RUN_DATE = V_RUN_DATE
                          AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK)
            LOOP
               FLAG := 'NO';
               IHN_VALUE := '';
               TODAY_TEST_COUNT := 0;
               FILTER_COUNT := 0;
               TODAY_ERRORPCT := 0;
               PREV_DAY_ERRORPCT := 0;
               V_THRESHOLD_LIMIT := 100000;
               CONSEQ_COUNT := 0;

               SELECT TRUNC (MIN (AE.COMPLETIONDATE)) MIN_COMPL_DATE
                 INTO V_REQ_START_DATE
                 FROM SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
                      A3600_LAYOUT_NODES_PL_SN ALN,
                      IDAOWNER.A3600SYSTEMINFORMATION ASI
                WHERE     BATCH_NUM = V_BATCH_NUM
                      AND RUN_DATE = V_RUN_DATE
                      AND ALN.SN = Y.MODULE_SN
                      AND AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
                      AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
                      AND ALN.CANID = AE.NODEID
                      AND ASI.CURRENT_ROW = 'Y';


               FOR D
                  IN (  SELECT *
                          FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                         WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                               AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                               AND NODETYPE = P.NODETYPE
                               AND ERRORCODE = P.ERRORCODE
                               AND INSTANCEID = P.INSTANCEID
                               AND NODEID = P.NODEID
                               AND BATCH_NUM = V_BATCH_NUM
                               AND RUN_DATE = V_RUN_DATE
                      ORDER BY FLAG_DATE)
               LOOP
                  BEGIN
                     IF TRUNC (D.FLAG_DATE) >= V_REQ_START_DATE
                     THEN
                        SELECT TRUNC (
                                  ABS (AVG (TESTCOUNT) - STDDEV (TESTCOUNT)),
                                  0)
                          INTO FILTER_COUNT
                          FROM (  SELECT *
                                    FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                   WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                         AND PHM_THRESHOLDS_SK =
                                                Z.PHM_THRESHOLDS_SK
                                         AND NODETYPE = P.NODETYPE
                                         AND ERRORCODE = P.ERRORCODE
                                         AND INSTANCEID = P.INSTANCEID
                                         AND NODEID = P.NODEID
                                         AND BATCH_NUM = V_BATCH_NUM
                                         AND RUN_DATE = V_RUN_DATE
                                         --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                         AND FLAG_DATE <= TRUNC (D.FLAG_DATE)
                                ORDER BY FLAG_DATE DESC)
                         WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                        IF FILTER_COUNT > 0
                        THEN
                           SELECT MIN (FLAG_DATE)
                             INTO V_DATE_30TH
                             FROM (  SELECT *
                                       FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                      WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                            AND BATCH_NUM = V_BATCH_NUM
                                            AND RUN_DATE = V_RUN_DATE
                                            AND PHM_THRESHOLDS_SK =
                                                   Z.PHM_THRESHOLDS_SK
                                            AND NODETYPE = P.NODETYPE
                                            AND ERRORCODE = P.ERRORCODE
                                            AND INSTANCEID = P.INSTANCEID
                                            AND NODEID = P.NODEID
                                            --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                            AND FLAG_DATE <=
                                                   TRUNC (D.FLAG_DATE)
                                   ORDER BY FLAG_DATE DESC)
                            WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                           -- Get TODAY_TEST_COUNT
                           SELECT TESTCOUNT
                             INTO TODAY_TEST_COUNT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND FLAG_DATE = TRUNC (D.FLAG_DATE);

                           -- GET TODAY_ERRORPCT
                           SELECT NVL (ERRORPCT, 0)
                             INTO TODAY_ERRORPCT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND PHM_THRESHOLDS_SK = V_ALG_NUM
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND TRUNC (FLAG_DATE) = TRUNC (D.FLAG_DATE);

                           FLAG := 'NO';
                           IHN_VALUE := '';

                           IF TODAY_TEST_COUNT >= FILTER_COUNT
                           THEN
                              SELECT   STDDEV (ERRORPCT) * Z.THRESHOLD_NUMBER
                                     + AVG (ERRORPCT)
                                INTO V_THRESHOLD_LIMIT
                                FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                               WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                     AND BATCH_NUM = V_BATCH_NUM
                                     AND RUN_DATE = V_RUN_DATE
                                     AND PHM_THRESHOLDS_SK =
                                            Z.PHM_THRESHOLDS_SK
                                     AND NODETYPE = P.NODETYPE
                                     AND ERRORCODE = P.ERRORCODE
                                     AND INSTANCEID = P.INSTANCEID
                                     AND NODEID = P.NODEID
                                     AND TESTCOUNT > FILTER_COUNT
                                     AND FLAG_DATE BETWEEN V_DATE_30TH
                                                       AND TRUNC (
                                                              D.FLAG_DATE);

                              IF V_THRESHOLD_LIMIT > 0
                              THEN
                                 IF TODAY_ERRORPCT >= V_THRESHOLD_LIMIT
                                 THEN
                                    CONSEQ_COUNT := 0;
                                    FLAG := 'NO';
                                    IHN_VALUE := '';

                                    --DBMS_OUTPUT.PUT_LINE('in  '||'v_prev_date'||V_PREV_DATE||' Z.THRESHOLD_NUMBER_UNIT '|| Z.THRESHOLD_NUMBER_UNIT
                                    --||'   ' ||'d.flag_date '||TRUNC(D.FLAG_DATE));
                                    FOR I
                                       IN (  SELECT *
                                               FROM (  SELECT *
                                                         FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                                        WHERE     MODULE_SN =
                                                                     UPPER (
                                                                        Y.MODULE_SN)
                                                              AND BATCH_NUM =
                                                                     V_BATCH_NUM
                                                              AND RUN_DATE =
                                                                     V_RUN_DATE
                                                              AND PHM_THRESHOLDS_SK =
                                                                     Z.PHM_THRESHOLDS_SK
                                                              AND NODETYPE =
                                                                     P.NODETYPE
                                                              AND ERRORCODE =
                                                                     P.ERRORCODE
                                                              AND INSTANCEID =
                                                                     P.INSTANCEID
                                                              AND NODEID =
                                                                     P.NODEID
                                                              AND TESTCOUNT >
                                                                     FILTER_COUNT
                                                              AND FLAG_DATE <=
                                                                     TRUNC (
                                                                        D.FLAG_DATE)
                                                     ORDER BY FLAG_DATE DESC)
                                              WHERE ROWNUM <=
                                                       Z.THRESHOLD_NUMBER_UNIT
                                           ORDER BY FLAG_DATE DESC)
                                    LOOP
                                       IF I.ERRORPCT >= V_THRESHOLD_LIMIT
                                       THEN
                                          CONSEQ_COUNT := CONSEQ_COUNT + 1;
                                       ELSE
                                          CONSEQ_COUNT := 0;
                                       END IF;
                                    END LOOP;

                                    IF CONSEQ_COUNT >=
                                          Z.THRESHOLD_NUMBER_UNIT
                                    THEN
                                       FLAG := 'YES';
                                       IHN_VALUE := Z.THRESHOLD_ALERT;

                                       V_FLAGGED_PL := NULL;
                                       V_FLAGGED_EXP_CODE := NULL;

                                       PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE (
                                          V_ALG_NUM,
                                          Y.PL,
                                          NULL,
                                          V_FLAGGED_PL,
                                          V_FLAGGED_EXP_CODE);
                                    ELSE
                                       FLAG := 'NO';
                                       IHN_VALUE := '';
                                    END IF;
                                 ELSE
                                    FLAG := 'NO';
                                    IHN_VALUE := '';
                                 END IF;
                              ELSE
                                 FLAG := 'NO';
                                 IHN_VALUE := '';
                              END IF;
                           ELSE
                              FLAG := 'NO';
                              IHN_VALUE := '';
                           END IF;
                        ELSE
                           TODAY_ERRORPCT := 0;
                           FLAG := 'NO';
                           IHN_VALUE := NULL;
                        END IF;
                     ELSE
                        TODAY_ERRORPCT := 0;
                        FLAG := 'NO';
                        IHN_VALUE := NULL;
                     END IF;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        V_ERROR_MESSAGE :=
                              ' CALCULATION OF SD OF ERRORS HAS FAILED FOR '
                           || Z.PHM_THRESHOLDS_SK
                           || ' FOR '
                           || Y.MODULE_SN
                           || ' FOR DATE '
                           || D.FLAG_DATE
                           || ', ERROR :'
                           || SQLERRM;
                        V_PROCESS_STATUS := 'ERRORED';
                        TODAY_ERRORPCT := 0;
                        FLAG := 'NO';
                        IHN_VALUE := NULL;
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
                  END;

                  --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_INSERT
                  PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL (
                     Y.CUSTOMERNAME,
                     Y.CUSTOMERNUMBER,
                     Y.DEVICE_ID,
                     Y.MODULE_SN,
                     Y.COUNTRYNAME,
                     Y.AREAREGION,
                     V_ALG_NUM,
                     Z.THRESHOLDS_SK_VAL,
                     D.MAX_DATE_VALUE,
                     D.ERRORPCT,
                     FLAG,
                     IHN_VALUE,
                     TO_CHAR (P.NODEID) || ',' || TO_CHAR (P.INSTANCEID),
                     VALGNAME,
                     V_PROD_FAMILY,
                     V_BATCH_NUM,
                     Z.PHM_THRESHOLDS_SK,
                     V_RUN_DATE,
                     V_PROCESS_ID,
                     V_FLAGGED_PL,
                     V_FLAGGED_EXP_CODE);
                  V_INSERT_COUNT := V_INSERT_COUNT + 1;

                  IF MOD (V_INSERT_COUNT, 10000) = 0
                  THEN
                     COMMIT;
                  END IF;
               END LOOP;
            END LOOP;
         END LOOP;
      END IF;

      IF Z.ALGORITHM_TYPE = 'SD_LOW_VOLUME'
      THEN
         DBMS_OUTPUT.PUT_LINE (
            'SD_LOW_VOLUME=' || '  ' || Z.THRESHOLD_NUMBER);

         FOR Y
            IN (SELECT DISTINCT E.MODULE_SN,
                                E.DEVICE_ID,
                                I.CUSTOMER_NAME CUSTOMERNAME,
                                I.CUSTOMER_NUMBER CUSTOMERNUMBER,
                                I.CITY,
                                I.COUNTRY_CODE COUNTRYCODE,
                                PC.COUNTRY COUNTRYNAME,
                                PC.AREAREGION AREA,
                                PC.AREAREGION,
                                E.PL
                  FROM (  SELECT MAX (SN) SN,
                                 MAX (PL) PL,
                                 MAX (CUSTOMER_NUM) CUSTOMER_NUMBER,
                                 MAX (CUSTOMER) CUSTOMER_NAME,
                                 MAX (CITY) City,
                                 MAX (COUNTRY_CODE) COUNTRY_CODE
                            FROM INSTRUMENTLISTING
                        GROUP BY sn) I,
                       SVC_PHM_ODS.PHM_A3600_TEMP_ERROR E,
                       PHM_COUNTRY PC
                 WHERE     UPPER (I.SN) = E.MODULE_SN
                       AND I.PL = E.PL
                       AND PC.COUNTRY_CODE = I.COUNTRY_CODE
                       AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                       AND E.BATCH_NUM = V_BATCH_NUM
                       AND E.RUN_DATE = V_RUN_DATE)
         LOOP
            FOR P
               IN (SELECT DISTINCT MODULE_SN,
                                   PHM_THRESHOLDS_SK,
                                   NODETYPE,
                                   ERRORCODE,
                                   NODEID,
                                   INSTANCEID
                     FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                    WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                          AND BATCH_NUM = V_BATCH_NUM
                          AND RUN_DATE = V_RUN_DATE
                          AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK)
            LOOP
               FLAG := 'NO';
               IHN_VALUE := '';
               TODAY_TEST_COUNT := 0;
               FILTER_COUNT := 0;
               TODAY_ERRORPCT := 0;
               PREV_DAY_ERRORPCT := 0;
               V_THRESHOLD_LIMIT := 100000;
               CONSEQ_COUNT := 0;

               SELECT TRUNC (MIN (AE.COMPLETIONDATE)) MIN_COMPL_DATE
                 INTO V_REQ_START_DATE
                 FROM SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
                      A3600_LAYOUT_NODES_PL_SN ALN,
                      IDAOWNER.A3600SYSTEMINFORMATION ASI
                WHERE     AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
                      AND ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
                      AND ALN.CANID = AE.NODEID
                      AND ASI.CURRENT_ROW = 'Y'
                      AND BATCH_NUM = V_BATCH_NUM
                      AND RUN_DATE = V_RUN_DATE
                      AND ALN.SN = Y.MODULE_SN;


               FOR D
                  IN (  SELECT *
                          FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                         WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                               AND BATCH_NUM = V_BATCH_NUM
                               AND RUN_DATE = V_RUN_DATE
                               AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                               AND NODETYPE = P.NODETYPE
                               AND ERRORCODE = P.ERRORCODE
                               AND INSTANCEID = P.INSTANCEID
                               AND NODEID = P.NODEID
                      ORDER BY FLAG_DATE)
               LOOP
                  BEGIN
                     IF TRUNC (D.FLAG_DATE) >= V_REQ_START_DATE
                     THEN
                        SELECT TRUNC (
                                  ABS (AVG (TESTCOUNT) - STDDEV (TESTCOUNT)),
                                  0)
                          INTO FILTER_COUNT
                          FROM (  SELECT *
                                    FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                   WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                         AND BATCH_NUM = V_BATCH_NUM
                                         AND RUN_DATE = V_RUN_DATE
                                         AND PHM_THRESHOLDS_SK =
                                                Z.PHM_THRESHOLDS_SK
                                         AND NODETYPE = P.NODETYPE
                                         AND ERRORCODE = P.ERRORCODE
                                         AND INSTANCEID = P.INSTANCEID
                                         AND NODEID = P.NODEID
                                         --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                         AND FLAG_DATE <= TRUNC (D.FLAG_DATE)
                                ORDER BY FLAG_DATE DESC)
                         WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                        IF FILTER_COUNT > 0
                        THEN
                           SELECT MIN (FLAG_DATE)
                             INTO V_DATE_30TH
                             FROM (  SELECT *
                                       FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                      WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                            AND BATCH_NUM = V_BATCH_NUM
                                            AND RUN_DATE = V_RUN_DATE
                                            AND PHM_THRESHOLDS_SK =
                                                   Z.PHM_THRESHOLDS_SK
                                            AND NODETYPE = P.NODETYPE
                                            AND ERRORCODE = P.ERRORCODE
                                            AND INSTANCEID = P.INSTANCEID
                                            AND NODEID = P.NODEID
                                            --AND FLAG_DATE >= TRUNC(D.FLAG_DATE) - Z.THRESHOLD_DATA_DAYS
                                            AND FLAG_DATE <=
                                                   TRUNC (D.FLAG_DATE)
                                   ORDER BY FLAG_DATE DESC)
                            WHERE ROWNUM <= Z.THRESHOLD_NUMBER_UNIT;

                           -- Get TODAY_TEST_COUNT
                           SELECT TESTCOUNT
                             INTO TODAY_TEST_COUNT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND FLAG_DATE = TRUNC (D.FLAG_DATE);

                           -- GET TODAY_ERRORPCT
                           SELECT NVL (ERRORPCT, 0)
                             INTO TODAY_ERRORPCT
                             FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                            WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                  AND BATCH_NUM = V_BATCH_NUM
                                  AND RUN_DATE = V_RUN_DATE
                                  AND PHM_THRESHOLDS_SK = V_ALG_NUM
                                  AND NODETYPE = P.NODETYPE
                                  AND ERRORCODE = P.ERRORCODE
                                  AND INSTANCEID = P.INSTANCEID
                                  AND NODEID = P.NODEID
                                  AND TRUNC (FLAG_DATE) = TRUNC (D.FLAG_DATE);

                           FLAG := 'NO';
                           IHN_VALUE := '';

                           IF TODAY_TEST_COUNT >= FILTER_COUNT
                           THEN
                              SELECT   STDDEV (ERRORPCT) * Z.THRESHOLD_NUMBER
                                     + AVG (ERRORPCT)
                                INTO V_THRESHOLD_LIMIT
                                FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR T
                               WHERE     MODULE_SN = UPPER (Y.MODULE_SN)
                                     AND BATCH_NUM = V_BATCH_NUM
                                     AND RUN_DATE = V_RUN_DATE
                                     AND PHM_THRESHOLDS_SK = V_ALG_NUM
                                     AND NODETYPE = P.NODETYPE
                                     AND ERRORCODE = P.ERRORCODE
                                     AND INSTANCEID = P.INSTANCEID
                                     AND NODEID = P.NODEID
                                     AND TESTCOUNT < FILTER_COUNT
                                     AND FLAG_DATE BETWEEN V_DATE_30TH
                                                       AND TRUNC (
                                                              D.FLAG_DATE);

                              IF V_THRESHOLD_LIMIT > 0
                              THEN
                                 IF TODAY_ERRORPCT >= V_THRESHOLD_LIMIT
                                 THEN
                                    CONSEQ_COUNT := 0;
                                    FLAG := 'NO';
                                    IHN_VALUE := '';

                                    --DBMS_OUTPUT.PUT_LINE('in  '||'v_prev_date'||V_PREV_DATE||' Z.THRESHOLD_NUMBER_UNIT '|| Z.THRESHOLD_NUMBER_UNIT
                                    --||'   ' ||'d.flag_date '||TRUNC(D.FLAG_DATE));

                                    FOR I
                                       IN (  SELECT *
                                               FROM (  SELECT *
                                                         FROM SVC_PHM_ODS.PHM_A3600_TEMP_ERROR
                                                        WHERE     MODULE_SN =
                                                                     UPPER (
                                                                        Y.MODULE_SN)
                                                              AND BATCH_NUM =
                                                                     V_BATCH_NUM
                                                              AND RUN_DATE =
                                                                     V_RUN_DATE
                                                              AND PHM_THRESHOLDS_SK =
                                                                     Z.PHM_THRESHOLDS_SK
                                                              AND NODETYPE =
                                                                     P.NODETYPE
                                                              AND ERRORCODE =
                                                                     P.ERRORCODE
                                                              AND INSTANCEID =
                                                                     P.INSTANCEID
                                                              AND NODEID =
                                                                     P.NODEID
                                                              AND TESTCOUNT <
                                                                     FILTER_COUNT
                                                              AND FLAG_DATE <=
                                                                     TRUNC (
                                                                        D.FLAG_DATE)
                                                     ORDER BY FLAG_DATE DESC)
                                              WHERE ROWNUM <=
                                                       Z.THRESHOLD_NUMBER_UNIT
                                           ORDER BY FLAG_DATE DESC)
                                    LOOP
                                       IF I.ERRORPCT >= V_THRESHOLD_LIMIT
                                       THEN
                                          CONSEQ_COUNT := CONSEQ_COUNT + 1;
                                       ELSE
                                          CONSEQ_COUNT := 0;
                                       END IF;
                                    END LOOP;

                                    IF CONSEQ_COUNT >=
                                          Z.THRESHOLD_NUMBER_UNIT
                                    THEN
                                       FLAG := 'YES';
                                       IHN_VALUE := Z.THRESHOLD_ALERT;

                                       V_FLAGGED_PL := NULL;
                                       V_FLAGGED_EXP_CODE := NULL;

                                       PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE (
                                          V_ALG_NUM,
                                          Y.PL,
                                          NULL,
                                          V_FLAGGED_PL,
                                          V_FLAGGED_EXP_CODE);
                                    ELSE
                                       FLAG := 'NO';
                                       IHN_VALUE := '';
                                    END IF;
                                 ELSE
                                    FLAG := 'NO';
                                    IHN_VALUE := '';
                                 END IF;
                              ELSE
                                 FLAG := 'NO';
                                 IHN_VALUE := '';
                              END IF;
                           ELSE
                              FLAG := 'NO';
                              IHN_VALUE := '';
                           END IF;
                        ELSE
                           TODAY_ERRORPCT := 0;
                           FLAG := 'NO';
                           IHN_VALUE := NULL;
                        END IF;
                     ELSE
                        TODAY_ERRORPCT := 0;
                        FLAG := 'NO';
                        IHN_VALUE := NULL;
                     END IF;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        V_ERROR_MESSAGE :=
                              ' CALCULATION OF SD OF ERRORS HAS FAILED FOR '
                           || V_ALG_NUM
                           || ' FOR '
                           || Y.MODULE_SN
                           || ' FOR DATE '
                           || D.FLAG_DATE
                           || ', ERROR :'
                           || SQLERRM;
                        V_PROCESS_STATUS := 'ERRORED';
                        TODAY_ERRORPCT := 0;
                        FLAG := 'NO';
                        IHN_VALUE := NULL;
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
                  END;

                  --PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_INSERT
                  PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL (
                     Y.CUSTOMERNAME,
                     Y.CUSTOMERNUMBER,
                     Y.DEVICE_ID,
                     Y.MODULE_SN,
                     Y.COUNTRYNAME,
                     Y.AREAREGION,
                     V_ALG_NUM,
                     Z.THRESHOLDS_SK_VAL,
                     D.MAX_DATE_VALUE,
                     D.ERRORPCT,
                     FLAG,
                     IHN_VALUE,
                     TO_CHAR (P.NODEID) || ',' || TO_CHAR (P.INSTANCEID),
                     VALGNAME,
                     V_PROD_FAMILY,
                     V_BATCH_NUM,
                     Z.PHM_THRESHOLDS_SK,
                     V_RUN_DATE,
                     V_PROCESS_ID,
                     V_FLAGGED_PL,
                     V_FLAGGED_EXP_CODE);
                  V_INSERT_COUNT := V_INSERT_COUNT + 1;

                  IF MOD (V_INSERT_COUNT, 10000) = 0
                  THEN
                     COMMIT;
                  END IF;
               END LOOP;
            END LOOP;
         END LOOP;
      END IF;

      COMMIT;

      FOR I
         IN (  SELECT RUN_DATE,
                      BATCH_NUM,
                      PHM_THRESHOLDS_SK,
                      SN,
                      TRUNC (FLAG_DATE) FLAG_DATE,
                      COUNT (DISTINCT FLAG_YN) FLAGS --,MAX(DEVICE_VALUE) MAX_VAL,MIN(DEVICE_VALUE) MIN_VAL
                 FROM PHM_ALG_OUTPUT
                WHERE     BATCH_NUM = V_BATCH_NUM
                      AND RUN_DATE = V_RUN_DATE
                      AND PHM_THRESHOLDS_SK = Z.PHM_THRESHOLDS_SK
             GROUP BY RUN_DATE,
                      BATCH_NUM,
                      PHM_THRESHOLDS_SK,
                      SN,
                      TRUNC (FLAG_DATE))
      LOOP
         IF I.FLAGS = 2
         THEN
            DELETE FROM PHM_ALG_OUTPUT
                  WHERE     BATCH_NUM = I.BATCH_NUM
                        AND RUN_DATE = I.RUN_DATE
                        AND SN = I.SN
                        AND PHM_THRESHOLDS_SK = I.PHM_THRESHOLDS_SK
                        AND TRUNC (FLAG_DATE) = I.FLAG_DATE
                        AND ROWID NOT IN (SELECT MAX (ROWID)
                                            FROM PHM_ALG_OUTPUT
                                           WHERE     BATCH_NUM = I.BATCH_NUM
                                                 AND RUN_DATE = I.RUN_DATE
                                                 AND SN = I.SN
                                                 AND PHM_THRESHOLDS_SK =
                                                        I.PHM_THRESHOLDS_SK
                                                 AND TRUNC (FLAG_DATE) =
                                                        I.FLAG_DATE
                                                 AND FLAG_YN = 'YES');
         END IF;

         IF I.FLAGS = 1
         THEN
            DELETE FROM PHM_ALG_OUTPUT
                  WHERE     BATCH_NUM = I.BATCH_NUM
                        AND RUN_DATE = I.RUN_DATE
                        AND SN = I.SN
                        AND PHM_THRESHOLDS_SK = I.PHM_THRESHOLDS_SK
                        AND TRUNC (FLAG_DATE) = I.FLAG_DATE
                        AND ROWID NOT IN (SELECT MAX (ROWID)
                                            FROM PHM_ALG_OUTPUT
                                           WHERE     BATCH_NUM = I.BATCH_NUM
                                                 AND RUN_DATE = I.RUN_DATE
                                                 AND SN = I.SN
                                                 AND PHM_THRESHOLDS_SK =
                                                        I.PHM_THRESHOLDS_SK
                                                 AND TRUNC (FLAG_DATE) =
                                                        I.FLAG_DATE);
         END IF;
      END LOOP;
   END LOOP;                                                   --ALL_THREHOLDS

   COMMIT;


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
EXCEPTION
   WHEN OTHERS
   THEN
      V_ERROR_MESSAGE :=
            ' EXECUTION OF A3600 ALGORITHM HAS FAILED BECAUSE OF ERROR :'
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
END PHM_A3600_1;
/