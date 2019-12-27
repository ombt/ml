CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_CC_CUVETTE_CODE2_PROC (
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

   V_EXISTING_REC_CNT   NUMBER;
   V_FLAG               VARCHAR (5);
   V_REC_COUNT          NUMBER := 0;
   V_REC_INS_COUNT      NUMBER := 0;
   V_FLAG_DATE_TIME     DATE;
   V_RES_COUNT          NUMBER;
   VALGNAME             VARCHAR (200);
   V_ALG_DFN_SK         NUMBER;
   V_IHN_LEVEL3_DESC  VARCHAR2(200);
   V_FLAGGED_PL       VARCHAR2(10);
   V_FLAGGED_EXP_CODE VARCHAR2(10);


   -- ALGORITHM PARAMETER VARAIBLES TO HANDLE THE PROCESS FLOW
   -- < CHANGE >

   V_F24_THRESHOLD      NUMBER;
   V_F25_THRESHOLD      NUMBER;
   V_F24_PCT            NUMBER;
   V_F25_PCT            NUMBER;
   V_C4_CUVETTE         NUMBER;
   V_C16_CUVETTE        NUMBER;
   V_THRESHOLDS_COUNT   NUMBER;



   vcMsg_Type           VARCHAR2 (30) := 'CUVETTE';

   vStartDate  DATE := SYSDATE - 8;
   vEndDate   DATE := SYSDATE;

   CURSOR BATCH_SN ( vRUN_DATE     DATE,
      vBATCH_NUM    VARCHAR2)
   IS
      SELECT DISTINCT MODULESNDRM AS SN
        FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC
       WHERE     BATCH_NUM = vBATCH_NUM
             AND RUN_DATE = vRUN_DATE
             AND (MODULESNDRM LIKE 'C4%' OR MODULESNDRM LIKE 'C16%');


   CURSOR Suspected_C4_SN (vSN VARCHAR2)
   IS
        SELECT sn, COUNT (*)
          FROM (SELECT R.DEVICEID,
                       R.MODULEID,
                       R.MODULESNDRM sn,
                       R.CUVETTENUMBER
                  FROM (SELECT DEVICEID,
                               MODULEID,
                               MODULESNDRM,
                               ReplicateID
                          FROM SVC_PHM_ODS.PHM_ODS_PRESSURES_DIS
                         WHERE     MODULESNDRM = vSN
                               AND COMPLETIONDATE BETWEEN vStartDate
                                                      AND vEndDate
                               AND RESULTCODE = '30'
                               AND REPLICATEID != 0) P,
                       (SELECT DEVICEID,
                               REPLICATEID,
                               MODULEID,
                               MODULESNDRM,
                               CUVETTENUMBER
                          FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC
                         WHERE     MODULESNDRM = vSN
                               AND COMPLETIONDATE BETWEEN vStartDate
                                                      AND vEndDate) R
                 WHERE     R.DEVICEID = P.DEVICEID
                       AND R.REPLICATEID = P.REPLICATEID) J
      GROUP BY sn
        HAVING COUNT (*) >= 2000;



   CURSOR Suspected_C16_SN (vSN VARCHAR2)
   IS
        SELECT sn, COUNT (*)
          FROM (SELECT R.DEVICEID,
                       R.MODULEID,
                       R.MODULESNDRM sn,
                       R.CUVETTENUMBER
                  FROM (SELECT DEVICEID,
                               MODULEID,
                               MODULESNDRM,
                               ReplicateID
                          FROM SVC_PHM_ODS.PHM_ODS_PRESSURES_DIS
                         WHERE     MODULESNDRM = vSN
                               AND COMPLETIONDATE BETWEEN vStartDate
                                                      AND vEndDate
                               AND RESULTCODE = '30'
                               AND REPLICATEID != 0) P,
                       (SELECT DEVICEID,
                               REPLICATEID,
                               MODULEID,
                               MODULESNDRM,
                               CUVETTENUMBER
                          FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC
                         WHERE     MODULESNDRM = vSN
                               AND COMPLETIONDATE BETWEEN vStartDate
                                                      AND vEndDate) R
                 WHERE     R.DEVICEID = P.DEVICEID
                       AND R.REPLICATEID = P.REPLICATEID) J
      GROUP BY sn
        HAVING COUNT (*) >= 3400;


   CURSOR Suspected_Cuvette (pcSN VARCHAR2)
   IS
        SELECT DEVICEID,
               MODULEID,
               sn,
               CUVETTENUMBER,
               COUNT (*)
          FROM (SELECT R.DEVICEID,
                       R.MODULEID,
                       R.MODULESNDRM sn,
                       R.CUVETTENUMBER
                  FROM (SELECT DEVICEID,
                               MODULEID,
                               MODULESNDRM,
                               ReplicateID
                          FROM SVC_PHM_ODS.PHM_ODS_PRESSURES_DIS
                         WHERE     MODULESNDRM = pcSN
                               AND COMPLETIONDATE BETWEEN vStartDate
                                                      AND vEndDate
                               AND TO_NUMBER (LOGFIELD25) >= 20000
                               AND (   MODULESNDRM LIKE 'C4%'
                                    OR MODULESNDRM LIKE 'C16%')
                               AND RESULTCODE = '30'
                               AND REPLICATEID != 0) P,
                       (SELECT DEVICEID,
                               REPLICATEID,
                               MODULEID,
                               MODULESNDRM,
                               CUVETTENUMBER
                          FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC
                         WHERE     MODULESNDRM = pcSN
                               AND COMPLETIONDATE BETWEEN vStartDate
                                                      AND vEndDate
                               AND (   MODULESNDRM LIKE 'C4%'
                                    OR MODULESNDRM LIKE 'C16%')) R
                 WHERE     R.DEVICEID = P.DEVICEID
                       AND R.REPLICATEID = P.REPLICATEID) J
      GROUP BY DEVICEID,
               MODULEID,
               sn,
               CUVETTENUMBER
        HAVING COUNT (*) >= 2;



   -- </ CHANGE >

   --  ALGORITHM CURSORS
   /* Device, SN List from PHM_ODS_RESULTS_IA table for the Current Batch and Run Date */
   CURSOR DEVICE_NOT_FLAG_LIST
   IS
        SELECT R.DEVICEID,
               R.MODULESNDRM SERIAL_NUM,
               MAX (IL.PL) PL,
               MAX (IL.CUSTOMER_NUM) CUSTOMER_NUMBER,
               MAX (IL.CUSTOMER) CUSTOMER_NAME,
               MAX (PC.COUNTRY) COUNTRY_NAME,
               MAX (PC.AREAREGION) AREA,
               MAX (IL.CITY) CITY,
               MAX (COMPLETIONDATE) MAX_COMPLETION_DATE,
               COUNT (*) DEVICE_SN_CNT
          FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC R,
               INSTRUMENTLISTING IL,
               PHM_COUNTRY PC
         WHERE     R.BATCH_NUM = V_BATCH_NUM
               AND R.RUN_DATE = V_RUN_DATE
               AND (R.MODULESNDRM LIKE 'C16%' OR R.MODULESNDRM LIKE 'C4%')
               AND UPPER (R.MODULESNDRM) = UPPER (IL.SN)
               AND LOWER (R.MODULESNDRM) NOT IN
                      (SELECT DISTINCT LOWER (SN)
                         FROM (SELECT distinct DEVICE_ID
                                         AS DEVICEID,
                                      SN,
                                      Msg
                                 FROM (  SELECT F.DEVICE_ID,
                                                LOWER (F.SN) AS SN,
                                                'Cuvette Wash Subassembly Mal-Function.' AS Msg,
                                                COUNT (*)
                                           FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                                          WHERE     F.ERRORPCT > 10 * 2
                                                AND F.TYPE ='E25'
                                                AND F.SN LIKE 'C4%'
                                                AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE
                                       GROUP BY DEVICE_ID,
                                                LOWER (F.SN)
                                         HAVING COUNT (*) >= 10 * 2
                                       UNION
                                         SELECT F.DEVICE_ID,
                                                LOWER (F.SN) AS SN,
                                                'A-Lines Cuvette Wash Subassembly Mal-Function.' AS Msg,
                                                COUNT (*)
                                           FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                                          WHERE     F.ERRORPCT > 10 * 2
                                                AND F.TYPE ='E25'
                                                AND F.SN LIKE  'C16%'
                                                AND MOD ( F.CUVETTE,2) = 1
                                                AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE       
                                       GROUP BY DEVICE_ID,
                                                LOWER (F.SN)
                                         HAVING COUNT (*) > 17* 2
                                       UNION
                                         SELECT F.DEVICE_ID,
                                                LOWER (F.SN) AS SN,
                                                -- 'B-Lines Cuvette Wash Subassembly Mal-Function.' AS Msg,
                                                'A-Lines Cuvette Wash Subassembly Mal-Function.' AS Msg,
                                                COUNT (*)
                                           FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                                          WHERE  F.ERRORPCT > 10 * 2
                                                AND F.TYPE = 'E25'
                                                AND F.SN LIKE 'C16%'
                                                AND MOD (F.CUVETTE,2) = 0
                                                AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE
                                       GROUP BY DEVICE_ID,
                                                LOWER (F.SN)
                                         HAVING COUNT (*) > 17 * 2
                                         ) B
                                WHERE B.SN NOT IN
                                         (SELECT LOWER (F.SN)
                                            FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                                           WHERE     F.ERRORPCT > 10
                                                 AND F.TYPE = 'E24'
                                                 AND F.SN LIKE 'C4%'
                                                 AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE
                                          UNION
                                          SELECT LOWER (F.SN)
                                            FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                                           WHERE     F.ERRORPCT > 33
                                                 AND F.TYPE = 'E24'
                                                 AND F.SN LIKE 'C16%'
                                                 AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE)))
               AND PC.COUNTRY_CODE = IL.COUNTRY_CODE and IL.INST_STATUS='Active'
      GROUP BY R.DEVICEID, R.MODULESNDRM;

   /* Matching LIST for the Input Parameters */
   CURSOR DEVICE_MATCHING_LIST
   IS
      SELECT N.DEVICEID,
             N.SN SERIAL_NUM,
             PL,
             IL.CUSTOMER_NUMBER,
             IL.CUSTOMER_NAME,
             PC.COUNTRY COUNTRY_NAME,
             PC.AREAREGION AREA,
             IL.CITY,
             N.Msg
        FROM (  SELECT MAX (SN) SN,
                       MAX (PL) PL,
                       MAX (CUSTOMER_NUM) CUSTOMER_NUMBER,
                       MAX (CUSTOMER) CUSTOMER_NAME,
                       MAX (CITY) City,
                       MAX (COUNTRY_CODE) COUNTRY_CODE
                  FROM INSTRUMENTLISTING WHERE INST_STATUS='Active'
              GROUP BY sn) IL,
             PHM_COUNTRY PC,
             (SELECT distinct DEVICE_ID AS DEVICEID, SN, Msg
                FROM (  SELECT F.DEVICE_ID,
                               LOWER (F.SN) AS SN,
                               'Cuvette Wash Subassembly Mal-Function.' AS Msg,
                               COUNT (*)
                          FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                         WHERE     F.ERRORPCT > 10 * 2
                               AND F.TYPE = 'E25'
                               AND F.SN LIKE 'C4%'
                               AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE
                      GROUP BY DEVICE_ID, LOWER (F.SN)
                        HAVING COUNT (*) >= 10 * 2
                      UNION
                        SELECT F.DEVICE_ID,
                               LOWER (F.SN) AS SN,
                               'A-Lines Cuvette Wash Subassembly Mal-Function.' AS Msg,
                               COUNT (*)
                          FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                         WHERE     F.ERRORPCT > 10 * 2
                               AND F.TYPE = 'E25'
                               AND F.SN LIKE 'C16%'
                               AND MOD (F.CUVETTE, 2) = 1
                               AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE
                      GROUP BY DEVICE_ID, LOWER (F.SN)
                        HAVING COUNT (*) > 17 * 2
                      UNION
                        SELECT F.DEVICE_ID,
                               LOWER (F.SN) AS SN,
                               --'B-Lines Cuvette Wash Subassembly Mal-Function.' AS Msg,
                               'A-Lines Cuvette Wash Subassembly Mal-Function.' AS Msg,
                               COUNT (*)
                          FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                         WHERE     F.ERRORPCT > 10 * 2
                               AND F.TYPE = 'E25'
                               AND F.SN LIKE 'C16%'
                               AND MOD (F.CUVETTE, 2) = 0
                               AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE
                      GROUP BY DEVICE_ID, LOWER (F.SN)
                        HAVING COUNT (*) > 17 * 2) B
               WHERE B.SN NOT IN
                        (SELECT LOWER (F.SN)
                           FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                          WHERE     F.ERRORPCT > 10
                                AND F.TYPE = 'E24'
                                AND F.SN LIKE 'C4%'
                                AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE
                         UNION
                         SELECT LOWER (F.SN)
                           FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
                          WHERE     F.ERRORPCT > 33
                                AND F.TYPE = 'E24'
                                AND F.SN LIKE 'C16%'
                                AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE)) N
       WHERE     UPPER (N.SN) = UPPER (IL.SN)
             AND PC.COUNTRY_CODE = IL.COUNTRY_CODE;



   CURSOR curSuspendedCuvettes_F24
   IS
      SELECT E.sn,
             E.cuvette,
             A.Total_count,
             CAST (
                ( (E.ERROR_COUNT / A.Total_COUNT) * 100) AS DECIMAL (5, 2))
                AS pct
        FROM (SELECT *
                FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
               WHERE F.TYPE = 'E24' AND F.ERROR_COUNT IS NOT NULL 
               AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE) E,
             (SELECT *
                FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
               WHERE F.TYPE = 'All' AND F.Total_COUNT IS NOT NULL
               AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE) A
       WHERE E.sn = A.sn AND E.cuvette = A.Cuvette;



   CURSOR curSuspendedCuvettes_F25
   IS
      SELECT E.sn,
             E.cuvette,
             A.Total_count,
             CAST (
                ( (E.ERROR_COUNT / A.Total_COUNT) * 100) AS DECIMAL (5, 2))
                AS pct
        FROM (SELECT *
                FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
               WHERE F.TYPE = 'E25' AND F.ERROR_COUNT IS NOT NULL
               AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE) E,
             (SELECT *
                FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 F
               WHERE F.TYPE = 'All' AND F.Total_COUNT IS NOT NULL
               AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE) A
       WHERE E.sn = A.sn AND E.cuvette = A.Cuvette;



   vcFlag               INTEGER := 0;
BEGIN
   -- STEP 1   :PURPOSE TO GET PROCESSID OF CURRENT EXECUTION

   V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID ();
   --V_PROCESS_ID := -1;
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

   FOR I IN (SELECT PARAMETER_VALUES, PARAMETER_NAME, PHM_PATTERNS_SK
               FROM PHM_THRESHOLD_PARAMETER
              WHERE PHM_PATTERNS_SK = V_ALG_NUM AND NVL(DELETE_FLAG,'N') <> 'Y')
   LOOP
      -- PURPOSE - IN CASE OF NEW PERAMETER DEFEINED IN ALGORITHM DEFINITION -  WRITE CODE WITH A NEW IF CONDITION TO GET NEW PARAMETER VALUE
      -- <CHANGE >

      IF I.PARAMETER_NAME = 'LOGFIELD24_THRESHOLD'
      THEN
         V_F24_THRESHOLD := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;

      IF I.PARAMETER_NAME = 'LOGFIELD25_THRESHOLD'
      THEN
         V_F25_THRESHOLD := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;

      IF I.PARAMETER_NAME = 'LOGFIELD24_PCT'
      THEN
         V_F24_PCT := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;

      IF I.PARAMETER_NAME = 'LOGFIELD25_PCT'
      THEN
         V_F25_PCT := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;

      IF I.PARAMETER_NAME = 'C4_CUVETTE_THRESHOLD'
      THEN
         V_C4_CUVETTE := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;

      IF I.PARAMETER_NAME = 'C16_CUVETTE_THRESHOLD'
      THEN
         V_C16_CUVETTE := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;

      IF I.PARAMETER_NAME = 'THRESHOLDS_COUNT'
      THEN
         V_THRESHOLDS_COUNT := TO_NUMBER (I.PARAMETER_VALUES);
      END IF;
      IF I.PARAMETER_NAME = 'IHN_LEVEL3_DESC'
      THEN
         V_IHN_LEVEL3_DESC := I.PARAMETER_VALUES;
      END IF;  

   -- < CHANGE>
   END LOOP;


   DELETE FROM SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 WHERE DATE_CREATED < sysdate -2;
   COMMIT;
   
   
   FOR u IN BATCH_SN (V_RUN_DATE, V_BATCH_NUM)
   LOOP
      vcFlag := 0;
      --DBMS_OUTPUT.PUT_LINE ('Suspected_Cuvette SN=' || u.sn);

      IF SUBSTR (u.sn, 2, 1) = '4'
      THEN
         FOR w IN suspected_C4_SN (u.sn)
         LOOP
            --DBMS_OUTPUT.PUT_LINE ('Suspected_Cuvette C4_SN=' || u.sn);

            vcFlag := 1;
         END LOOP;                                           --suspected_C4_SN
      ELSE
         FOR w IN suspected_C16_SN (u.sn)
         LOOP
            --DBMS_OUTPUT.PUT_LINE ('Suspected_Cuvette C16_SN=' || u.sn);

            vcFlag := 1;
         END LOOP;                                          --suspected_C16_SN
      END IF;

      IF vcFlag = 1
      THEN
         vcFlag := 0;

         FOR v IN Suspected_Cuvette (u.sn)
         LOOP
            --DBMS_OUTPUT.PUT_LINE ('Suspected_Cuvette time=' || TO_CHAR (SYSTIMESTAMP, 'mm/dd/yyyy  hh:mi:ss am'));
            vcFlag := 1;
         END LOOP;                                         --Suspected_Cuvette
      END IF;

      IF vcFlag = 1
      THEN
         --DBMS_OUTPUT.PUT_LINE ('Insert      v.SN=' || u.sn);

         -- F25 >20000
         INSERT INTO SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 (DEVICE_ID,
                                                      Module_Number,
                                                      SN,
                                                      Cuvette,
                                                      TYPE,
                                                      BATCH_NUM,
                                                      RUN_DATE,
                                                      Error_COUNT)
              SELECT /*+ USE_HASH(M MT) */
                    R.DEVICEID,
                     R.MODULEID,
                     R.MODULESNDRM,
                     R.CUVETTENUMBER,
                     'E24',
                     V_BATCH_NUM,
                     V_RUN_DATE,
                      COUNT (*)
                FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC R
               WHERE     COMPLETIONDATE BETWEEN TRUNC (vStartDate)
                                            AND TRUNC (vEndDate)
                     AND R.MODULESNDRM = u.sn                        --10027--
                     AND R.REPLICATEID IN
                            (SELECT REPLICATEID
                               FROM SVC_PHM_ODS.PHM_ODS_PRESSURES_DIS
                              WHERE     COMPLETIONDATE BETWEEN TRUNC (
                                                                  vStartDate)
                                                           AND TRUNC (
                                                                  vEndDate)
                                    AND MODULESNDRM = u.sn
                                    AND TO_NUMBER (LOGFIELD24) >= 15000
                                    AND RESULTCODE = 30
                                    AND REPLICATEID != 0)
            GROUP BY R.DEVICEID,
                     R.MODULEID,
                     R.MODULESNDRM,
                     R.CUVETTENUMBER,
                     'E24',
                     V_BATCH_NUM,
                     V_RUN_DATE;

         COMMIT;

         --All
         INSERT INTO SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 (DEVICE_ID,
                                                      Module_Number,
                                                      SN,
                                                      Cuvette,
                                                      TYPE,
                                                      BATCH_NUM,
                                                      RUN_DATE,
                                                      Total_COUNT)
              SELECT /*+ USE_HASH(M MT) */
                    R.DEVICEID,
                     R.MODULEID,
                     R.MODULESNDRM,
                     R.CUVETTENUMBER,
                     'All',
                     V_BATCH_NUM,
                     V_RUN_DATE,
                     COUNT (*)
                FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC R
               WHERE     COMPLETIONDATE BETWEEN TRUNC (vStartDate)
                                            AND TRUNC (vEndDate)
                     AND MODULESNDRM = u.sn
            GROUP BY R.DEVICEID,
                     R.MODULEID,
                     R.MODULESNDRM,
                     R.CUVETTENUMBER,
                     'All',
                     V_BATCH_NUM,
                     V_RUN_DATE;

         COMMIT;

         -- F25 >20000
         INSERT INTO SVC_PHM_ODS.PHM_ODS_CUVETTE_F25 (DEVICE_ID,
                                                      Module_Number,
                                                      SN,
                                                      Cuvette,
                                                      TYPE,
                                                      BATCH_NUM,
                                                      RUN_DATE,
                                                      Error_COUNT)
              SELECT /*+ USE_HASH(M MT) */
                    R.DEVICEID,
                     R.MODULEID,
                     R.MODULESNDRM,
                     R.CUVETTENUMBER,
                     'E25',
                     V_BATCH_NUM,
                     V_RUN_DATE,
                     COUNT (*)
                FROM SVC_PHM_ODS.PHM_ODS_RESULTS_CC R
               WHERE     COMPLETIONDATE BETWEEN TRUNC (vStartDate)
                                            AND TRUNC (vEndDate)
                     AND R.MODULESNDRM = u.sn                        --10027--
                     AND R.REPLICATEID IN
                            (SELECT REPLICATEID
                               FROM SVC_PHM_ODS.PHM_ODS_PRESSURES_DIS
                              WHERE     COMPLETIONDATE BETWEEN TRUNC (
                                                                  vStartDate)
                                                           AND TRUNC (
                                                                  vEndDate)
                                    AND MODULESNDRM = u.sn
                                    AND TO_NUMBER (LOGFIELD25) >= 20000
                                    AND RESULTCODE = 30
                                    AND REPLICATEID != 0)
            GROUP BY R.DEVICEID,
                     R.MODULEID,
                     R.MODULESNDRM,
                     R.CUVETTENUMBER,
                     'E25',
                     V_BATCH_NUM,
                     V_RUN_DATE;

         COMMIT;

      END IF;
   END LOOP;


   -- Update Pct
   FOR x IN curSuspendedCuvettes_F24
   LOOP
      --DBMS_OUTPUT.PUT_LINE ('v.SN='||v.SN||', time='||TO_CHAR (SYSTIMESTAMP, 'mm/dd/yyyy  hh:mi:ss am'));
      UPDATE SVC_PHM_ODS.PHM_ODS_CUVETTE_F25
         SET ErrorPct = x.pct, Total_Count = x.total_count
       WHERE SN = x.SN AND cuvette = x.cuvette AND TYPE = 'E24' 
       AND BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE;

      COMMIT;
   END LOOP;



   FOR y IN curSuspendedCuvettes_F25
   LOOP
      --DBMS_OUTPUT.PUT_LINE ('v.SN='||v.SN||', time='||TO_CHAR (SYSTIMESTAMP, 'mm/dd/yyyy  hh:mi:ss am'));
      UPDATE SVC_PHM_ODS.PHM_ODS_CUVETTE_F25
         SET ErrorPct = y.pct, Total_Count = y.total_count
       WHERE SN = y.SN AND cuvette = y.cuvette AND TYPE = 'E25' 
       AND BATCH_NUM = V_BATCH_NUM 
       AND RUN_DATE = V_RUN_DATE;

      COMMIT;
   END LOOP;

   COMMIT;


   FOR DL IN DEVICE_NOT_FLAG_LIST
   LOOP
      BEGIN
         V_FLAG := 'NO';
         V_FLAG_DATE_TIME := V_RUN_DATE; -- should it be null for healthy instruments?
         V_RES_COUNT := 0;


         --  INSERT THE DATA INTO COMMON RESULT OUTPUT TABLE
         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_INSERT (
            DL.CUSTOMER_NAME,
            DL.CUSTOMER_NUMBER,
            DL.DEVICEID,
            DL.SERIAL_NUM,
            DL.COUNTRY_NAME,
            DL.AREA                                                 --DL.CITY,
                   ,
            V_ALG_DFN_SK,
            -1,
            V_FLAG_DATE_TIME,
            V_RES_COUNT,
            V_FLAG,
            NULL,
            NULL,
            VALGNAME,
            NULL,
            V_BATCH_NUM,
            V_ALG_NUM,
            V_RUN_DATE,
            V_PROCESS_ID);

         --  INSERT THE DATA INTO COMMON CHART OUTPUT TABLE
         PHM_ALGORITHM_UTILITIES_1.PHM_ALG_CHART_INSERT (
            DL.DEVICEID,
            DL.PL,
            DL.SERIAL_NUM,
            DL.COUNTRY_NAME,
            DL.AREA                                                  --DL.CITY
                   ,
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

         V_REC_INS_COUNT := V_REC_INS_COUNT + 1;                -- DL%ROWCOUNT
      EXCEPTION
         --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS
         WHEN OTHERS
         THEN
            V_ERROR_MESSAGE :=
                  ' PHM_CC_CUVETTE_PROC EXECUTION HAS FAILED FOR '
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
   END LOOP;                                            --DEVICE_NOT_FLAG_LIST



   FOR ML IN DEVICE_MATCHING_LIST
   LOOP
      BEGIN
         V_FLAG := 'YES';
         V_FLAG_DATE_TIME := V_RUN_DATE; -- should it be null for healthy instruments?
         V_RES_COUNT := 1;
         V_FLAGGED_PL := NULL;
         V_FLAGGED_EXP_CODE := NULL;
         
         -- Get the PL and experience code for the flagged instrument
         PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(V_ALG_NUM, ML.PL, NULL, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);         


         --  INSERT THE DATA INTO COMMON RESULT OUTPUT TABLE
         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL (
            ML.CUSTOMER_NAME,
            ML.CUSTOMER_NUMBER,
            ML.DEVICEID,
            ML.SERIAL_NUM,
            ML.COUNTRY_NAME,
            ML.AREA                                                 --ML.CITY,
                   ,
            V_ALG_DFN_SK,
            -1,
            V_FLAG_DATE_TIME,
            V_RES_COUNT,
            V_FLAG,
            --ML.MSG,
            V_IHN_LEVEL3_DESC,
            NULL,
            VALGNAME,
            NULL,
            V_BATCH_NUM,
            V_ALG_NUM,
            V_RUN_DATE,
            V_PROCESS_ID, V_FLAGGED_PL, V_FLAGGED_EXP_CODE);

         --  INSERT THE DATA INTO COMMON CHART OUTPUT TABLE
         PHM_ALGORITHM_UTILITIES_1.PHM_ALG_CHART_INSERT (
            ML.DEVICEID,
            ML.PL,
            ML.SERIAL_NUM,
            ML.COUNTRY_NAME,
            ML.AREA                                                  --ML.CITY
                   ,
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

         V_REC_INS_COUNT := V_REC_INS_COUNT + 1;                -- DL%ROWCOUNT
      EXCEPTION
         --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS
         WHEN OTHERS
         THEN
            V_ERROR_MESSAGE :=
                  ' PHM_CC_CUVETTE_PROC EXECUTION HAS FAILED FOR '
               || V_ALG_NUM
               || ' FOR '
               || ML.SERIAL_NUM
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
   END LOOP;                                                  --DEVICE_SN_LIST

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
EXCEPTION
   --  PURPOSE - TO CATCH ALL THE RUN TIME EXCEPTIONS AND  TO UPDATE THE AUDIT TABLES WITH ERROR STATUS
   WHEN OTHERS
   THEN
      V_PROCESS_STATUS := 'ERRORED';
      V_ERROR_MESSAGE :=
            'ALGORITHM EXECUTION FAILED FOR PHM_CC_CUVETTE_PROC, DUE TO: '
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
END PHM_CC_CUVETTE_CODE2_PROC;


-- Assigning the grants to SVC_PHM_CONNECT user for ETL process to pick
grant execute on SVC_PHM_OWNER.PHM_CC_CUVETTE_CODE2_PROC to SVC_PHM_CONNECT;