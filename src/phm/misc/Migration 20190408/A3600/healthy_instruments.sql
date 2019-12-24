
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
