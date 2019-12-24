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
