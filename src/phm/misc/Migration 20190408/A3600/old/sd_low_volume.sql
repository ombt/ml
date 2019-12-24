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
