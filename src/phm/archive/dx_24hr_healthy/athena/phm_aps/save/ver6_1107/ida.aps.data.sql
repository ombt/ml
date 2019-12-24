
                    SELECT 
                        DEVICE_ID, 
                        MAX(PSM.IOM_SN) IOM_SN, 
                        PSM.PL, 
                        PSM.SN MODULE_SN, 
                        TRUNC(TIMESTAMP) FLAG_DATE, 
                        COUNT(FILEID) ERRORCOUNT, 
                        max(timestamp) MS_TIME
                    FROM 
                        SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, 
                        SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM
                    WHERE 
                        TIMESTAMP BETWEEN (
                            SELECT  
                                -- MIN (TRUNC(TIMESTAMP)) - TH.THRESHOLDS_DAYS 
                                MIN (TRUNC(TIMESTAMP)) - 2
                            FROM 
                                SVC_PHM_ODS.PHM_ODS_APS_ERRORS 
                            WHERE 
                                -- RUN_DATE = V_RUN_DATE 
                                RUN_DATE = TO_DATE('30-OCT-2019')
                            AND 
                                -- BATCH_NUM = V_BATCH_NUM 
                                BATCH_NUM = 'BTH0600'
                            AND 
                                SN = AE.SN) 
                        AND (
                            SELECT  
                                MAX (TIMESTAMP) 
                            FROM 
                                SVC_PHM_ODS.PHM_ODS_APS_ERRORS 
                            WHERE 
                                -- RUN_DATE = V_RUN_DATE 
                                RUN_DATE = TO_DATE('30-OCT-2019')
                            AND 
                                -- BATCH_NUM = V_BATCH_NUM 
                                BATCH_NUM = 'BTH0600'
                            AND 
                                SN = AE.SN 
                            and 
                                TRUNC(TIMESTAMP) <> TRUNC(SYSDATE)
                            )
                    AND 
                        -- AE.MESSAGE LIKE '%' || TH.PATTERN_TEXT  || '%' AND PSM.MESSAGE LIKE '%' || TH.PATTERN_TEXT  || '%' 
                        AE.MESSAGE LIKE '%' || 'Carrier Routing Error - Decapper Gate' || '%' 
                    AND 
                        PSM.MESSAGE LIKE '%' || 'Carrier Routing Error - Decapper Gate' || '%' 

                    and 
                        AE.SN = PSM.IOM_SN 
                    GROUP BY 
                        DEVICE_ID, 
                        PSM.PL, 
                        PSM.SN, 
                        TRUNC(TIMESTAMP) 
                    ORDER BY 
                        IOM_SN, 
                        MODULE_SN, 
                        FLAG_DATE
