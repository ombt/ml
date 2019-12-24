-- 
--     CURSOR CURAPS_DEVICE_LAS (V_SN VARCHAR2, 
--                               V_START_DATE DATE, 
--                               V_END_DATE DATE, 
--                               LAS_PATTERN_TEXT VARCHAR2)
--     IS
--         SELECT 
--             MAX(A.DEVICE_ID) DEVICE_ID, 
--             MAX(A.IOM_SN) IOM_SN, 
--             MAX(A.PL) PL, 
--             MAX(A.SN) SN, 
--             A.DT, 
--             MAX(A.TIMESTAMP) MAX_TIMESTAMP, 
--             COUNT(*) LAS_ERROR_COUNT 
--         FROM (
--             SELECT 
--                 AE.DEVICE_ID, 
--                 PSM.IOM_SN, 
--                 PSM.PL, 
--                 PSM.SN, 
--                 TRUNC(AE.TIMESTAMP) DT, 
--                 AE.TIMESTAMP 
--             from 
--                 SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, 
--                 SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM 
--             WHERE 
--                AE.SN = V_SN 
--             AND 
--                 AE.MESSAGE = LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' 
--             AND 
--                 TIMESTAMP BETWEEN 
--                     V_START_DATE 
--                 AND 
--                     V_END_DATE
--             AND 
--                 PSM.MESSAGE LIKE LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' || '%' 
--             and 
--                 AE.SN = PSM.IOM_SN
--         ) A, 
--         (
--             SELECT 
--                 DEVICE_ID, 
--                 SN, 
--                 TRUNC(TIMESTAMP) DT, 
--                 TIMESTAMP 
--             from 
--                 SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE 
--             WHERE 
--                 SN = V_SN 
--             AND 
--                 MESSAGE like 'Interface Module Unreadable Barcode -%' 
--             AND 
--                 TIMESTAMP BETWEEN 
--                     V_START_DATE 
--                 AND 
--                     V_END_DATE
--         ) B
--     WHERE 
--         A.TIMESTAMP = B.TIMESTAMP
--     GROUP BY 
--         A.DT 
--     ORDER BY 1, 2, 3;

--         to_timestamp('09/16/2019 00:00:00', 
--                      'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
--     and 
--         r.logdate_local < to_timestamp('09/17/2019 00:00:00', 
--                                        'MM/DD/YYYY HH24:MI:SS')
        SELECT 
            MAX(A.DEVICE_ID) DEVICE_ID, 
            MAX(A.IOM_SN) IOM_SN, 
            MAX(A.PL) PL, 
            MAX(A.SN) SN, 
            A.DT, 
            MAX(A.TIMESTAMP) MAX_TIMESTAMP, 
            COUNT(*) LAS_ERROR_COUNT 
        FROM (
            SELECT 
                AE.DEVICE_ID, 
                PSM.IOM_SN, 
                PSM.PL, 
                PSM.SN, 
                TRUNC(AE.TIMESTAMP) DT, 
                AE.TIMESTAMP 
            from 
                SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, 
                SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM 
            WHERE 
                -- AE.MESSAGE = LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' 
            (
                (AE.MESSAGE LIKE 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (AE.MESSAGE LIKE 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (AE.MESSAGE LIKE 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (AE.MESSAGE LIKE 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (AE.MESSAGE LIKE 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (AE.MESSAGE LIKE 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (AE.MESSAGE LIKE 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (AE.MESSAGE LIKE 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) 
            )
            AND 
                TIMESTAMP BETWEEN 
                    -- V_START_DATE 
                    to_timestamp('09/18/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
                AND 
                    -- V_END_DATE
                    to_timestamp('09/19/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
            AND 
                -- PSM.MESSAGE LIKE LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' || '%' 
            (
                (PSM.MESSAGE LIKE 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (PSM.MESSAGE LIKE 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (PSM.MESSAGE LIKE 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (PSM.MESSAGE LIKE 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (PSM.MESSAGE LIKE 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (PSM.MESSAGE LIKE 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (PSM.MESSAGE LIKE 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
                (PSM.MESSAGE LIKE 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) 
            )
            and 
                AE.SN = PSM.IOM_SN
        ) A, 
        (
            SELECT 
                DEVICE_ID, 
                SN, 
                TRUNC(TIMESTAMP) DT, 
                TIMESTAMP 
            from 
                SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE 
            WHERE 
                MESSAGE like 'Interface Module Unreadable Barcode -%' 
            AND 
                TIMESTAMP BETWEEN 
                    -- V_START_DATE 
                    to_timestamp('09/18/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
                AND 
                    -- V_END_DATE
                    to_timestamp('09/19/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
        ) B
    WHERE 
        A.TIMESTAMP = B.TIMESTAMP
    GROUP BY 
        A.DT 
    ORDER BY 1, 2, 3;
