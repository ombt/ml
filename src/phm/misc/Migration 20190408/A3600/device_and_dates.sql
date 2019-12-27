-- 
--    CURSOR DEVICE_AND_DATES (
--       V_NODETYPE1     VARCHAR2,
--       V_ERRORCODE1    VARCHAR2)
--    IS
-- 
SELECT 
    ASI.PRODUCTLINEREF,
    ASI.DEVICEID,
    ASI.SYSTEMSN,
    ALN.SN,
    AE.NODETYPE,
    AE.ERRORCODE,
    MAX (AE.COMPLETIONDATE) MAX_COMPL_DATE,
    TRUNC (MIN (AE.COMPLETIONDATE)) MIN_COMPL_DATE
FROM 
    SVC_PHM_ODS.PHM_ODS_A3600_ERRORS AE,
    A3600_LAYOUT_NODES_PL_SN ALN,
    IDAOWNER.A3600SYSTEMINFORMATION ASI
WHERE     
    BATCH_NUM = V_BATCH_NUM
AND 
    RUN_DATE = V_RUN_DATE
AND 
    AE.ERRORCODE = V_ERRORCODE1
AND 
    AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID
AND 
    ALN.SYSTEMINFOID = ASI.SYSTEMINFOID
AND 
    ALN.SN IS NOT NULL
AND 
    ALN.CANID = AE.NODEID
AND 
    ASI.CURRENT_ROW = 'Y'
AND 
    ((V_NODETYPE1 != '%' AND AE.NODETYPE = V_NODETYPE1) OR 
     (V_NODETYPE1 = '%' AND AE.NODETYPE LIKE V_NODETYPE1))
GROUP BY 
    ASI.PRODUCTLINEREF,
    ASI.DEVICEID,
    ASI.SYSTEMSN,
    ALN.SN,
    AE.NODETYPE,
    AE.ERRORCODE
ORDER BY 
    ASI.SYSTEMSN, 
    AE.NODETYPE, 
    AE.ERRORCODE