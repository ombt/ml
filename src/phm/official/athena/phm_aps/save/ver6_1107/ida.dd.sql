

--   CURSOR CURAPS_DEVICES_DATES -- ( STARTDATE DATE, ENDDATE DATE)
       SELECT  
           SN, 
           MIN(TRUNC(TIMESTAMP)) DT, 
           MAX(TIMESTAMP) DT_MAX
       FROM 
           SVC_PHM_ODS.PHM_ODS_APS_ERRORS
       WHERE 
           -- RUN_DATE = V_RUN_DATE 
           RUN_DATE = TO_DATE('05-NOV-2019')
       AND 
           -- BATCH_NUM = V_BATCH_NUM  
           BATCH_NUM = 'BTH0600'
       AND 
           -- TRUNC(TIMESTAMP) <> TRUNC(SYSDATE) 
           TRUNC(TIMESTAMP) <> TRUNC(TO_DATE('05-NOV-2019'))
       GROUP BY 
           SN 
       ORDER BY 1,2;


