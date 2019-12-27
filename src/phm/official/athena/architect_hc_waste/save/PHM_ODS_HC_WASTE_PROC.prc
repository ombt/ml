CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_ODS_HC_WASTE_PROC(V_PROD_ODS_ROUTINE_SK NUMBER,V_RUN_DATE DATE, V_BATCH_NUM VARCHAR2,V_UNIX_ID VARCHAR)
AS
  
  V_ODS_NAME VARCHAR2(50);
  V_END_KEY NUMBER(30);
  V_START_KEY NUMBER(30);
  V_ERROR_MESSAGE VARCHAR2(500);
  V_ROUTINE_NAME VARCHAR2(30);
  V_ROUTINE_TYPE VARCHAR2(30);
  V_PROCESS_TYPE VARCHAR2(30);
  V_RUN_MODE VARCHAR2(30);
  V_TABLE_NAME VARCHAR2(30);
  V_PROCESS_STATUS VARCHAR2(20);
  V_STATUS VARCHAR2(20);
  NUM_ROWS NUMBER(10):= 0 ;
  V_START_DATE  DATE; 
  V_END_DATE DATE;
  V_RUN_TYPE VARCHAR2(15);
  V_PROCESS_ID NUMBER;
  V_PROD_FAMILY VARCHAR2(25);
  
BEGIN
--what function does V_ODS_NAME have and what is difference with V_TABLE_NAME?
  V_ODS_NAME := 'HC_WASTE_ODS';
  
  /* TO GET THE BASIC DETAILS OF ODS PROCEDURE */
  PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_DETAILS(V_PROD_ODS_ROUTINE_SK,V_ROUTINE_TYPE,V_PROCESS_TYPE,V_ROUTINE_NAME,V_RUN_MODE,V_TABLE_NAME,V_PROD_FAMILY);
  V_TABLE_NAME := 'PHM_ODS_HC_WASTE';
    
  IF V_ROUTINE_NAME IS NOT NULL THEN
    -- GET THE ODS PREVIOUS EXECUTION DETAILS
    PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_EXEC_DETAILS(V_PROD_ODS_ROUTINE_SK,V_RUN_DATE,V_BATCH_NUM,'KEY_VALUE',V_ROUTINE_TYPE,V_ROUTINE_NAME,
    V_TABLE_NAME,V_START_KEY,V_END_KEY,V_START_DATE,V_END_DATE,V_RUN_TYPE,V_STATUS,V_PROCESS_ID);
    V_START_DATE := TO_DATE(V_START_KEY , 'YYYYMMDDHH24MISS');
    V_END_DATE := TO_DATE(V_END_KEY , 'YYYYMMDDHH24MISS');
    
    
     IF V_RUN_TYPE IS NULL  THEN
        -- for the first time run, load the last 2 hours data
        -- V_END_DATE := TRUNC(SYSDATE - 1);
        
        V_END_DATE := SYSDATE - 60;
        V_RUN_TYPE := 'NEW_RUN';
        
     END IF;
     
     IF ( V_RUN_TYPE IN ('RE_RUN')  AND V_STATUS NOT  IN ('COMPLETED','STARTED','FAILED' )) OR
         ( V_RUN_TYPE IN ('NEW_RUN') AND V_STATUS NOT  IN ('FAILED') )  THEN
          V_PROCESS_STATUS := 'STARTED';
          IF V_RUN_TYPE = 'RE_RUN' THEN
              BEGIN
                -- DELETE THE OLD DATA OF CURRENT INSTANCE IF IT IS A RE_RUN
                DELETE FROM  SVC_PHM_ODS.PHM_ODS_HC_WASTE WHERE BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE;
                COMMIT;
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                 V_ERROR_MESSAGE := V_ERROR_MESSAGE || 'NO OLD DATA FOUND ';

                WHEN OTHERS THEN
                 V_PROCESS_STATUS := 'ERRORED';
                 V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA FROM PHM_ODS_HC_WASTE FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                 PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                 PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
              
              END;  
          
          END IF;


          IF V_RUN_TYPE = 'NEW_RUN' THEN
              BEGIN 
                -- GET START KEY AND END KEY FOR ODS EXECUTION
                SELECT MIN(A.LOADENDTIME),MAX(A.LOADENDTIME) INTO V_START_DATE, V_END_DATE
                FROM (SELECT FILEID,FILESTRUCTURE,FILENAME,PROCESSCODE,LOADSTARTTIME,LOADENDTIME FROM IDAOWNER.IDALOGDETAILS
                -- if I use 'RESULTS' below (but I need RESULTS_CC + WAVELENGTHREADS), will still update appropriately?
                WHERE PROCESSCODE='PD' AND LOADENDTIME >  V_END_DATE AND DATATYPE = 'RESULTS') A, 
                (SELECT FILEID,FILENAME,PROCESSFLAG,FILESOURCEDATE FROM IDAOWNER.IDALOGFILES WHERE PROCESSFLAG='P' AND LOADENDTIME >  V_END_DATE ) B
                WHERE A.FILEID=B.FILEID;
                
               -- V_END_KEY := V_START_KEY  + 100;
              EXCEPTION
                 WHEN NO_DATA_FOUND THEN
                      V_PROCESS_STATUS := 'NO_NEW_DATA';
                      V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTAM AFTER DATE  '||V_END_DATE|| ' AT '||SYSDATE;
                      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
                 WHEN OTHERS THEN
                     V_PROCESS_STATUS := 'ERRORED';
                     V_ERROR_MESSAGE := 'NOT ABLE TO GET THE START KEY AND END KEY VALUES FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                     PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                     PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
              END; 
          END IF;
          
          V_START_KEY := TO_CHAR(V_START_DATE , 'YYYYMMDDHH24MISS'); 
          V_END_KEY := TO_CHAR(V_END_DATE , 'YYYYMMDDHH24MISS');
          --start date is last time ran
          
          DBMS_OUTPUT.PUT_LINE('V_START_KEY: ' || V_START_KEY || 'V_END_KEY: ' || V_END_KEY);
          
          IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA')  AND V_RUN_TYPE = 'NEW_RUN' THEN
           -- LOG THE DETAILS FOR THE CURRENT INSTANCE OF THE ODS  
            V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,' ',V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS, '', NULL,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);
            COMMIT;
          END IF; 
          
          IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA') THEN  
              -- INSERTING THE DATA INTO ODS TABLES...
              IF V_START_KEY <= V_END_KEY THEN
              FOR ER_ROW IN  ( 
                    SELECT
                      CC.FILEID, 
                      CC.DEVICEID,
                      CC.MODULEID,
                      CC.MODULESNDRM,
                      CC.COMPLETIONDATE,
                      CC.REPLICATEID,
                      WLR.READ29,
                      WLR.READ30,
                      WLR.READ31,
                      WLR.READ32,
                      WLR.READ33 
                    FROM 
                      IDAOWNER.RESULTS_CC CC,
                      IDAOWNER.WAVELENGTHREADS WLR
                    WHERE 
                    CC.COMPLETIONDATE = WLR.COMPLETIONDATE AND
                    CC.PARENTID = WLR.PARENTID AND
                    CC.EXCEPTIONCODE = 1053 AND
                    WLR.READCOUNT = 34 AND
                    WLR.READTYPE = 'PRIMARY_WAVELENGTH_READS' AND
                    CC.FILEID IN (SELECT FILEID FROM IDAOWNER.IDALOGFILES WHERE LOADENDTIME BETWEEN V_START_DATE AND V_END_DATE))
                    
--                  FOR ER_ROW IN  ( SELECT * FROM IDAOWNER.RESULTS_CC WHERE FILEID IN 
--                                  (SELECT FILEID FROM IDAOWNER.IDALOGFILEs WHERE LOADENDTIME BETWEEN V_START_DATE AND V_END_DATE))
                                    --and rownum < 11)
                  LOOP
                       BEGIN
                           INSERT INTO SVC_PHM_ODS.PHM_ODS_HC_WASTE VALUES (V_BATCH_NUM,V_RUN_DATE,NULL,ER_ROW.FILEID,'',
                           ER_ROW.DEVICEID,
                           ER_ROW.MODULEID,
                           ER_ROW.MODULESNDRM,
                           ER_ROW.COMPLETIONDATE,
                           ER_ROW.REPLICATEID,
                           ER_ROW.READ29,
                           ER_ROW.READ30,
                           ER_ROW.READ31,
                           ER_ROW.READ32,
                           ER_ROW.READ33,
                           V_ODS_NAME,
                           SYSDATE,
                           V_ODS_NAME,
                           SYSDATE);
                           
                           NUM_ROWS:= NUM_ROWS + 1;
                           
                           IF MOD(NUM_ROWS,10000) = 0 THEN
                             COMMIT;
                           END IF;
                           
                       EXCEPTION
                          WHEN OTHERS THEN
                             V_PROCESS_STATUS := 'ERRORED';
                             V_ERROR_MESSAGE := 'NOT ABLE TO INSERT THE DATA INTO PHM_ODS_HC_WASTE FOR THE BATCH_NUM ' || V_BATCH_NUM ||'DEVICE ID' ||ER_ROW.DEVICEID ||' DUE TO ' || SQLERRM;
                             PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                             PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
                             COMMIT;
                             EXIT;
                       END;
                  END LOOP;
              ELSE 
                  -- NO NEW FILES
                  V_PROCESS_STATUS := 'NO_NEW_DATA';
                  V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTEM AFTER FILEID '||V_END_KEY|| ' AT '||SYSDATE;
                  PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE,NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                  PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
              END IF; 
          END IF;   

     ELSE
         -- SUBMITTED A ODS REQUEST WHICH IS IN IN-PROGRESS OR COMPLETED STATUS 
         V_ERROR_MESSAGE := 'NOT ABLE TO RUN ODS AS THE SUBMITTED HC_WASTE_ODS IS IN  ' || V_STATUS ||' STATUS';
         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,V_PROD_ODS_ROUTINE_SK,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,V_ODS_NAME);
     END IF;
  ELSE
    -- NO ODS DATA PRESENT
    V_ERROR_MESSAGE := 'NOT ABLE TO FETCH THE BASIC INFORMATION FOR HC_WASTE_ODS DUE TO ' || SQLERRM;
    PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,V_PROD_ODS_ROUTINE_SK,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,V_ODS_NAME);
  END IF;
  
   IF V_PROCESS_STATUS NOT IN ('NO_NEW_DATA','ERRORED') THEN
     -- MAKING THE ODS COMPLETED 
    V_PROCESS_STATUS := 'COMPLETED';
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
   END IF; 
 
   COMMIT; 
EXCEPTION
    WHEN OTHERS THEN
      V_ERROR_MESSAGE := 'NOT ABLE TO EXECUTE THE PROCEDURE PHM_ODS_HC_WASTE_PROC DUE TO ' || SQLERRM;
      V_PROCESS_STATUS := 'ERRORED';
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
 
END PHM_ODS_HC_WASTE_PROC;
/