CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_ODS_PRESSURES_DIS_PROC (
   V_PROD_ODS_ROUTINE_SK    NUMBER,
   V_RUN_DATE               DATE,
   V_BATCH_NUM              VARCHAR2,
   V_UNIX_ID                VARCHAR)
AS
   V_ODS_NAME         VARCHAR2 (50);
   V_END_KEY          NUMBER (30);
   V_START_KEY        NUMBER (30);
   V_ERROR_MESSAGE    VARCHAR2 (500);
   V_ROUTINE_NAME     VARCHAR2 (30);
   V_ROUTINE_TYPE     VARCHAR2 (30);
   V_PROCESS_TYPE     VARCHAR2 (30);
   V_RUN_MODE         VARCHAR2 (30);
   V_TABLE_NAME       VARCHAR2 (30);
   V_PROCESS_STATUS   VARCHAR2 (20);
   V_STATUS           VARCHAR2 (20);
   NUM_ROWS           NUMBER (10) := 0;
   V_START_DATE       DATE;
   V_END_DATE         DATE;
   V_RUN_TYPE         VARCHAR2 (15);
   V_PROCESS_ID       NUMBER;
   V_PROD_FAMILY      VARCHAR2 (25);
   
   TYPE T_FILEID is table of IDAOWNER.PRESSURES_DIS.FILEID%TYPE;
   TYPE T_DEVICEID is table of IDAOWNER.PRESSURES_DIS.DEVICEID%TYPE;
   TYPE T_MODULEID is table of IDAOWNER.PRESSURES_DIS.MODULEID%TYPE;
   TYPE T_REPLICATEID is table of IDAOWNER.PRESSURES_DIS.REPLICATEID%TYPE;
   TYPE T_COMPLETIONDATE is table of IDAOWNER.PRESSURES_DIS.COMPLETIONDATE%TYPE;
   TYPE T_MODULESNDRM is table of IDAOWNER.PRESSURES_DIS.MODULESNDRM%TYPE;
   TYPE T_LOGFIELD24 is table of IDAOWNER.PRESSURES_DIS.LOGFIELD24%TYPE;
   TYPE T_LOGFIELD25 is table of IDAOWNER.PRESSURES_DIS.LOGFIELD25%TYPE;
   TYPE T_LOADDATE is table of IDAOWNER.PRESSURES_DIS.LOADDATE%TYPE;
   TYPE T_RESULTCODE is table of IDAOWNER.PRESSURES_DIS.RESULTCODE%TYPE;
   
   V_FILEID          T_FILEID;                   
   V_DEVICEID        T_DEVICEID;      
   V_MODULEID        T_MODULEID;
   V_REPLICATEID     T_REPLICATEID;
   V_COMPLETIONDATE  T_COMPLETIONDATE;  
   V_MODULESNDRM     T_MODULESNDRM;
   V_LOGFIELD24      T_LOGFIELD24;
   V_LOGFIELD25      T_LOGFIELD25;
   V_LOADDATE        T_LOADDATE;
   V_RESULTCODE      T_RESULTCODE;     
   
   c_limit PLS_INTEGER := 500;
  
   CURSOR pressures_dis_cur(V_START_DT DATE, V_END_DT DATE)
    IS
      -- Included partitioned column to improve performance of the select, also restricting the data set to last 7 days per algorithm needs
        SELECT FILEID, DEVICEID, MODULEID, REPLICATEID, COMPLETIONDATE, MODULESNDRM, LOGFIELD24, LOGFIELD25, LOADDATE, RESULTCODE 
              FROM IDAOWNER.PRESSURES_DIS WHERE COMPLETIONDATE > SYSDATE - 7 AND FILEID IN
                 (SELECT FILEID FROM IDAOWNER.IDALOGFILES WHERE LOADENDTIME BETWEEN V_START_DT and V_END_DT);        
      
   
BEGIN
   V_ODS_NAME := 'PHM_ODS_PRESSURES_DIS';

   /* TO GET THE BASIC DETAILS OF ODS PROCEDURE */
   PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_DETAILS (V_PROD_ODS_ROUTINE_SK,
                                                  V_ROUTINE_TYPE,
                                                  V_PROCESS_TYPE,
                                                  V_ROUTINE_NAME,
                                                  V_RUN_MODE,
                                                  V_TABLE_NAME,
                                                  V_PROD_FAMILY);
   V_TABLE_NAME := 'PHM_ODS_PRESSURES_DIS';

   IF V_ROUTINE_NAME IS NOT NULL
   THEN
      -- GET TEH ODS PREVIOUS EXECUTION DETAILS
      PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_EXEC_DETAILS(V_PROD_ODS_ROUTINE_SK, V_RUN_DATE, V_BATCH_NUM,'KEY_VALUE', V_ROUTINE_TYPE, V_ROUTINE_NAME,
            V_TABLE_NAME, V_START_KEY, V_END_KEY, V_START_DATE, V_END_DATE, V_RUN_TYPE, V_STATUS, V_PROCESS_ID);
      
      V_START_DATE := TO_DATE (V_START_KEY, 'YYYYMMDDHH24MISS');
      V_END_DATE := TO_DATE (V_END_KEY, 'YYYYMMDDHH24MISS');
      
      DBMS_OUTPUT.PUT_LINE('V_RUN_TYPE: ' || V_RUN_TYPE || ', V_STATUS: ' || V_STATUS || ', V_START_KEY: ' || V_START_KEY || ', V_END_KEY: ' || V_END_KEY);

      IF V_RUN_TYPE IS NULL
      THEN
         -- for the first time run, load the last 2 hours data
         V_END_DATE := SYSDATE - 2 / 24;
         V_RUN_TYPE := 'NEW_RUN';
      END IF;

      IF ( V_RUN_TYPE IN ('RE_RUN')  AND V_STATUS NOT  IN ('COMPLETED','STARTED','FAILED' )) OR
         ( V_RUN_TYPE IN ('NEW_RUN') AND V_STATUS NOT  IN ('FAILED') )  THEN
         V_PROCESS_STATUS := 'STARTED';
         IF V_RUN_TYPE = 'RE_RUN' THEN
              BEGIN
                -- DELETE THE OLD DATA OF CURRENT INSTANCE IF IT IS A RE_RUN
                DELETE FROM  SVC_PHM_ODS.PHM_ODS_PRESSURES_DIS WHERE BATCH_NUM = V_BATCH_NUM AND RUN_DATE = V_RUN_DATE;
                COMMIT;
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                 V_ERROR_MESSAGE := V_ERROR_MESSAGE || 'NO OLD DATA FOUND ';

                WHEN OTHERS THEN
                 V_PROCESS_STATUS := 'ERRORED';
                 V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA FROM PHM_ODS_PRESSURES_DIS FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                 PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                 PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
              END;  


         END IF;


         IF V_RUN_TYPE = 'NEW_RUN' THEN
            BEGIN
               DBMS_OUTPUT.PUT_LINE('V_START_DATE: ' || TO_CHAR(V_START_DATE , 'YYYYMMDDHH24MISS') || ', V_END_DATE: ' || TO_CHAR(V_END_DATE , 'YYYYMMDDHH24MISS') || ', Diff in hrs: ' || (V_END_DATE - V_START_DATE) * 24);
               -- GET START KEY AND END KEY FOR ODS EXECUTION
               
                               -- GET START KEY AND END KEY FOR ODS EXECUTION
                SELECT MIN(A.LOADENDTIME),MAX(A.LOADENDTIME) INTO V_START_DATE, V_END_DATE
                FROM (SELECT FILEID,FILESTRUCTURE,FILENAME,PROCESSCODE,LOADSTARTTIME,LOADENDTIME FROM IDAOWNER.IDALOGDETAILS 
                WHERE PROCESSCODE='PD' AND LOADENDTIME >  V_END_DATE AND DATATYPE = 'PM') A, 
                (SELECT FILEID,FILENAME,PROCESSFLAG,FILESOURCEDATE FROM IDAOWNER.IDALOGFILES WHERE PROCESSFLAG='P' AND LOADENDTIME >  V_END_DATE ) B
                WHERE A.FILEID=B.FILEID;
                
                DBMS_OUTPUT.PUT_LINE('V_START_DATE: ' || TO_CHAR(V_START_DATE , 'YYYYMMDDHH24MISS') || ', V_END_DATE: ' || TO_CHAR(V_END_DATE , 'YYYYMMDDHH24MISS') || ', Diff: ' || (V_END_DATE - V_START_DATE));
                
                IF (V_START_DATE < SYSDATE - 7) THEN
                    V_START_DATE := SYSDATE - 5;
                    DBMS_OUTPUT.PUT_LINE('V_START_DATE after update: ' || TO_CHAR(V_START_DATE , 'YYYYMMDDHH24MISS'));
                END IF;
                -- IF the start date is more than a day ago, load only 6 hours of data incrementally
                IF (V_START_DATE < SYSDATE - 1) THEN
                    DBMS_OUTPUT.PUT_LINE('Last retrieval is more than a day ago. Loading 6 hours data incrementally!!');
                    V_END_DATE := V_START_DATE + (6/24);
                     DBMS_OUTPUT.PUT_LINE('V_END_DATE after update: ' || TO_CHAR(V_END_DATE , 'YYYYMMDDHH24MISS'));
                END IF; 

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                     V_PROCESS_STATUS := 'NO_NEW_DATA';
                     V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTAM AFTER FILEID '||V_END_KEY|| ' AT '||SYSDATE;
                     PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                     PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
                WHEN OTHERS THEN
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE := 'NOT ABLE TO GET THE START KEY AND END KEY VALUES FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
            END; 
                

         END IF;

         V_START_KEY := TO_CHAR (V_START_DATE, 'YYYYMMDDHH24MISS');
         V_END_KEY := TO_CHAR (V_END_DATE, 'YYYYMMDDHH24MISS');

         IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA')  AND V_RUN_TYPE IN ('RE_RUN', 'NEW_RUN') THEN
           -- LOG THE DETAILS FOR THE CURRENT INSTANCE OF THE ODS  
            V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
            V_START_KEY := TO_CHAR(V_START_DATE , 'YYYYMMDDHH24MISS'); 
            V_END_KEY := TO_CHAR(V_END_DATE , 'YYYYMMDDHH24MISS');
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,NULL,V_RUN_DATE,SYSDATE,V_BATCH_NUM,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK);
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,NULL,NULL,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);
            COMMIT;
         END IF; 

         IF V_PROCESS_STATUS NOT IN ('ERRORED', 'NO_NEW_DATA') THEN
            -- INSERTING THE DATA INTO ODS TABLES...
            IF V_START_DATE <= V_END_DATE THEN
                
                DBMS_OUTPUT.PUT_LINE('Fetching data from Cursor!!');
                OPEN pressures_dis_cur(V_START_DATE, V_END_DATE);
                LOOP
                    BEGIN
                        FETCH pressures_dis_cur BULK COLLECT INTO 
                             V_FILEID, V_DEVICEID, V_MODULEID, V_REPLICATEID, V_COMPLETIONDATE, V_MODULESNDRM, V_LOGFIELD24, V_LOGFIELD25, V_LOADDATE, V_RESULTCODE
                        LIMIT c_limit;                                         
                        
                        --DBMS_OUTPUT.PUT_LINE('Looping: c%rowcount = ' || pressures_dis_cur%ROWCOUNT || ', pd_data: ' || pd_data.count);

                        FORALL i in 1 .. V_FILEID.count
                            INSERT INTO SVC_PHM_ODS.PHM_ODS_PRESSURES_DIS(BATCH_NUM, RUN_DATE, ZIPID, FILEID, FILENAME, DEVICEID, MODULEID, REPLICATEID, COMPLETIONDATE, MODULESNDRM
                                , LOGFIELD24, LOGFIELD25, LOADDATE, RESULTCODE, PHM_ODS_CREATE_USER, PHM_ODS_CREATE_DATE, PHM_ODS_MODIFY_USER, PHM_ODS_MODIFY_DATE)
                            VALUES (V_BATCH_NUM, V_RUN_DATE, NULL, V_FILEID(i), NULL, V_DEVICEID(i), V_MODULEID(i), V_REPLICATEID(i), V_COMPLETIONDATE(i), V_MODULESNDRM(i)
                                , V_LOGFIELD24(i), V_LOGFIELD25(i), V_LOADDATE(i), V_RESULTCODE(i), V_ODS_NAME, SYSDATE, V_ODS_NAME, SYSDATE); 
                                
                        NUM_ROWS := NUM_ROWS + V_FILEID.count;
                        IF MOD(NUM_ROWS, 10000) = 0 THEN
                           --DBMS_OUTPUT.PUT_LINE('Committed. Total Record Count: ' || NUM_ROWS); 
                           COMMIT;
                        END IF;                                        
                                
                        EXIT WHEN pressures_dis_cur%NOTFOUND;    
                    EXCEPTION
                        WHEN OTHERS THEN
                        V_PROCESS_STATUS := 'ERRORED';
                        V_ERROR_MESSAGE := 'NOT ABLE TO INSERT THE DATA INTO PHM_ODS_PRESSURES_DIS FOR THE BATCH_NUM ' || V_BATCH_NUM || ' DUE TO ' || SQLERRM;
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
                        COMMIT;
                        EXIT;
                    END;            
               END LOOP;
               CLOSE pressures_dis_cur;
               DBMS_OUTPUT.PUT_LINE('Total Records Inserted: ' || NUM_ROWS);                    
            ELSE
                  -- NO NEW FILES
                  V_PROCESS_STATUS := 'NO_NEW_DATA';
                  V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTEM AFTER V_END_DATE ' || V_END_KEY || ' AT ' || SYSDATE;
                  PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE,NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                  PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
            
            END IF;
         END IF;
      ELSE
         -- SUBMITTED A ODS REQUEST WHICH IS IN IN-PROGRESS OR COMPLETED STATUS 
         V_ERROR_MESSAGE := 'NOT ABLE TO RUN ODS AS THE SUBMITTED ODS IS IN  ' || V_STATUS || ' STATUS';
         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,V_PROD_ODS_ROUTINE_SK,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,V_ODS_NAME);
      END IF;
   ELSE
        -- NO ODS DATA PRESENT
        V_ERROR_MESSAGE := 'NOT ABLE TO FETCH THE BASIC INFORMATION FOR PRESSURES_DIS_ODS DUE TO ' || SQLERRM;
        PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,V_PROD_ODS_ROUTINE_SK,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,V_ODS_NAME);
   END IF;
   
   IF V_PROCESS_STATUS NOT IN ('NO_NEW_DATA','ERRORED') THEN
      -- MAKING THE ODS COMPLETED 
      V_PROCESS_STATUS := 'COMPLETED';
      V_ERROR_MESSAGE := NULL;
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
   END IF; 
 
   COMMIT;              
   

EXCEPTION
    WHEN OTHERS THEN
      V_ERROR_MESSAGE := 'NOT ABLE TO EXECUTE THE PROCEDURE PHM_ODS_PRESSURES_DIS_PROC DUE TO ' || SQLERRM;
      V_PROCESS_STATUS := 'ERRORED';
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
 
END PHM_ODS_PRESSURES_DIS_PROC;
/