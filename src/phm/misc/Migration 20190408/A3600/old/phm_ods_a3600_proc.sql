CREATE OR REPLACE PROCEDURE 
SVC_PHM_OWNER.PHM_ODS_A3600_PROC(V_PROD_ODS_ROUTINE_SK NUMBER,
                                 V_RUN_DATE DATE,
                                 V_BATCH_NUM VARCHAR2,
                                 V_UNIX_ID VARCHAR2)
AS
  
    V_ODS_NAME       VARCHAR2(50);
    V_PREV_END_KEY   NUMBER(10);
    V_END_KEY        NUMBER(10);
    V_START_KEY      NUMBER(10);
    V_ERROR_MESSAGE  VARCHAR2(500);
    V_ROUTINE_NAME   VARCHAR2(30);
    V_ROUTINE_TYPE   VARCHAR2(30);
    V_PROCESS_TYPE   VARCHAR2(30);
    V_RUN_MODE       VARCHAR2(30);
    V_TABLE_NAME     VARCHAR2(30);
    V_PROCESS_STATUS VARCHAR2(20);
    V_STATUS         VARCHAR2(20);
    V_NUM_ROWS       NUMBER(10) := 0;
    V_START_DATE     DATE; 
    V_END_DATE       DATE;
    V_RUN_TYPE       VARCHAR2(15);
    V_PROD_FAMILY    VARCHAR2(15);
    V_PROCESS_ID     VARCHAR2(25);
  
    TYPE T_ZIPID is table of IDAOWNER.A3600_LOG_FILE_DETAILS.ZIPID%TYPE;
    TYPE T_FILEID is table of IDAOWNER.A3600_LOG_FILE_DETAILS.FILEID%TYPE;
    TYPE T_FILE_NAME is table of IDAOWNER.A3600_LOG_FILE_DETAILS.FILE_NAME%TYPE;
    TYPE T_ERROR_ID is table of IDAOWNER.A3600_ERRORS.ERROR_ID%TYPE;
    TYPE T_LAYOUT_NODES_ID is table of IDAOWNER.A3600_ERRORS.LAYOUT_NODES_ID%TYPE;
    TYPE T_NODEID is table of IDAOWNER.A3600_ERRORS.NODEID%TYPE;
    TYPE T_NODETYPE is table of IDAOWNER.A3600_ERRORS.NODETYPE%TYPE;
    TYPE T_INSTANCEID is table of IDAOWNER.A3600_ERRORS.INSTANCEID%TYPE;
    TYPE T_COMPLETIONDATE is table of IDAOWNER.A3600_ERRORS.COMPLETIONDATE%TYPE;
    TYPE T_ERRORCODE is table of IDAOWNER.A3600_ERRORS.ERRORCODE%TYPE;
    TYPE T_SAMPLEID is table of IDAOWNER.A3600_ERRORS.SAMPLEID%TYPE;
    TYPE T_MOREINFO is table of IDAOWNER.A3600_ERRORS.MOREINFO%TYPE;
    TYPE T_OFF_LINE is table of IDAOWNER.A3600_ERRORS.OFF_LINE%TYPE;
    TYPE T_LOADDATE is table of IDAOWNER.A3600_ERRORS.LOADDATE%TYPE;

    V_ZIPID              T_ZIPID;
    V_FILEID             T_FILEID;
    V_FILE_NAME          T_FILE_NAME;
    V_ERROR_ID           T_ERROR_ID;
    V_LAYOUT_NODES_ID    T_LAYOUT_NODES_ID;
    V_NODEID             T_NODEID;
    V_NODETYPE           T_NODETYPE;
    V_INSTANCEID         T_INSTANCEID;
    V_COMPLETIONDATE     T_COMPLETIONDATE;
    V_ERRORCODE          T_ERRORCODE;
    V_SAMPLEID           T_SAMPLEID;
    V_MOREINFO           T_MOREINFO;
    V_OFF_LINE           T_OFF_LINE;
    V_LOADDATE           T_LOADDATE;
        
    c_limit PLS_INTEGER := 500;
  
    CURSOR a3600_errors_cur(C_START_KEY NUMBER, C_END_KEY NUMBER)
    IS
        SELECT 
            FD.ZIPID, 
            FD.FILEID, 
            FD.FILE_NAME, 
            E.ERROR_ID, 
            E.LAYOUT_NODES_ID, 
            E.NODEID, 
            E.NODETYPE,
            E.INSTANCEID,
            E.COMPLETIONDATE,
            E.ERRORCODE,
            E.SAMPLEID,
            E.MOREINFO,
            E.OFF_LINE,
            E.LOADDATE
       FROM 
           IDAOWNER.A3600_LOG_FILES F, 
           IDAOWNER.A3600_LOG_FILE_DETAILS FD,
           (
               SELECT 
                   ASI.DEVICEID, 
                   AE.* 
               FROM 
                   IDAOWNER.A3600_ERRORS  AE, 
                   IDAOWNER.A3600_LAYOUT_NODES ALN, 
                   IDAOWNER.A3600SYSTEMINFORMATION ASI
               WHERE 
                   AE.LAYOUT_NODES_ID = ALN.LAYOUT_NODES_ID 
               AND 
                   ALN.SYSTEMINFOID = ASI.SYSTEMINFOID 
               AND 
                   COMPLETIONDATE > TRUNC(SYSDATE) - 31
           ) E  
       WHERE 
           F.ZIPID = FD.ZIPID 
       AND 
           FD.DEVICE_ID = E.DEVICEID 
       AND 
           F.ZIPID BETWEEN C_START_KEY 
       AND 
           C_END_KEY 
       AND  
           F.PROCESSCODE ='P'
       AND 
           SUBSTR(E.FILENAME,(LENGTH(E.FILENAME) - INSTR(REVERSE(E.FILENAME),'/')) + 2, LENGTH(E.FILENAME)) = FD.FILE_NAME;
                   
    BEGIN
        V_ODS_NAME := 'A3600_ODS';
  
        /* TO GET THE BASIC DETAILS OF ODS PROCEDURE */
        PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_DETAILS(V_PROD_ODS_ROUTINE_SK,
                                                      V_ROUTINE_TYPE,
                                                      V_PROCESS_TYPE,
                                                      V_ROUTINE_NAME,
                                                      V_RUN_MODE,
                                                      V_TABLE_NAME,
                                                      V_PROD_FAMILY);
        V_TABLE_NAME := 'PHM_ODS_A3600_ERRORS';
  
        IF V_ROUTINE_NAME IS NOT NULL 
        THEN
            /* TO GET ODS PREVIOUS EXECUTION DETAILS */
            PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_EXEC_DETAILS(V_PROD_ODS_ROUTINE_SK,
                                                               V_RUN_DATE,
                                                               V_BATCH_NUM,
                                                               'KEY_VALUE',
                                                               V_ROUTINE_TYPE,
                                                               V_ROUTINE_NAME,
                                                               V_TABLE_NAME,
                                                               V_START_KEY,
                                                               V_PREV_END_KEY,
                                                               V_START_DATE,
                                                               V_END_DATE,
                                                               V_RUN_TYPE,
                                                               V_STATUS,
                                                               V_PROCESS_ID);
            -- V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();

            -- DBMS_OUTPUT.PUT_LINE'V_RUN_TYPE: ' || V_RUN_TYPE || ', V_STATUS: ' || V_STATUS || ', V_START_KEY: ' || V_START_KEY || ', V_END_KEY: ' || V_END_KEY);
  
            /* TO GET LAST FEW DAYAS DATA IN CASE OF FIRST RUN */
            IF V_RUN_TYPE IS NULL  
            THEN
                SELECT 
                    MAX(ZIPID) 
                INTO 
                    V_PREV_END_KEY 
                FROM 
                    IDAOWNER.A3600_LOG_FILES 
                WHERE 
                    PROCESS_START_DATE < SYSDATE - 6 
                AND 
                    PROCESSCODE ='P'; 
                V_RUN_TYPE := 'NEW_RUN';
            END IF;


            IF  ( V_RUN_TYPE IN ('RE_RUN')  AND 
                  V_STATUS NOT  IN ('COMPLETED','STARTED','FAILED' )) OR
                ( V_RUN_TYPE IN ('NEW_RUN') AND V_STATUS NOT  IN ('FAILED') )  
            THEN

                V_PROCESS_STATUS := 'STARTED';

                IF V_RUN_TYPE = 'RE_RUN' 
                THEN
                    /* IN CASE OF RE RUN DELETE THE PREVIOUS DATA FROM ODS TABLES */ 
                BEGIN
                    DELETE FROM  
                        SVC_PHM_ODS.PHM_ODS_A3600_ERRORS 
                    WHERE 
                        ZIPID BETWEEN V_START_KEY AND V_PREV_END_KEY
                    AND 
                        BATCH_NUM = V_BATCH_NUM 
                    AND 
                        RUN_DATE = V_RUN_DATE;

                    EXCEPTION WHEN OTHERS 
                    THEN
                        V_PROCESS_STATUS := 'ERRORED';
                        V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA FROM PHM_A3600_ERRORS FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID, 
                                                                                V_RUN_DATE,
                                                                                V_TABLE_NAME,
                                                                                V_START_KEY,
                                                                                V_PREV_END_KEY,
                                                                                V_PROCESS_STATUS,
                                                                                V_ERROR_MESSAGE, 
                                                                                V_NUM_ROWS,
                                                                                    V_BATCH_NUM,
                                                                                V_ODS_NAME,
                                                                                V_PROD_ODS_ROUTINE_SK);     
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                                        V_PROD_FAMILY,
                                                                        V_PROCESS_TYPE,
                                                                        V_ROUTINE_TYPE,
                                                                        V_ODS_NAME,
                                                                        V_ROUTINE_NAME,
                                                                        V_RUN_MODE,
                                                                        V_PROCESS_STATUS,
                                                                        V_ERROR_MESSAGE,
                                                                        V_RUN_DATE ,
                                                                        SYSDATE,
                                                                        V_BATCH_NUM ,
                                                                        V_UNIX_ID,
                                                                        V_PROD_ODS_ROUTINE_SK );
                    END;  
                END IF;

        IF V_RUN_TYPE = 'NEW_RUN' THEN
          /* GET THE START ID AND END ID FOR THE ODS DATA INSERT */ 
           BEGIN 
                SELECT MIN(ZIPID),MAX(ZIPID) INTO V_START_KEY,V_END_KEY FROM IDAOWNER.A3600_LOG_FILES WHERE ZIPID >  V_PREV_END_KEY AND PROCESSCODE  = 'P';
           EXCEPTION
             WHEN NO_DATA_FOUND THEN
                V_PROCESS_STATUS := 'NO_NEW_DATA';
                V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTAM AFTER ZIPID '||V_PREV_END_KEY|| ' AT '||SYSDATE;
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ('',V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_PREV_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, V_NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
             WHEN OTHERS THEN
                V_PROCESS_STATUS := 'ERRORED';
                V_ERROR_MESSAGE := 'NOT ABLE TO GET THE START KEY AND END KEY VALUES FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID, V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_PREV_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, V_NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
           END;
        END IF;
            
        IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA') AND V_RUN_TYPE IN ('RE_RUN', 'NEW_RUN') THEN  
            /* LOG THE AUDIT RECORDS FOR ODS PROCESS */
            V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID(); 
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,' ',V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,'', NULL,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);
            COMMIT;
        END IF; 
          
        IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA') THEN  
               /* INSERTING THE DATA INTO ODS TABLES..... */
           IF V_START_KEY <= V_END_KEY THEN
               --DBMS_OUTPUT.PUT_LINE'Fetching data from Cursor!!');
               OPEN a3600_errors_cur(V_START_KEY, V_END_KEY);
               LOOP
                    BEGIN
                        FETCH a3600_errors_cur BULK COLLECT INTO 
                             V_ZIPID, V_FILEID, V_FILE_NAME, V_ERROR_ID, V_LAYOUT_NODES_ID, V_NODEID, V_NODETYPE, V_INSTANCEID, V_COMPLETIONDATE, V_ERRORCODE, V_SAMPLEID, V_MOREINFO, V_OFF_LINE, V_LOADDATE
                        LIMIT c_limit;                                         
                        
                        ----DBMS_OUTPUT.PUT_LINE'Looping: c%rowcount = ' || pressures_dis_cur%ROWCOUNT || ', pd_data: ' || pd_data.count);

                        FORALL i in 1 .. V_ZIPID.count
                            INSERT INTO SVC_PHM_ODS.PHM_ODS_A3600_ERRORS(BATCH_NUM, RUN_DATE, ZIPID, FILEID, FILENAME, ERROR_ID, LAYOUT_NODES_ID, NODEID
                                    , NODETYPE, INSTANCEID, COMPLETIONDATE, ERRORCODE, SAMPLEID, MOREINFO, OFF_LINE, LOADDATE) 
                            VALUES (V_BATCH_NUM, V_RUN_DATE, V_ZIPID(i), V_FILEID(i), V_FILE_NAME(i), V_ERROR_ID(i), V_LAYOUT_NODES_ID(i), V_NODEID(i)
                                    , V_NODETYPE(i), V_INSTANCEID(i), V_COMPLETIONDATE(i), V_ERRORCODE(i), V_SAMPLEID(i), V_MOREINFO(i), V_OFF_LINE(i), V_LOADDATE(i));
                                
                        V_NUM_ROWS := V_NUM_ROWS + V_ZIPID.count;
                        IF MOD(V_NUM_ROWS, 10000) = 0 THEN
                           ----DBMS_OUTPUT.PUT_LINE'Committed. Total Record Count: ' || V_NUM_ROWS); 
                           COMMIT;
                        END IF; 
                        
                        EXIT WHEN a3600_errors_cur%NOTFOUND;    
                    EXCEPTION
                        WHEN OTHERS THEN
                        V_PROCESS_STATUS := 'ERRORED';
                        V_ERROR_MESSAGE := 'NOT ABLE TO INSERT THE DATA INTO PHM_A3600_ERRORS FOR THE BATCH_NUM ' || V_BATCH_NUM || ' DUE TO ' || SQLERRM;
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID ,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, V_NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
                        COMMIT;
                        EXIT;
                    END;            
               END LOOP;
               CLOSE a3600_errors_cur;
               --DBMS_OUTPUT.PUT_LINE'Total Records Inserted: ' || V_NUM_ROWS);              
              
           ELSE 
                /* STOP THE ODS PROCESS IF NO NEW FILES EXISTS FOR THE CURRENT RUN*/
                V_PROCESS_STATUS := 'NO_NEW_DATA';
                V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTAM AFTER ZIPID '||V_START_KEY|| ' AT '||SYSDATE;
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, V_NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
           END IF; 
        END IF;   
     ELSE
         /* SUBMITTED A ODS REQUEST WHICH IS IN IN-PROGRESS OR COMPLETED STATUS */ 
         V_ERROR_MESSAGE := 'NOT ABLE TO RUN ODS AS THE SUBMITTED ODS IS IN  ' || V_STATUS ||' STATUS';
         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,V_PROD_ODS_ROUTINE_SK,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,V_ODS_NAME); 
     END IF;
     
  ELSE
    -- NO BASIC INFORMATION OF ODS PRESENT
    V_ERROR_MESSAGE := 'NOT ABLE TO FETCH THE BASIC INFORMATION FOR ODS DUE TO ' || SQLERRM;
    PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,V_PROD_ODS_ROUTINE_SK,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,V_ODS_NAME); 
  END IF;
  
  IF V_PROCESS_STATUS NOT IN ('NO_NEW_DATA','ERRORED') THEN
    V_PROCESS_STATUS := 'COMPLETED';
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, V_NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
  END IF; 
 
  COMMIT;              
 
EXCEPTION
    WHEN OTHERS THEN
      V_ERROR_MESSAGE := 'NOT ABLE TO EXECUTE THE PROCEDURE PHM_ODS_A3600 DUE TO ' || SQLERRM;
      V_PROCESS_STATUS := 'ERRORED';
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG ( V_PROCESS_ID,V_RUN_DATE,V_TABLE_NAME,V_START_KEY,V_END_KEY,V_PROCESS_STATUS,V_ERROR_MESSAGE, V_NUM_ROWS,V_BATCH_NUM,V_ODS_NAME,V_PROD_ODS_ROUTINE_SK);     
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,V_PROD_FAMILY,V_PROCESS_TYPE,V_ROUTINE_TYPE,V_ODS_NAME,V_ROUTINE_NAME,V_RUN_MODE,V_PROCESS_STATUS,V_ERROR_MESSAGE,V_RUN_DATE ,SYSDATE,V_BATCH_NUM ,V_UNIX_ID,V_PROD_ODS_ROUTINE_SK );
END PHM_ODS_A3600_PROC;

CREATE OR REPLACE PUBLIC SYNONYM PHM_ODS_A3600_PROC FOR SVC_PHM_OWNER.PHM_ODS_A3600_PROC;
GRANT EXECUTE ON PHM_ODS_A3600_PROC TO SVC_PHM_CONNECT;

GRANT EXECUTE ON PHM_ODS_A3600_PROC TO SVC_PHM_CONNECT_ROLE;
