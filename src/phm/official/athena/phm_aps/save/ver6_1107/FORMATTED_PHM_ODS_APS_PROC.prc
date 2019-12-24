CREATE OR REPLACE PROCEDURE SVC_PHM_OWNER.PHM_ODS_APS_PROC(V_PROD_ODS_ROUTINE_SK NUMBER,
                                                           V_RUN_DATE DATE, 
                                                           V_BATCH_NUM VARCHAR2,
                                                           V_UNIX_ID VARCHAR)
AS
  
    V_ODS_NAME VARCHAR2(50);
    V_END_KEY NUMBER(10);
    V_START_KEY NUMBER(10);
    V_ERROR_MESSAGE VARCHAR2(500);
    V_ROUTINE_NAME VARCHAR2(30);
    V_ROUTINE_TYPE VARCHAR2(30);
    V_PROCESS_TYPE VARCHAR2(30);
    V_RUN_MODE VARCHAR2(30);
    V_TABLE_NAME VARCHAR2(30);
    V_PROCESS_STATUS VARCHAR2(20);
    V_STATUS VARCHAR2(20);
    NUM_ROWS NUMBER(10) := 0 ;
    V_START_DATE  DATE; 
    V_END_DATE DATE;
    V_RUN_TYPE VARCHAR2(15);
    V_PROCESS_ID VARCHAR2(25);
    V_PROD_FAMILY VARCHAR2(15);
  
    BEGIN
        V_ODS_NAME := 'APS_ODS';

        /* TO GET THE BASIC DETAILS OF ODS PROCEDURE */
        PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_DETAILS(V_PROD_ODS_ROUTINE_SK,
                                                      V_ROUTINE_TYPE,
                                                      V_PROCESS_TYPE,
                                                      V_ROUTINE_NAME,
                                                      V_RUN_MODE,
                                                      V_TABLE_NAME,
                                                      V_PROD_FAMILY);
        V_TABLE_NAME := 'PHM_ODS_APS_ERRORS';
  
        IF V_ROUTINE_NAME IS NOT NULL 
        THEN

            -- GET TEH ODS PREVIOUS EXECUTION DETAILS
            PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_EXEC_DETAILS(V_PROD_ODS_ROUTINE_SK,
                                                               V_RUN_DATE,
                                                               V_BATCH_NUM,
                                                               'KEY_VALUE',
                                                               V_ROUTINE_TYPE,
                                                               V_ROUTINE_NAME, 
                                                               V_TABLE_NAME,
                                                               V_START_KEY,
                                                               V_END_KEY,
                                                               V_START_DATE,
                                                               V_END_DATE,
                                                               V_RUN_TYPE,
                                                               V_STATUS,
                                                               V_PROCESS_ID);

            --V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
    
            IF V_RUN_TYPE IS NULL  
            THEN
                -- GET END_KEY FOR THE FIRST RUN OF THE ODS
                SELECT 
                    MAX(ZIPID) INTO V_END_KEY 
                FROM 
                    IDAOWNER.APS_LOG_FILES 
                WHERE 
                    PROCESS_START_DATE < SYSDATE - 30 
                AND 
                    PROCESSCODE ='P'; 
                V_RUN_TYPE := 'NEW_RUN';
            END IF;
   
            IF ( V_RUN_TYPE IN ('RE_RUN')  AND V_STATUS NOT  IN ('COMPLETED','STARTED','FAILED' )) OR
               ( V_RUN_TYPE IN ('NEW_RUN') AND V_STATUS NOT  IN ('FAILED') )  
            THEN
                V_PROCESS_STATUS := 'STARTED';
                IF V_RUN_TYPE = 'RE_RUN' 
                THEN

                    BEGIN
                        -- DELETE THE OLD DATA OF CURRENT INSTANCE IF IT IS A RE_RUN
                        DELETE FROM  
                            SVC_PHM_ODS.PHM_ODS_APS_ERRORS 
                        WHERE 
                            ZIPID BETWEEN 
                                V_START_KEY 
                            AND 
                                V_END_KEY
                        AND 
                            BATCH_NUM = V_BATCH_NUM 
                        AND 
                            RUN_DATE = V_RUN_DATE;
                    EXCEPTION WHEN NO_DATA_FOUND 
                    THEN
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || 'NO OLD DATA FOUND ';
                    WHEN OTHERS 
                    THEN
                        V_PROCESS_STATUS := 'ERRORED';
                        V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA FROM PHM_ODS_APS_ERRORS FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,
                                                                                V_RUN_DATE,
                                                                                V_TABLE_NAME,
                                                                                V_START_KEY,
                                                                                V_END_KEY,
                                                                                V_PROCESS_STATUS,
                                                                                V_ERROR_MESSAGE, 
                                                                                NUM_ROWS,
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


            IF V_RUN_TYPE = 'NEW_RUN' 
            THEN
                -- GET START KEY AND END KEY FOR ODS EXECUTION
                BEGIN 
                    SELECT 
                        MIN(ZIPID),
                        MAX(ZIPID)
                    INTO 
                        V_START_KEY,
                        V_END_KEY 
                    FROM 
                        IDAOWNER.APS_LOG_FILES 
                    WHERE 
                        ZIPID >  V_END_KEY 
                    AND 
                        PROCESSCODE  = 'P';

                    --V_END_KEY := V_START_KEY + 1000;

                EXCEPTION WHEN NO_DATA_FOUND 
                THEN
                    V_PROCESS_STATUS := 'NO_NEW_DATA';
                    V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTAM AFTER FILEID '||V_END_KEY|| ' AT '||SYSDATE;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID,
                                                                            V_RUN_DATE,
                                                                            V_TABLE_NAME,
                                                                            V_START_KEY,
                                                                            V_END_KEY,
                                                                            V_PROCESS_STATUS,
                                                                            V_ERROR_MESSAGE, 
                                                                            NUM_ROWS,
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
                WHEN OTHERS 
                THEN
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE := 'NOT ABLE TO GET THE START KEY AND END KEY VALUES FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,
                                                                            V_RUN_DATE,
                                                                            V_TABLE_NAME,
                                                                            V_START_KEY,
                                                                            V_END_KEY,
                                                                            V_PROCESS_STATUS,
                                                                            V_ERROR_MESSAGE, 
                                                                            NUM_ROWS,
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

            IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA')  AND 
               V_RUN_TYPE IN ('RE_RUN', 'NEW_RUN') 
            THEN
                -- LOG THE DETAILS FOR THE CURRENT INSTANCE OF THE ODS  
                V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID(); 
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                                V_PROD_FAMILY,
                                                                V_PROCESS_TYPE,
                                                                V_ROUTINE_TYPE,
                                                                V_ODS_NAME,
                                                                V_ROUTINE_NAME,
                                                                V_RUN_MODE,
                                                                V_PROCESS_STATUS,
                                                                ' ',
                                                                V_RUN_DATE ,
                                                                SYSDATE,
                                                                V_BATCH_NUM ,
                                                                V_UNIX_ID,
                                                                V_PROD_ODS_ROUTINE_SK );
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,
                                                                        V_RUN_DATE,
                                                                        V_TABLE_NAME,
                                                                        V_START_KEY,
                                                                        V_END_KEY,
                                                                        V_PROCESS_STATUS,
                                                                        '', 
                                                                        NULL,
                                                                        V_BATCH_NUM,
                                                                        V_ODS_NAME,
                                                                        V_PROD_ODS_ROUTINE_SK);
                COMMIT;
            END IF; 

            IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA') 
            THEN  
                NUM_ROWS := 0; 

                -- INSERTING THE DATA INTO ODS TABLES...
                IF V_START_KEY <= V_END_KEY 
                THEN
                    FOR ER_ROW IN ( 
                        SELECT 
                            FD.ZIPID,
                            FD.FILEID as FILEID,
                            FD.LOG_FILE_NAME,
                            E.FILEID AS FILEID1,
                            E.MESSAGE_ID,
                            E.DEVICE_ID,
                            E.PL,E.SN,
                            E.TIMESTAMP,
                            E.MESSAGE,
                            E.MESSAGE_TYPE,
                            E.LOAD_DATE
                        FROM 
                            IDAOWNER.APS_LOG_FILES F, 
                            IDAOWNER.APS_LOG_FILE_DETAILS FD,
                            ( 
                                SELECT 
                                    * 
                                FROM 
                                    IDAOWNER.APS_ERRORS 
                                WHERE 
                                    TIMESTAMP > V_RUN_DATE - 30
                            ) E
                        WHERE 
                            F.ZIPID = FD.ZIPID 
                        AND  
                            FD.FILEID = E.FILEID 
                        AND 
                            FD.DEVICE_ID = E.DEVICE_ID
                        AND 
                            F.ZIPID BETWEEN 
                                V_START_KEY 
                            AND 
                                V_END_KEY 
                            AND  
                            F.PROCESSCODE ='P'
                        )
                        --and rownum < 11)
                    LOOP
                        BEGIN
                            INSERT INTO 
                                SVC_PHM_ODS.PHM_ODS_APS_ERRORS 
                            VALUES 
                            (
                                V_BATCH_NUM,
                                V_RUN_DATE,
                                ER_ROW.ZIPID,
                                ER_ROW.LOG_FILE_NAME,
                                ER_ROW.FILEID,
                                ER_ROW.MESSAGE_ID,
                                ER_ROW.DEVICE_ID,
                                ER_ROW.PL,
                                ER_ROW.SN,
                                ER_ROW.TIMESTAMP,
                                ER_ROW.MESSAGE,
                                ER_ROW.MESSAGE_TYPE,
                                ER_ROW.LOAD_DATE,
                                'SVC_PHM_OWNER',
                                SYSDATE,
                                'SVC_PHM_OWNER',
                                SYSDATE
                            ); 
                            NUM_ROWS:= NUM_ROWS + 1;
                        EXCEPTION WHEN OTHERS 
                        THEN
                            V_PROCESS_STATUS := 'ERRORED';
                            V_ERROR_MESSAGE := 'NOT ABLE TO INSERT THE DATA INTO PHM_APS_ERRORS FOR THE ZIPID' || ER_ROW.ZIPID||' FILE ID ' ||ER_ROW.FILEID ||'ERROR ID' ||ER_ROW.MESSAGE_ID ||' DUE TO ' || SQLERRM;
                            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,
                                                                                    V_RUN_DATE,
                                                                                    V_TABLE_NAME,
                                                                                    V_START_KEY ,
                                                                                    V_END_KEY,
                                                                                    V_PROCESS_STATUS,
                                                                                    V_ERROR_MESSAGE, 
                                                                                    NUM_ROWS,
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
                            COMMIT;
                            EXIT;
                        END;
                    END LOOP;
                ELSE 
                    -- NO NEW FILES
                    V_PROCESS_STATUS := 'NO_NEW_DATA';
                    V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTEM AFTER ZIPID '||V_END_KEY|| ' AT '||SYSDATE;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,
                                                                            V_RUN_DATE,
                                                                            V_TABLE_NAME,
                                                                            V_START_KEY,
                                                                            V_END_KEY,
                                                                            V_PROCESS_STATUS,
                                                                            V_ERROR_MESSAGE,
                                                                            NUM_ROWS,
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
                END IF; 
            END IF;   
        ELSE
            -- SUBMITTED A ODS REQUEST WHICH IS IN IN-PROGRESS OR COMPLETED STATUS 
            V_ERROR_MESSAGE := 'NOT ABLE TO RUN ODS AS THE SUBMITTED ODS IS IN  ' || V_STATUS ||' STATUS';
            PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,
                                                            V_PROD_ODS_ROUTINE_SK,
                                                            V_RUN_DATE,
                                                            V_BATCH_NUM,
                                                            V_ERROR_MESSAGE,
                                                            V_ODS_NAME); 
        END IF;
    ELSE
        -- NO ODS DATA PRESENT
        V_ERROR_MESSAGE := 'NOT ABLE TO FETCH THE BASIC INFORMATION FOR ODS DUE TO ' || SQLERRM;
        PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,
                                                        V_PROD_ODS_ROUTINE_SK,
                                                        V_RUN_DATE,
                                                        V_BATCH_NUM,
                                                        V_ERROR_MESSAGE,
                                                        V_ODS_NAME);
    END IF;
  
    IF V_PROCESS_STATUS NOT IN ('NO_NEW_DATA','ERRORED') THEN
        -- MAKING THE ODS COMPLETED 
        V_PROCESS_STATUS := 'COMPLETED';
        V_ERROR_MESSAGE := '';
        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,
                                                                V_RUN_DATE,
                                                                V_TABLE_NAME,
                                                                V_START_KEY,
                                                                V_END_KEY,
                                                                V_PROCESS_STATUS,
                                                                V_ERROR_MESSAGE, 
                                                                NUM_ROWS,
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
    END IF; 
 
    COMMIT;              
 
EXCEPTION WHEN OTHERS 
THEN
    V_ERROR_MESSAGE := 'NOT ABLE TO EXECUTE THE PROCEDURE PHM_ODS_APS_PROC DUE TO ' || SQLERRM;
    V_PROCESS_STATUS := 'ERRORED';
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (V_PROCESS_ID ,
                                                            V_RUN_DATE,
                                                            V_TABLE_NAME,
                                                            V_START_KEY,
                                                            V_END_KEY,
                                                            V_PROCESS_STATUS,
                                                            V_ERROR_MESSAGE, 
                                                            NUM_ROWS,
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
END PHM_ODS_APS_PROC;
/
