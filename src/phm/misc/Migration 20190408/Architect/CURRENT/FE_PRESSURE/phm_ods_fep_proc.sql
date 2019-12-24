CREATE OR REPLACE PROCEDURE 
SVC_PHM_OWNER.PHM_ODS_FEP_PROC(V_PROD_ODS_ROUTINE_SK NUMBER,
                               V_RUN_DATE DATE, 
                               V_BATCH_NUM VARCHAR2,
                               V_UNIX_ID VARCHAR)
AS
  
V_ODS_NAME       VARCHAR2(50);
V_END_KEY        NUMBER(35);
V_START_KEY      NUMBER(35);
V_ERROR_MESSAGE  VARCHAR2(500);
V_ROUTINE_NAME   VARCHAR2(30);
V_ROUTINE_TYPE   VARCHAR2(30);
V_PROCESS_TYPE   VARCHAR2(30);
V_RUN_MODE       VARCHAR2(30);
V_TABLE_NAME     VARCHAR2(30);
V_PROCESS_STATUS VARCHAR2(20);
V_STATUS         VARCHAR2(20);
NUM_ROWS         NUMBER(10);
V_START_DATE     DATE;                            
V_END_DATE       DATE;
V_RUN_TYPE       VARCHAR2(15);
V_PROCESS_ID     NUMBER(15);
V_PROD_FAMILY    varchar2(15);
V_END_PREV_KEY   NUMBER(35);
  
BEGIN
    V_ODS_NAME := 'FEP_ODS';

    /* TO GET THE BASIC DETAILS OF ODS PROCEDURE */
    PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_DETAILS(V_PROD_ODS_ROUTINE_SK,
                                                  V_ROUTINE_TYPE,
                                                  V_PROCESS_TYPE,
                                                  V_ROUTINE_NAME,
                                                  V_RUN_MODE,
                                                  V_TABLE_NAME,
                                                  V_PROD_FAMILY);
    V_TABLE_NAME := 'PHM_ODS_PRESSURES_IA';
  
    IF V_ROUTINE_NAME IS NOT NULL 
    THEN
        -- GET PREVIOUS EXECUTION DETAILS
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
                                                           V_STATUS , 
                                                           V_PROCESS_ID );
        V_START_DATE := TO_DATE(V_START_KEY , 'YYYYMMDDHH24MISS');
        V_END_DATE := TO_DATE(V_END_KEY , 'YYYYMMDDHH24MISS');
        --V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
    
        V_END_PREV_KEY := V_END_KEY;
        IF V_RUN_TYPE IS NULL
        THEN
            -- GET END_KEY FOR THE FIRST RUN OF THE ODS
            --SELECT MAX(A.FILEID) INTO V_END_KEY FROM (SELECT FILEID,FILESTRUCTURE,FILENAME,PROCESSCODE,LOADSTARTTIME,LOADENDTIME FROM IDAOWNER.IDALOGDETAILS 
            --WHERE PROCESSCODE='PD'  AND DATATYPE = 'PM') A, (SELECT FILEID,FILENAME,PROCESSFLAG,FILESOURCEDATE FROM IDAOWNER.IDALOGFILES 
            --WHERE PROCESSFLAG='P' ) B WHERE A.FILEID=B.FILEID AND A.LOADSTARTTIME < SYSDATE - 5 ;
            V_END_DATE := TRUNC(SYSDATE -3);
            V_RUN_TYPE := 'NEW_RUN';
         END IF;
   
        IF ( V_RUN_TYPE IN ('RE_RUN')  AND 
             V_STATUS NOT  IN ('COMPLETED','STARTED','FAILED' )) OR
             ( V_RUN_TYPE IN ('NEW_RUN') AND 
               V_STATUS NOT  IN ('FAILED') )  
        THEN
            V_PROCESS_STATUS := 'STARTED';
            IF V_RUN_TYPE = 'RE_RUN'
            THEN
            BEGIN
                -- DELETE THE OLD DATA OF CURRENT INSTANCE IF IT IS A RE_RUN
                DELETE FROM  
                    SVC_PHM_ODS.PHM_ODS_PRESSURES_IA 
                WHERE  
                    BATCH_NUM = V_BATCH_NUM 
                AND 
                    RUN_DATE = V_RUN_DATE;
                EXCEPTION 
                WHEN NO_DATA_FOUND 
                THEN
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || 'NO OLD DATA FOUND ';

                WHEN OTHERS 
                THEN
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA FROM PHM_ODS_PRESSURES_IA FOR THE BATCH_NUM' || V_BATCH_NUM||' RUN_DATE ' ||V_RUN_DATE ||' DUE TO ' || SQLERRM;
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
                END;  
            END IF;

            IF V_RUN_TYPE = 'NEW_RUN' THEN
            BEGIN 
                -- GET START KEY AND END KEY FOR ODS EXECUTION
                SELECT 
                    MIN(A.LOADENDTIME),
                    MAX(A.LOADENDTIME) 
                INTO 
                    V_START_DATE,
                    V_END_DATE 
                FROM (
                    SELECT 
                        FILEID,
                        FILESTRUCTURE,
                        FILENAME,
                        PROCESSCODE,
                        LOADSTARTTIME,
                        LOADENDTIME 
                    FROM 
                        IDAOWNER.IDALOGDETAILS 
                    WHERE 
                        PROCESSCODE='PD' 
                    AND 
                        LOADENDTIME >  V_END_DATE 
                    AND 
                        DATATYPE = 'PM') A, 
                    (
                        SELECT 
                            FILEID,
                            FILENAME,
                            PROCESSFLAG,
                            FILESOURCEDATE 
                        FROM 
                            IDAOWNER.IDALOGFILES 
                        WHERE 
                            PROCESSFLAG='P' 
                        AND 
                            LOADENDTIME >  V_END_DATE ) B
                WHERE 
                    A    .FILEID=B.FILEID;

                --V_END_KEY := V_START_KEY  + 200;

                EXCEPTION
                WHEN NO_DATA_FOUND 
                THEN
                    V_PROCESS_STATUS := 'NO_NEW_DATA';
                    V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTAM AFTER FILEID '||V_END_PREV_KEY|| ' AT '||SYSDATE;
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
         
            V_START_KEY := TO_CHAR(V_START_DATE , 'YYYYMMDDHH24MISS'); 
            V_END_KEY   := TO_CHAR(V_END_DATE , 'YYYYMMDDHH24MISS');
         
            IF V_PROCESS_STATUS NOT IN ( 'ERRORED','NO_NEW_DATA') AND 
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
                NUM_ROWS  := 0;
                IF V_START_KEY <= V_END_KEY 
                THEN
                    -- INSERTING THE DATA INTO ODS TABLES...
                    FOR ER_ROW IN ( 
                        SELECT 
                            * 
                        FROM 
                            IDAOWNER.PRESSURES_IA 
                        WHERE 
                            FILEID IN (
                                SELECT 
                                    FILEID 
                                FROM 
                                    IDAOWNER.IDALOGFILEs 
                                WHERE 
                                    LOADENDTIME BETWEEN 
                                        V_START_DATE 
                                    AND 
                                        V_END_DATE
                                )
                    )
                    LOOP
                    BEGIN
                        INSERT INTO 
                            SVC_PHM_ODS.PHM_ODS_PRESSURES_IA 
                        VALUES (
                            V_BATCH_NUM,
                            V_RUN_DATE,
                            NULL,
                            ER_ROW.FILEID,
                            '',
                            ER_ROW.DEVICEID,
                            ER_ROW.MODULEID,
                            ER_ROW.REPLICATEID,
                            ER_ROW.COMPLETIONDATE,
                            ER_ROW.MODULESNDRM,
                            ER_ROW.ASSAYID,
                            ER_ROW.PIPETTER,
                            ER_ROW.PIPESTEP,
                            ER_ROW.LOCATION, 
                            ER_ROW.VOLUME, 
                            ER_ROW.PMSTATUS,
                            ER_ROW.FRONTENDPRESSURE,
                            ER_ROW.ASPIRATEPRESSURE ,
                            ER_ROW.BACKENDPRESSURE,
                            ER_ROW.SHAPE,
                            ER_ROW.PSIDISCORD,
                            ER_ROW.CLOTSCORE,
                            ER_ROW.OVERPRESSURESLOPE,
                            ER_ROW.LOADDATE ,
                            V_ODS_NAME,
                            SYSDATE,
                            V_ODS_NAME,
                            SYSDATE); 
                        NUM_ROWS:= NUM_ROWS + 1;
                        IF MOD(NUM_ROWS,10000) = 0 THEN
                            COMMIT;
                        END IF;
                           
                        EXCEPTION
                        WHEN OTHERS 
                        THEN
                             V_PROCESS_STATUS := 'ERRORED';
                             V_ERROR_MESSAGE := 'NOT ABLE TO INSERT THE DATA INTO PHM_ODS_PRESSURES_IA FOR THE  FILE ID ' ||ER_ROW.FILEID 
                                    ||'DEVICE ID' ||ER_ROW.DEVICEID ||' FOR PIPRTOR '||ER_ROW.PIPETTER||' DUE TO ' || SQLERRM;
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
                             COMMIT;
                             EXIT;
                        END;
                    END LOOP;
                ELSE 
                    -- NO NEW FILES
                    V_PROCESS_STATUS := 'NO_NEW_DATA';
                    V_ERROR_MESSAGE := 'NO NEW FILE PRESENT IN THE SYSTEM AFTER FILEID '||V_END_KEY|| ' AT '||SYSDATE;
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
                                                                    V_UNIX_ID ,
                                                                    V_PROD_ODS_ROUTINE_SK);
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
  
    IF V_PROCESS_STATUS NOT IN ('NO_NEW_DATA','ERRORED') 
    THEN
        V_PROCESS_STATUS := 'COMPLETED';
        --V_ERROR_MESSAGE := 'FE PRESSURE ODS HAS BEEN COMPLETED SUCCESSFULLY';
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
 
    EXCEPTION
    WHEN OTHERS THEN
        V_ERROR_MESSAGE := 'NOT ABLE TO EXECUTE THE PROCEDURE PHM_ODS_FEP_PROC DUE TO ' || SQLERRM;
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

 
END PHM_ODS_FEP_PROC;
/
