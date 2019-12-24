CREATE OR REPLACE PROCEDURE 
SVC_PHM_OWNER.PHM_ODS_SCM_ACTIVITY_PROC (
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
   

TYPE T_FILENAME is table of IDAQOWNER.IDAQ_LOGDETAILS.FILENAME%TYPE;
TYPE T_LOGDETAIL_ID is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.LOGDETAIL_ID%TYPE;
TYPE T_CUSTOMERNUMBER is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.CUSTOMERNUMBER%TYPE;
TYPE T_SYSTEMSN is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.SYSTEMSN%TYPE;
TYPE T_DEVICEID is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.DEVICEID%TYPE;
TYPE T_MODULESN is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.MODULESN%TYPE;
TYPE T_ACTIVITY is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.ACTIVITY%TYPE;
TYPE T_COMPONENT is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.COMPONENT%TYPE;
TYPE T_LOGDATE_LOCAL is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.LOGDATE_LOCAL%TYPE;
TYPE T_LOADDATE is table of IDAQOWNER.ICQ_INSTRUMENTACTIVITY.LOADDATE%TYPE;

V_LOGDETAIL_ID                      T_LOGDETAIL_ID;
V_FILENAME                          T_FILENAME;
V_CUSTOMERNUMBER                    T_CUSTOMERNUMBER;
V_SYSTEMSN                          T_SYSTEMSN;  
V_DEVICEID                          T_DEVICEID;
V_MODULESN                          T_MODULESN;
V_ACTIVITY                          T_ACTIVITY;
V_COMPONENT                         T_COMPONENT;
V_LOGDATE_LOCAL                     T_LOGDATE_LOCAL;
V_LOADDATE                          T_LOADDATE;
       
c_limit PLS_INTEGER := 500;
     
CURSOR  ICQ_ACTIVITY_CUR(V_START_DATE DATE, 
                         V_END_DATE DATE)
IS      
    SELECT 
        A.LOGDETAIL_ID,
        L.FILENAME,
        CUSTOMERNUMBER,
        SYSTEMSN,
        UPPER(MODULESN) AS MODULESN,
        DEVICEID,
        ACTIVITY,
        COMPONENT, 
        A.LOGDATE_LOCAL,
        A.LOADDATE
    FROM 
        IDAQOWNER.ICQ_INSTRUMENTACTIVITY A,
        (
            SELECT 
                LOGDETAIL_ID, 
                FILENAME
            FROM 
                IDAQOWNER.IDAQ_LOGDETAILS
            WHERE 
                LOADSTARTTIME > (V_START_DATE - 1) 
            and 
                LOADENDTIME BETWEEN 
                    V_START_DATE 
                AND 
                    V_END_DATE
       ) L
    WHERE 
        A.LOGDATE_LOCAL > SYSDATE - 7 
    and 
        A.LOGDETAIL_ID = L.LOGDETAIL_ID;
    --AND UPPER(A.MODULESN) like 'AI%';    
   
BEGIN
    V_ODS_NAME := 'PHM_ODS_SCM_INSTRUMENTACTIVITY';

    /* TO GET THE BASIC DETAILS OF ODS PROCEDURE */
    PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_DETAILS (V_PROD_ODS_ROUTINE_SK,
                                                   V_ROUTINE_TYPE,
                                                   V_PROCESS_TYPE,
                                                   V_ROUTINE_NAME,
                                                   V_RUN_MODE,
                                                   V_TABLE_NAME,
                                                   V_PROD_FAMILY);
    V_TABLE_NAME := 'PHM_ODS_SCM_INSTRUMENTACTIVITY';

    IF V_ROUTINE_NAME IS NOT NULL
    THEN
        -- GET TEH ODS PREVIOUS EXECUTION DETAILS
        PHM_ALGORITHM_UTILITIES_1.PHM_GET_ODS_EXEC_DETAILS (V_PROD_ODS_ROUTINE_SK,
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
        V_START_DATE := TO_DATE (V_START_KEY, 'YYYYMMDDHH24MISS');
        V_END_DATE := TO_DATE (V_END_KEY, 'YYYYMMDDHH24MISS');

        IF V_RUN_TYPE IS NULL
        THEN
            -- for the first time run, load the last 2 hours data
            -- V_END_DATE := TRUNC(SYSDATE - 1);
 
            V_END_DATE := SYSDATE - 2 / 24;
            V_RUN_TYPE := 'NEW_RUN';
        END IF;

        DBMS_OUTPUT.PUT_LINE ('V_RUN_TYPE := ' || V_RUN_TYPE || ' , V_STATUS=' || V_STATUS);

        IF (V_RUN_TYPE IN ('RE_RUN') AND 
            V_STATUS NOT IN ('COMPLETED', 'STARTED', 'FAILED')) OR 
           (V_RUN_TYPE IN ('NEW_RUN') AND 
            V_STATUS NOT IN ('FAILED'))
        THEN
            V_PROCESS_STATUS := 'STARTED';

            IF V_RUN_TYPE = 'RE_RUN'
            THEN
                BEGIN
                   -- DELETE THE OLD DATA OF CURRENT INSTANCE IF IT IS A RE_RUN
                   DELETE FROM 
                       SVC_PHM_ODS.PHM_ODS_CI_SCM_INSTACTIVITY
                   WHERE 
                       BATCH_NUM = V_BATCH_NUM 
                   AND 
                       RUN_DATE = V_RUN_DATE;
                  COMMIT;
                EXCEPTION WHEN NO_DATA_FOUND
                THEN
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || 'NO OLD DATA FOUND ';
                WHEN OTHERS
                THEN
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE :=
                        'NOT ABLE TO DELETE THE DATA FROM PHM_ODS_SCM_INSTRUMENTACTIVITY FOR THE BATCH_NUM'
                       || V_BATCH_NUM
                       || ' RUN_DATE '
                       || V_RUN_DATE
                       || ' DUE TO '
                       || SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
                        V_PROCESS_ID,
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
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (
                        V_PROCESS_ID,
                        V_PROD_FAMILY,
                        V_PROCESS_TYPE,
                        V_ROUTINE_TYPE,
                        V_ODS_NAME,
                        V_ROUTINE_NAME,
                        V_RUN_MODE,
                        V_PROCESS_STATUS,
                        V_ERROR_MESSAGE,
                        V_RUN_DATE,
                        SYSDATE,
                        V_BATCH_NUM,
                        V_UNIX_ID,
                        V_PROD_ODS_ROUTINE_SK);
                END;
            END IF;


            IF V_RUN_TYPE = 'NEW_RUN'
            THEN
                BEGIN
                    -- GET START KEY AND END KEY FOR ODS EXECUTION
                    SELECT 
                        MIN (A.LOADENDTIME), 
                        MAX (A.LOADENDTIME)
                    INTO 
                        V_START_DATE, 
                        V_END_DATE
                    FROM (
                            SELECT 
                                FILEID,
                                FILENAME,
                                PROCESSCODE,
                                LOADSTARTTIME,
                                LOADENDTIME
                            FROM 
                                IDAQOWNER.IDAQ_LOGDETAILS
                            WHERE     
                                PROCESSCODE = 'P'
                            AND 
                                LOADENDTIME > V_END_DATE
                            AND 
                                DATATYPE = 'INSTRUMENTACTIVITY_ODR'
                        ) A,
                        (
                            SELECT 
                                FILEID,
                                FILENAME,
                                PROCESSFLAG,
                                FILESOURCEDATE
                            FROM 
                                IDAQOWNER.IDAQ_LOGFILES
                            WHERE 
                                PROCESSFLAG = 'P' 
                            AND 
                                LOADENDTIME > V_END_DATE
                        ) B
                    WHERE 
                        A.FILEID = B.FILEID;
                        --V_END_KEY := V_START_KEY  + 100;
                EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                    V_PROCESS_STATUS := 'NO_NEW_DATA';
                    V_ERROR_MESSAGE :=
                        'NO NEW FILE PRESENT IN THE SYSTAM AFTER DATE  '
                        || V_END_DATE
                        || ' AT '
                        || SYSDATE;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
                        V_PROCESS_ID,
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
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (
                        V_PROCESS_ID,
                        V_PROD_FAMILY,
                        V_PROCESS_TYPE,
                        V_ROUTINE_TYPE,
                        V_ODS_NAME,
                        V_ROUTINE_NAME,
                        V_RUN_MODE,
                        V_PROCESS_STATUS,
                        V_ERROR_MESSAGE,
                        V_RUN_DATE,
                        SYSDATE,
                        V_BATCH_NUM,
                        V_UNIX_ID,
                        V_PROD_ODS_ROUTINE_SK);
                WHEN OTHERS
                THEN
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE :=
                        'NOT ABLE TO GET THE START KEY AND END KEY VALUES FOR THE BATCH_NUM'
                        || V_BATCH_NUM
                        || ' RUN_DATE '
                        || V_RUN_DATE
                        || ' DUE TO '
                        || SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
                        V_PROCESS_ID,
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
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (
                        V_PROCESS_ID,
                        V_PROD_FAMILY,
                        V_PROCESS_TYPE,
                        V_ROUTINE_TYPE,
                        V_ODS_NAME,
                        V_ROUTINE_NAME,
                        V_RUN_MODE,
                        V_PROCESS_STATUS,
                        V_ERROR_MESSAGE,
                        V_RUN_DATE,
                        SYSDATE,
                        V_BATCH_NUM,
                        V_UNIX_ID,
                        V_PROD_ODS_ROUTINE_SK);
                END;
            END IF;

            V_START_KEY := TO_CHAR (V_START_DATE, 'YYYYMMDDHH24MISS');
            V_END_KEY := TO_CHAR (V_END_DATE, 'YYYYMMDDHH24MISS');

            DBMS_OUTPUT.PUT_LINE ('V_START_KEY: ' || V_START_KEY || 'V_END_KEY: ' || V_END_KEY);

            IF V_PROCESS_STATUS NOT IN ('ERRORED', 'NO_NEW_DATA') AND 
               V_RUN_TYPE = 'NEW_RUN'
            THEN
                -- LOG THE DETAILS FOR THE CURRENT INSTANCE OF THE ODS
                V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID ();
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (
                    V_PROCESS_ID,
                    V_PROD_FAMILY,
                    V_PROCESS_TYPE,
                    V_ROUTINE_TYPE,
                    V_ODS_NAME,
                    V_ROUTINE_NAME,
                    V_RUN_MODE,
                    V_PROCESS_STATUS,
                    ' ',
                    V_RUN_DATE,
                    SYSDATE,
                    V_BATCH_NUM,
                    V_UNIX_ID,
                    V_PROD_ODS_ROUTINE_SK);
                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
                    V_PROCESS_ID,
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

            DBMS_OUTPUT.PUT_LINE (
                'V_RUN_TYPE := '
                || V_RUN_TYPE
                || ' , V_START_DATE='
                || V_START_DATE
                || ' , V_END_DATE='
                || V_END_DATE);

            IF V_PROCESS_STATUS NOT IN ('ERRORED', 'NO_NEW_DATA')
            THEN
                -- INSERTING THE DATA INTO ODS TABLES...
                IF V_START_KEY <= V_END_KEY
                THEN

                OPEN ICQ_ACTIVITY_CUR(V_START_DATE, V_END_DATE);
                LOOP
                    BEGIN
                        FETCH 
                            ICQ_ACTIVITY_CUR BULK COLLECT 
                        INTO 
                            V_LOGDETAIL_ID, 
                            V_FILENAME, 
                            V_CUSTOMERNUMBER, 
                            V_SYSTEMSN, 
                            V_MODULESN,  
                            V_DEVICEID, 
                            V_ACTIVITY, 
                            V_COMPONENT, 
                            V_LOGDATE_LOCAL, 
                            V_LOADDATE
                        LIMIT c_limit;                                         

                        ----DBMS_OUTPUT.PUT_LINE'Looping: c%rowcount = ' || pressures_dis_cur%ROWCOUNT || ', pd_data: ' || pd_data.count);

                        FORALL i in 1 .. V_LOGDETAIL_ID.count
                            INSERT INTO 
                                SVC_PHM_ODS.PHM_ODS_CI_SCM_INSTACTIVITY
                            (
                                BATCH_NUM, 
                                RUN_DATE, 
                                ZIPID, 
                                LOGDETAIL_ID, 
                                FILENAME, 
                                CUSTOMERNUMBER, 
                                SYSTEMSN, 
                                DEVICEID, 
                                MODULESN, 
                                ACTIVITY, 
                                COMPONENT, 
                                LOGDATE_LOCAL, 
                                LOADDATE, 
                                PHM_ODS_CREATE_USER, 
                                PHM_ODS_CREATE_DATE, 
                                PHM_ODS_MODIFY_USER, 
                                PHM_ODS_MODIFY_DATE
                            ) 
                            VALUES 
                            (
                                V_BATCH_NUM, 
                                V_RUN_DATE, 
                                NULL, 
                                V_LOGDETAIL_ID(i), 
                                V_FILENAME(i), 
                                V_CUSTOMERNUMBER(i), 
                                V_SYSTEMSN(i), 
                                V_DEVICEID(i), 
                                V_MODULESN(i), 
                                V_ACTIVITY(i), 
                                V_COMPONENT(i), 
                                V_LOGDATE_LOCAL(i), 
                                V_LOADDATE(i), 
                                'PHM_ODS_SCM_INSTRUMENTACTIVITY', 
                                SYSDATE,
                                'PHM_ODS_SCM_INSTRUMENTACTIVITY', 
                                SYSDATE
                            );

                            NUM_ROWS := NUM_ROWS + V_LOGDETAIL_ID.count;
                            IF MOD(NUM_ROWS, 10000) = 0 
                            THEN
                                DBMS_OUTPUT.PUT_LINE('Committed. Total Record Count: ' || NUM_ROWS); 
                               COMMIT;
                            END IF; 

                            EXIT WHEN ICQ_ACTIVITY_CUR%NOTFOUND;   
 
                            EXCEPTION
                            WHEN OTHERS
                            THEN
                                V_PROCESS_STATUS := 'ERRORED';
                                V_ERROR_MESSAGE :=
                                      'NOT ABLE TO INSERT THE DATA INTO PHM_ODS_SCM_INSTRUMENTACTIVITY FOR THE BATCH_NUM ' || V_BATCH_NUM || ' DUE TO ' || SQLERRM;
                                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
                                    V_PROCESS_ID,
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
                                PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (
                                    V_PROCESS_ID,
                                    V_PROD_FAMILY,
                                    V_PROCESS_TYPE,
                                    V_ROUTINE_TYPE,
                                    V_ODS_NAME,
                                    V_ROUTINE_NAME,
                                    V_RUN_MODE,
                                    V_PROCESS_STATUS,
                                    V_ERROR_MESSAGE,
                                    V_RUN_DATE,
                                    SYSDATE,
                                    V_BATCH_NUM,
                                    V_UNIX_ID,
                                    V_PROD_ODS_ROUTINE_SK);
                                COMMIT;
                            EXIT;
                        END;
                    END LOOP;
                CLOSE ICQ_ACTIVITY_CUR;  
            ELSE
               -- NO NEW FILES
               V_PROCESS_STATUS := 'NO_NEW_DATA';
               V_ERROR_MESSAGE :=
                     'NO NEW FILE PRESENT IN THE SYSTEM AFTER FILEID '
                  || V_END_KEY
                  || ' AT '
                  || SYSDATE;
               PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
                  V_PROCESS_ID,
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
               PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (
                  V_PROCESS_ID,
                  V_PROD_FAMILY,
                  V_PROCESS_TYPE,
                  V_ROUTINE_TYPE,
                  V_ODS_NAME,
                  V_ROUTINE_NAME,
                  V_RUN_MODE,
                  V_PROCESS_STATUS,
                  V_ERROR_MESSAGE,
                  V_RUN_DATE,
                  SYSDATE,
                  V_BATCH_NUM,
                  V_UNIX_ID,
                  V_PROD_ODS_ROUTINE_SK);
            END IF;
         END IF;
      ELSE
         -- SUBMITTED A ODS REQUEST WHICH IS IN IN-PROGRESS OR COMPLETED STATUS
         V_ERROR_MESSAGE :=
               'NOT ABLE TO RUN ODS AS THE SUBMITTED ICQ_INSTRUMENTACTIVITY_ODS IS IN  '
            || V_STATUS
            || ' STATUS';
         PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (
            V_PROCESS_ID,
            V_PROD_ODS_ROUTINE_SK,
            V_RUN_DATE,
            V_BATCH_NUM,
            V_ERROR_MESSAGE,
            V_ODS_NAME);
      END IF;
   ELSE
      -- NO ODS DATA PRESENT
      V_ERROR_MESSAGE :=
            'NOT ABLE TO FETCH THE BASIC INFORMATION FOR ICQ_INSTRUMENTACTIVITY_ODS DUE TO '
         || SQLERRM;
      PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,
                                                      V_PROD_ODS_ROUTINE_SK,
                                                      V_RUN_DATE,
                                                      V_BATCH_NUM,
                                                      V_ERROR_MESSAGE,
                                                      V_ODS_NAME);
   END IF;

   IF V_PROCESS_STATUS NOT IN ('NO_NEW_DATA', 'ERRORED')
   THEN
      -- MAKING THE ODS COMPLETED
      V_PROCESS_STATUS := 'COMPLETED';
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
         V_PROCESS_ID,
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
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (V_PROCESS_ID,
                                                       V_PROD_FAMILY,
                                                       V_PROCESS_TYPE,
                                                       V_ROUTINE_TYPE,
                                                       V_ODS_NAME,
                                                       V_ROUTINE_NAME,
                                                       V_RUN_MODE,
                                                       V_PROCESS_STATUS,
                                                       V_ERROR_MESSAGE,
                                                       V_RUN_DATE,
                                                       SYSDATE,
                                                       V_BATCH_NUM,
                                                       V_UNIX_ID,
                                                       V_PROD_ODS_ROUTINE_SK);
   END IF;

   COMMIT;
EXCEPTION
   WHEN OTHERS
   THEN
      V_ERROR_MESSAGE :=
            'NOT ABLE TO EXECUTE THE PROCEDURE PHM_ODS_ICQ_ACTIVITY_PROC DUE TO '
         || SQLERRM;
      V_PROCESS_STATUS := 'ERRORED';
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_DETAIL_AUDIT_LOG (
         V_PROCESS_ID,
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
      PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG (V_PROCESS_ID,
                                                       V_PROD_FAMILY,
                                                       V_PROCESS_TYPE,
                                                       V_ROUTINE_TYPE,
                                                       V_ODS_NAME,
                                                       V_ROUTINE_NAME,
                                                       V_RUN_MODE,
                                                       V_PROCESS_STATUS,
                                                       V_ERROR_MESSAGE,
                                                       V_RUN_DATE,
                                                       SYSDATE,
                                                       V_BATCH_NUM,
                                                       V_UNIX_ID,
                                                       V_PROD_ODS_ROUTINE_SK);
END PHM_ODS_SCM_ACTIVITY_PROC;
/
