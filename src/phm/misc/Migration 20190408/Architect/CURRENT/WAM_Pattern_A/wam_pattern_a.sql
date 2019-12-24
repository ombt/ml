CREATE OR REPLACE 
PROCEDURE SVC_PHM_OWNER.PHM_WAM_A(V_ALG_NUM NUMBER,
                                  V_RUN_DATE DATE,
                                  V_BATCH_NUM VARCHAR2, 
                                  V_UNIX_ID VARCHAR2)
IS

CURSOR DEVICE_STATS (V_MODULESNDRM VARCHAR2,
                     V_START_DATE DATE,
                     V_END_DATE DATE)
IS
    SELECT 
        DEVICEID,
        MODULESNDRM,
        TRUNC(EVENTDATE) DT,
        WASHZONEID  WZ_ID 
    FROM 
        SVC_PHM_ODS.PHM_ODS_WASHASPIRATIONS
    WHERE     
        EVENTDATE BETWEEN 
            V_START_DATE 
        AND 
            V_END_DATE 
    AND 
        MODULESNDRM = V_MODULESNDRM  
    AND 
        EVENTDATE >= TRUNC(V_RUN_DATE-1)
    GROUP BY 
        DEVICEID,
        MODULESNDRM,
        TRUNC(EVENTDATE),
        WASHZONEID
    ORDER BY 3,4;

CURSOR DEVICE_AND_DATE_LIST
IS
    SELECT 
        ODS.DEVICEID,
        ODS.MODULESNDRM,
        TRUNC(MIN(ODS.EVENTDATE)) START_DATE, 
        MAX(EVENTDATE) END_DATE,
        IA.COUNTRYNAME,
        IA.AREAREGION,
        IA.CUSTOMERNAME,
        IA.CUSTOMERNUMBER,
        IA.MODULETYPE
    FROM 
        PHM_ODS_WASHASPIRATIONS ODS,
        IDAOWNER.IDAMODULEINFORMATION IA
    WHERE 
        ODS.BATCH_NUM = V_BATCH_NUM 
    AND       
        ODS.RUN_DATE = V_RUN_DATE 
    AND 
        ODS.MODULESNDRM = IA.MODULESN 
    --AND ODS.MODULESNDRM = 'ISR51878'
    AND IA.CREATEDATE = (
        SELECT 
            MAX(CREATEDATE) 
        FROM 
            IDAOWNER.IDAMODULEINFORMATION 
        WHERE 
            MODULESN = IA.MODULESN 
        AND
            CREATEDATE <= SYSDATE 
        AND 
            PRODUCTLINE<>'XXX')
    AND 
        ODS.EVENTDATE >= IA.EFFECTIVEFROMDATE 
    AND 
        ODS.EVENTDATE <= IA.EFFECTIVETODATE 
    AND 
        IA.AREA IS NOT NULL 
    AND
        IA.CUSTOMERNAME NOT LIKE '%ABBOTT%' 
    AND  
        IA.CUSTOMERNAME NOT LIKE '%Flextronics%'
    GROUP BY  
        ODS.DEVICEID,
        ODS.MODULESNDRM,
        IA.COUNTRYNAME,
        IA.AREAREGION,
        IA.CUSTOMERNAME,
        IA.CUSTOMERNUMBER,
        IA.MODULETYPE 
    ORDER BY 1,2,3;

CURSOR CUR_IHN3_MSG
IS
    SELECT  
        SN,
        FLAGDATE,
        COUNT(*) CNT
    FROM (
        SELECT DISTINCT 
            COUNTRY, 
            REGION, 
            CUSTOMER, 
            SN, 
            WZPROBE, 
            TRUNC(FLAGDATE) FLAGDATE, 
            FLAGA,

            CUSTOMERNUMBER,  
            IHN_LEVEL3_DESC, 
            TRUNC(DATE_CREATED) DATE_CREATED
        FROM 
            PHM_WAM_OUTPUT_1 
        where 
            BATCH_NUM=V_BATCH_NUM 
        AND  
            PHM_PATTERNS_SK = V_ALG_NUM 
        AND 
            TRUNC(DATE_CREATED)=TRUNC(SYSDATE)
        ) WAM_OUTPUT
    WHERE  
        FLAGA = 'YES' 
    AND 
        TRUNC(DATE_CREATED)=TRUNC(SYSDATE) 
    -- OR FLAGB = 'YES' OR  FLAGC = 'YES' )
    GROUP BY 
        SN,FLAGDATE
    ORDER BY 1,2;

CURSOR CUR_ALG_OUTPUT 
IS
    SELECT 
        *
    FROM (
        SELECT DISTINCT 
            CUSTOMERNAME AS CUSTOMER, 
            CUSTOMERNUMBER, 
            SN, 
            PHM_PRODUCT_LINE_CODE AS PL, 
            COUNTRY, 
            REGION, 
            PHM_ALGORITHM_DEFINITIONS_SK, 
            PHM_THRESHOLDS_SK,
            FLAGDATE FLAGDATE, 
            FLAGA, 
            IHN_LEVEL3_DESC, 
            CREATED_BY,  
            DATE_CREATED, 
            PRODUCT_LINE, 
            BATCH_NUM, 
            PHM_PATTERNS_SK,
            RANK() OVER (
                PARTITION BY 
                    BATCH_NUM, 
                    SN, 
                    TRUNC(FLAGDATE), 
                    PHM_PATTERNS_SK 
                ORDER BY 
                    FLAGA DESC, 
                    FLAGDATE ASC
            ) RANK
        FROM (
            SELECT DISTINCT 
                CUSTOMER CUSTOMERNAME, 
                CUSTOMERNUMBER, 
                SN, 
                PHM_PRODUCT_LINE_CODE, 
                COUNTRY, 
                REGION, 
                PHM_ALGORITHM_DEFINITIONS_SK, 
                PHM_THRESHOLDS_SK,
                FLAGDATE FLAGDATE, 
                FLAGA, 
                IHN_LEVEL3_DESC, 
                CREATED_BY, 
                TRUNC(DATE_CREATED) DATE_CREATED, 
                PRODUCT_LINE, 
                BATCH_NUM, 
                PHM_PATTERNS_SK
            FROM 
                PHM_WAM_OUTPUT_1 
            WHERE 
                BATCH_NUM= V_BATCH_NUM 
            AND  
                PHM_PATTERNS_SK = V_ALG_NUM 
            AND 
                TRUNC(DATE_CREATED)=TRUNC(SYSDATE))
        )
    WHERE 
        RANK=1;


VCOUNTRY    IDAOWNER.IDAMODULEINFORMATION.COUNTRYNAME%TYPE ;
VCITY       IDAOWNER.IDAMODULEINFORMATION.CITY%TYPE ;
VCUSTNAME   IDAOWNER.IDAMODULEINFORMATION.CUSTOMERNAME%TYPE ;
VCUST_NUM   IDAOWNER.IDAMODULEINFORMATION.CUSTOMERNUMBER%TYPE ;
VTYPE       IDAOWNER.IDAMODULEINFORMATION.MODULETYPE%TYPE ;

V_FLAG               VARCHAR(5);
NUM_DAYS             NUMBER(2);
V_FLAG_DATE          DATE;
IHN3_MSG             VARCHAR2(50);
MTYPE                VARCHAR2(50);
WZ_SUM               VARCHAR2(50);
WZ                   VARCHAR2(50);
PRB                  VARCHAR2(50);
V_OCUR_COUNT         NUMBER(10);
V_CONSEC_COUNT       NUMBER(10);
VALGNAME             VARCHAR(25);
V_ERROR_MESSAGE      VARCHAR(2000);
V_IHN4_CALL_MESSAGE  VARCHAR(150);
V_DEVICE_VALUE       NUMBER(15,7);
VMAX_THRESHOLD_UNIT  NUMBER(3);
V_PROCESS_TYPE       VARCHAR(25);
V_PROCESS_STATUS     VARCHAR2(25) := 'STARTED';
V_PROCESS_ID         NUMBER(15);
V_PROD_FAMILY        VARCHAR2(25);
V_RUN_MODE           VARCHAR2(10);
V_ROUTINE_NAME       VARCHAR(35);
V_ROUTINE_TYPE       VARCHAR(35);
V_ALG_TYPE           VARCHAR2(10);
V_INSERT_COUNT       NUMBER(25);
V_PAT_sK_A           PHM_PATTERNS.PHM_PATTERNS_SK%TYPE;
V_PAT_NAME_A         PHM_PATTERNS.PATTERN_NAME%TYPE;
V_THRESHOLD_SK_A     PHM_THRESHOLDS.PHM_THRESHOLDS_SK%TYPE := -1;
V_THRESHOLD_NUMBER_A PHM_THRESHOLDS.THRESHOLD_NUMBER%TYPE;
V_THRESHOLD_TEMP     NUMBER(10);
V_OCCURED_COUNT      NUMBER(10);
v_alg_sk             number(10) ;
V_DATE               DATE;
V_FLAGGED_PL         VARCHAR2(10);
V_FLAGGED_EXP_CODE   VARCHAR2(10);

V_EXCLUDE_STRING1 VARCHAR2(50) := '''%ABBOTT%''';
V_EXCLUDE_STRING2 VARCHAR2(50) := '''%Flextronics%''';
V_EXCLUDE_STRING3 VARCHAR2(50) := '''XXX''';
INSERT_STRING     VARCHAR2(10000);
V_RUN_DATE_STR    VARCHAR2(50);
V_PID             VARCHAR2(10);


CURSOR WAM_A_QUERY
IS
    SELECT  
        WAM.COUNTRY,    
        WAM.AREAREGION,    
        WAM.CUSTOMER,    
        WAM.CUSTOMERNUMBER,    
        WAM.SN, 
        WAM.PL,   
        WAM.WZPROBE,    
        WAM.FLAGDATE, 
        WAM.DEVICE_VALUE,
        WAM.FLAGA,   
        '' AS IHN_LEVEL3_DESC,    
        SYSDATE
    FROM (
        SELECT 
            ABC.COUNTRY,  
            ABC.AREAREGION,  
            ABC.CUSTOMER, 
            ABC.CUSTOMERNUMBER, 
            ABC.SN, 
            ABC.PL, 
            ABC.WZPROBE,
            (
                SELECT DISTINCT 
                    DM.MODULE_TYPE 
                FROM 
                    IDAMART.D_MODULE DM 
                WHERE 
                    ABC.SN=DM.MODULE_SN
            ) AS MODULE_TYPE,
            ABC.FLAGDATEA AS FLAGDATE,  
            ABC.DEVICE_VALUE AS DEVICE_VALUE, 
            ABC.FLAGA
        FROM (
            SELECT  
                A.COUNTRY AS COUNTRY, 
                A.AREAREGION AS AREAREGION,  
                A.CUSTOMER_NAME AS CUSTOMER, 
                A.CUSTOMERNUMBER   AS CUSTOMERNUMBER, 
                A.SN  AS SN, 
                A.PL AS PL,
                A.WZPROBEA AS WZPROBE,
                A.FLAGDATEA AS FLAGDATEA, 
                A.MAXTEMP AS DEVICE_VALUE, 
                CASE WHEN A.FLAGA = 'YES'  
                     THEN 'YES'  
                     ELSE 'NO' END   AS FLAGA
            FROM (
                SELECT  
                    RAWA.COUNTRY,  
                    RAWA.AREAREGION,  
                    RAWA.CUSTOMER_NAME,   
                    RAWA.CUSTOMERNUMBER,
                    RAWA.SN, 
                    RAWA.PL, 
                    RAWA.WZPROBE AS WZPROBEA, 
                    RAWA.FLAGA AS FLAGA, 
                    RAWA.FLAGDATEA AS FLAGDATEA, 
                    RAWA.MAXTEMP
                FROM (
                    SELECT 
                        IA.PRODUCTLINE AS PL, 
                        IA.AREA,IA.
                        COUNTRYNAME COUNTRY,
                        IA.AREAREGION,
                        IA.CUSTOMERNAME AS CUSTOMER_NAME,
                        IA.CUSTOMERNUMBER CUSTOMERNUMBER,

                        WZA.MODULESNDRM AS SN,
                        WZA.WASHZONEID || '.' || WZA.POSITION AS WZPROBE,
                        WZA.EVENTDATE AS FLAGDATEA,
                        WZA.MAXTEMP AS MAXTEMP,
                        CASE WHEN 
                                 WZA.MAXTEMP > V_THRESHOLD_TEMP 
                             AND
                                 LAG (WZA.MAXTEMP) OVER (
                                     PARTITION BY 
                                         WZA.MODULESNDRM, 
                                         WZA.WASHZONEID, 
                                         WZA.POSITION 
                                     ORDER BY WZA.EVENTDATE
                                 ) > V_THRESHOLD_TEMP 
                             AND
                                 LAG (WZA.MAXTEMP, 2) OVER (
                                     PARTITION BY 
                                         WZA.MODULESNDRM, 
                                         WZA.WASHZONEID, 
                                         WZA.POSITION 
                                     ORDER BY WZA.EVENTDATE
                                 ) > V_THRESHOLD_TEMP 
                             AND
                                 LAG (WZA.MAXTEMP, 3) OVER (
                                     PARTITION BY 
                                         WZA.MODULESNDRM, 
                                         WZA.WASHZONEID, 
                                         WZA.POSITION 
                                     ORDER BY WZA.EVENTDATE
                                 ) > V_THRESHOLD_TEMP 
                             AND
                                 LAG (WZA.MAXTEMP, 4) OVER (
                                     PARTITION BY 
                                         WZA.MODULESNDRM, 
                                         WZA.WASHZONEID, 
                                         WZA.POSITION 
                                     ORDER BY WZA.EVENTDATE
                                 ) > V_THRESHOLD_TEMP
                             THEN 'YES'  
                             ELSE 'NO' 
                             END AS FLAGA
                    FROM ( 
                        SELECT 
                            WA.MODULESNDRM,
                            WA.EVENTDATE,  
                            WA.WASHZONEID - 1 AS WASHZONEID,
                            '1' AS POSITION,
                            WA.POSITION1 AS REPLICATEID,
                            CASE WHEN 
                                     WA.POSITION1 = LAG (WA.POSITION1) OVER 
                                     (
                                         ORDER BY 
                                             WA.MODULESNDRM, 
                                             WA.POSITION1, 
                                             WA.WASHZONEID, 
                                             WA.EVENTDATE
                                     )
                                 AND
                                     WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
                                     (
                                         ORDER BY 
                                             WA.MODULESNDRM, 
                                             WA.POSITION1, 
                                             WA.WASHZONEID, 
                                             WA.EVENTDATE
                                     ) 
                                 AND
                                     WA.EVENTDATE - 10 /(24*60*60) < LAG (WA.EVENTDATE) OVER 
                                     (
                                         ORDER BY 
                                             WA.MODULESNDRM, 
                                             WA.POSITION1, 
                                             WA.WASHZONEID, 
                                             WA.EVENTDATE
                                     )
                                 THEN 'Probe 1 Second Temp'
                                 ELSE 'Probe 1 First Temp' 
                                 END
                            AS PIP_ORDER,WA.MAXTEMPPOSITION1/1000 MAXTEMP
                        FROM 
                            PHM_ODS_WASHASPIRATIONS WA
                        WHERE  
                            WA.POSITION1 > 0 
                        AND 
                            WA.EVENTDATE >= TRUNC(SYSDATE - 1) 
                        AND 
                            WA.EVENTDATE < TRUNC(SYSDATE)
                        AND 
                            WA.MODULESNDRM IN (
                                SELECT DISTINCT 
                                    MODULESNDRM 
                                FROM 
                                    PHM_ODS_WASHASPIRATIONS 
                                WHERE 
                                    BATCH_NUM=V_BATCH_NUM 
                                AND 
                                    EVENTDATE >= TRUNC(SYSDATE - 1) 
                                AND 
                                    EVENTDATE < TRUNC(SYSDATE)
                            )
                        UNION ALL
                        SELECT 
                            WA.MODULESNDRM,
                            WA.EVENTDATE, 
                            WA.WASHZONEID - 1 AS WASHZONEID, 
                            '2' AS POSITION,
                            WA.POSITION2 AS REPLICATEID,
                            'Probe 2' AS PIP_ORDER, 
                            WA.MAXTEMPPOSITION2/1000 MAXTEMP
                        FROM  
                            PHM_ODS_WASHASPIRATIONS WA
                        WHERE 
                            WA.POSITION2 > 0 
                        AND 
                            WA.EVENTDATE >= TRUNC(SYSDATE - 1) 
                        AND 
                            WA.EVENTDATE < TRUNC(SYSDATE)
                        AND 
                            WA.MODULESNDRM IN (
                                SELECT DISTINCT 
                                    MODULESNDRM 
                                FROM 
                                    PHM_ODS_WASHASPIRATIONS 
                                WHERE 
                                    BATCH_NUM=V_BATCH_NUM 
                                AND 
                                    EVENTDATE >= TRUNC(SYSDATE - 1) 
                                AND 
                                    EVENTDATE < TRUNC(SYSDATE)
                            )
                        UNION ALL
                        SELECT 
                            WA.MODULESNDRM,
                            WA.EVENTDATE,  
                            WA.WASHZONEID - 1 AS WASHZONEID,
                            '3' AS POSITION,
                            WA.POSITION3 AS REPLICATEID,
                            CASE WHEN 
                                     WA.POSITION3 = LAG (WA.POSITION3) OVER 
                                     (
                                         ORDER BY 
                                             WA.MODULESNDRM, 
                                             WA.POSITION3, 
                                             WA.WASHZONEID, 
                                             WA.EVENTDATE
                                     ) 
                                 AND
                                     WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
                                     (
                                         ORDER BY 
                                             WA.MODULESNDRM, 
                                             WA.POSITION3, 
                                             WA.WASHZONEID, 
                                             WA.EVENTDATE
                                     ) 
                                 AND
                                     WA.EVENTDATE - 10 /(24*60*60) < LAG (WA.EVENTDATE) OVER
                                     (
                                         ORDER BY 
                                             WA.MODULESNDRM, 
                                             WA.POSITION3, 
                                             WA.WASHZONEID, 
                                             WA.EVENTDATE
                                     )
                                 THEN 'Probe 3 Second Temp' 
                                 ELSE 'Probe 3 First Temp' 
                                 END AS PIP_ORDER, WA.MAXTEMPPOSITION3/1000 MAXTEMP
                        FROM 
                            PHM_ODS_WASHASPIRATIONS WA
                        WHERE 
                            WA.POSITION3 > 0 
                        AND 
                            WA.EVENTDATE >= TRUNC(SYSDATE - 1) 
                        AND 
                            WA.EVENTDATE < TRUNC(SYSDATE)
                        AND 
                            WA.MODULESNDRM IN (
                                SELECT DISTINCT 
                                    MODULESNDRM 
                                FROM 
                                    PHM_ODS_WASHASPIRATIONS 
                                WHERE 
                                    BATCH_NUM=V_BATCH_NUM 
                                AND 
                                    EVENTDATE >= TRUNC(SYSDATE - 1) 
                                AND 
                                    EVENTDATE < TRUNC(SYSDATE)
                            )
                         ) WZA 
                    INNER JOIN 
                        IDAOWNER.IDAMODULEINFORMATION IA
                    ON  
                        WZA.MODULESNDRM = IA.MODULESN 
                    AND
                        IA.CREATEDATE = ( 
                            SELECT 
                                MAX(CREATEDATE) 
                            from 
                                IDAOWNER.IDAMODULEINFORMATION 
                            where 
                                MODULESN = IA.MODULESN 
                            AND 
                                CREATEDATE <= SYSDATE)
                    AND
                        WZA.EVENTDATE > IA.EFFECTIVEFROMDATE 
                    AND   
                        WZA.EVENTDATE < IA.EFFECTIVETODATE
                    WHERE 
                        NOT WZA.PIP_ORDER = 'Probe 3 Second Temp' 
                    AND 
                        NOT WZA.PIP_ORDER = 'Probe 1 First Temp' 
                    AND
                        IA.AREA IS NOT NULL 
                    AND 
                        IA.CUSTOMERNAME NOT LIKE '%ABBOTT%' 
                    AND 
                        IA.CUSTOMERNAME NOT LIKE '%Flextronics%'   
                ) RAWA
            ) A
        ) ABC
    ) WAM;

BEGIN
    /* TO GET ALL TEH BASIC INFO FOR TEH ALGORITHM NUMBER PROVIDED */
    -- PHM_ALGORITHM_UTILITIES_1.PHM_GET_ALG_DETAILS(V_ALG_NUM, VALGNAME,V_PROCESS_TYPE,V_ROUTINE_NAME,V_RUN_MODE,V_PROD_FAMILY);
    V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();

    SELECT 
        AR.ROUTINE_NAME, 
        AR.ROUTINE_TYPE,
        AR.RUN_MODE,
        AR.ROUTINE_INVOKE_COMMAND,
        PF.PRODUCT_FAMILY_NAME
    INTO 
        VALGNAME,
        V_PROCESS_TYPE,
        V_RUN_MODE ,
        V_ROUTINE_NAME,
        V_PROD_FAMILY
    FROM 
        PHM_ALGORITHM_ROUTINES AR,
        PHM_PATTERNS PP , 
        PHM_PRODUCT_FAMILY PF
    WHERE 
        AR.PHM_PATTERNS_SK = V_ALG_NUM   
    AND 
        PP.PHM_PATTERNS_SK = AR.PHM_PATTERNS_SK
    AND 
        PP.PHM_PROD_FAMILY_SK = PF.PHM_PROD_FAMILY_SK  
    AND 
        NVL(PP.DELETE_FLAG,'N') <> 'Y';

    BEGIN
        SELECT 
            PHM_ALGORITHM_DEFINITIONS_SK  
        INTO 
            V_ALG_SK FROM PHM_PATTERNS
        WHERE 
            PHM_PATTERNS_SK= V_ALG_NUM ;
    EXCEPTION
        WHEN OTHERS 
        THEN
            V_ERROR_MESSAGE := ' UNABLE TO GET PATTERNA INFORMATION :'||VALGNAME||', ERROR :'|| SQLERRM;
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                            V_PROD_FAMILY,
                                                            V_PROCESS_TYPE,
                                                            V_ROUTINE_TYPE,
                                                            VALGNAME||'1',
                                                            V_ROUTINE_NAME,
                                                            V_RUN_MODE,
                                                            V_PROCESS_STATUS,
                                                            V_ERROR_MESSAGE,
                                                            V_RUN_DATE ,
                                                            SYSDATE,
                                                            V_BATCH_NUM ,
                                                            V_UNIX_ID,
                                                            V_ALG_NUM );
    COMMIT;
END;

FOR I IN ( 
    SELECT 
        PARAMETER_VALUES,
        PARAMETER_NAME,
        PHM_PATTERNS_SK 
    FROM 
        PHM_THRESHOLD_PARAMETER
    WHERE 
        PHM_PATTERNS_SK = V_ALG_NUM  
    AND 
        NVL(DELETE_FLAG,'N') <> 'Y'
    )
LOOP
    IF I.PARAMETER_NAME = 'ERROR_CODE_VALUE' 
    THEN 
        V_THRESHOLD_TEMP  :=  I.PARAMETER_VALUES;  
    END IF;
    IF I.PARAMETER_NAME = 'THRESHOLDS_COUNT' 
    THEN 
        V_OCCURED_COUNT   :=  I.PARAMETER_VALUES;  
    END IF;

END LOOP;

DBMS_OUTPUT.PUT_LINE(V_THRESHOLD_TEMP||'    '||V_OCCURED_COUNT);

IF VALGNAME IS NOT NULL 
THEN
    DBMS_OUTPUT.PUT_LINE('STARTED '||TO_CHAR(SYSDATE,'HH:MI:SS'));
    BEGIN
        DELETE FROM 
            PHM_ALG_OUTPUT 
        WHERE 
            PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_SK 
        AND 
            BATCH_NUM = V_BATCH_NUM 
        AND 
            PHM_PATTERNS_SK IN (V_ALG_NUM);

        --DELETE FROM PHM_WAM_OUTPUT_1 WHERE PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_SK AND BATCH_NUM = V_BATCH_NUM AND PHM_PATTERNS_SK IN (V_ALG_NUM);

        --DELETE FROM PHM_WAM_ALG_CHART_OUTPUT WHERE PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_SK AND BATCH_NUM = V_BATCH_NUM AND PHM_PATTERNS_SK IN (V_ALG_NUM);

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('OLD DATA DELETED'||TO_CHAR(SYSDATE,'HH:MI:SS'));

        EXCEPTION
            WHEN OTHERS 
            THEN /* TO CATCH ALL EXCEPTIONS WHILE DELETING THE DATA */
                --V_PROCESS_STATUS := 'ERRORED';
                V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA OF PREVIOUS RUN FOR RUN_DATE '||V_RUN_DATE||' FOR BATCH_NUM ' ||V_BATCH_NUM ||' DUE TO  : '||SQLERRM;
                PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,V_ALG_NUM,V_RUN_DATE,V_BATCH_NUM,V_ERROR_MESSAGE,VALGNAME);
           END;

        BEGIN
            EXECUTE IMMEDIATE 'ALTER TABLE PHM_WAM_ALG_CHART_OUTPUT TRUNCATE SUBPARTITION '||V_BATCH_NUM||VALGNAME||' DROP STORAGE UPDATE INDEXES';
            EXECUTE IMMEDIATE 'ALTER TABLE PHM_WAM_OUTPUT_1 TRUNCATE SUBPARTITION '||V_BATCH_NUM||VALGNAME||' DROP STORAGE UPDATE INDEXES';
            EXCEPTION
                WHEN OTHERS 
                THEN /* TO CATCH ALL EXCEPTIONS WHILE DELETING THE DATA */
                    V_PROCESS_STATUS := 'ERRORED';
                    dbms_output.put_line('ALTER TABLE PHM_WAM_ALG_CHART_OUTPUT TRUNCATE SUBPARTITION '||V_BATCH_NUM||VALGNAME||' DROP STORAGE UPDATE INDEXES');
                    V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA OF PREVIOUS RUN FOR RUN_DATE '||V_RUN_DATE||' FOR BATCH_NUM ' ||V_BATCH_NUM ||' DUE TO  : '||SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,
                                                                    V_ALG_NUM,
                                                                    V_RUN_DATE,
                                                                    V_BATCH_NUM,
                                                                    V_ERROR_MESSAGE,
                                                                    VALGNAME);
        END;

        DBMS_OUTPUT.PUT_LINE('OLD DATA truncated'||TO_CHAR(SYSDATE,'HH:MI:SS'));
    
        IF V_PROCESS_STATUS <>  'ERRORED' 
        THEN
            V_PROCESS_STATUS := 'STARTED'; /* TO INSERT AUDIT RECORDS AS PROCESS STARTED */
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                            V_PROD_FAMILY,
                                                            V_PROCESS_TYPE,
                                                            V_ROUTINE_TYPE,
                                                            VALGNAME,
                                                            V_ROUTINE_NAME,
                                                            V_RUN_MODE,
                                                            V_PROCESS_STATUS,
                                                            V_ERROR_MESSAGE,
                                                            V_RUN_DATE ,
                                                            SYSDATE,
                                                            V_BATCH_NUM ,
                                                            V_UNIX_ID ,
                                                            V_ALG_NUM);

            FOR F IN  WAM_A_QUERY
            LOOP
                INSERT INTO PHM_WAM_OUTPUT_1
                (
                    CUSTOMER, 
                    CUSTOMERNUMBER, 
                    DEVICE_ID, 
                    SN, 
                    COUNTRY, 
                    REGION, 
                    PHM_ALGORITHM_DEFINITIONS_SK, 
                    PHM_THRESHOLDS_SK, 
                    FLAGDATE , 
                    DEVICE_VALUE, 
                    WZPROBE, 
                    FLAGA, 
                    IHN_LEVEL3_DESC, 
                    REMARKS, 
                    CREATED_BY, 
                    DATE_CREATED, 
                    PRODUCT_LINE, 
                    BATCH_NUM, 
                    PHM_PATTERNS_SK, 
                    PHM_PRODUCT_LINE_CODE
                ) 
                VALUES 
                (
                    F.CUSTOMER, 
                    F.CUSTOMERNUMBER, 
                    NULL, 
                    F.SN, 
                    F.COUNTRY, 
                    F.AREAREGION, 
                    V_ALG_SK, 
                    V_THRESHOLD_SK_A, 
                    F.FLAGDATE, 
                    F.DEVICE_VALUE, 
                    F.WZPROBE, 
                    F.FLAGA, 
                    V_IHN4_CALL_MESSAGE, 
                    NULL, 
                    VALGNAME, 
                    sysdate, 
                    NULL, 
                    V_BATCH_NUM, 
                    V_ALG_NUM, 
                    F.PL
                );
                                        
                V_INSERT_COUNT := V_INSERT_COUNT + 1;

                IF MOD(V_INSERT_COUNT,10000) = 0 
                THEN 
                    COMMIT; 
                END IF;
            END LOOP;
        END IF;

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('MAIN DONE '||TO_CHAR(SYSDATE,'HH:MI:SS'));

        FOR X IN CUR_IHN3_MSG
        LOOP
            BEGIN
                IHN3_MSG := '';
                IF X.CNT = 1 THEN
                BEGIN
                    SELECT 
                        NVL(SUBSTR(W.WZPROBE,1,1),'NA'), 
                        NVL(SUBSTR(W.WZPROBE,3,1),'NA'), 
                        CASE WHEN W.PHM_PRODUCT_LINE_CODE = '117' 
                             THEN 
                                 'i1SR' 
                             WHEN W.PHM_PRODUCT_LINE_CODE = '116' 
                             THEN
                                 'i2SR'
                             WHEN W.PHM_PRODUCT_LINE_CODE = '115' 
                             THEN 
                                 'i2' 
                             END INTO WZ, 
                        PRB, 
                        MTYPE
                    FROM 
                        PHM_WAM_OUTPUT_1 W
                    WHERE 
                        W.WZPROBE IS NOT NULL 
                    AND  
                        PHM_PATTERNS_SK = V_ALG_NUM 
                    AND 
                        W.FLAGA = 'YES' 
                    AND 
                        W.SN = X.SN
                    AND 
                        W.BATCH_NUM = V_BATCH_NUM 
                    AND 
                        TRUNC(W.FLAGDATE) = TRUNC(X.FLAGDATE) 
                    AND 
                        ROWNUM < 2;

                    IF MTYPE = 'i1SR' 
                    then
                        WZ := '';
                        IHN3_MSG := 'WAM1 ' || MTYPE || ' WZ' || WZ || ' P' || PRB;
                    ELSE
                        IHN3_MSG := 'WAM1 ' || 'WZ' || WZ ||' P'||SUBSTR(PRB,1,1);
                    END IF;

                    UPDATE 
                        PHM_WAM_OUTPUT_1 
                    SET 
                        IHN_LEVEL3_DESC = IHN3_MSG  
                    WHERE 
                        SN = X.SN 
                    AND 
                        TRUNC(FLAGDATE) = TRUNC(X.FLAGDATE)
                    AND  
                        PHM_PATTERNS_SK = V_ALG_NUM  
                    AND 
                        BATCH_NUM = V_BATCH_NUM  
                    AND 
                        ( FLAGA = 'YES' );

                    EXCEPTION
                    WHEN OTHERS THEN
                        V_PROCESS_STATUS := 'ERRORED';
                        V_ERROR_MESSAGE := 'NOT ABLE TO UPDATE PHM_WAM_OUTPUT_1 FOR SN '||X.SN|| ' FOR DATE '||V_DATE||NULL||' FOR THE BATCH_NUM ' || V_BATCH_NUM||', RUN_DATE ' ||V_RUN_DATE ||'- DUE TO ' || SQLERRM;
                        DBMS_OUTPUT.PUT_LINE(V_ERROR_MESSAGE);

                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                                    V_PROD_FAMILY,
                                                                    V_PROCESS_TYPE,
                                                                    V_ROUTINE_TYPE,
                                                                    VALGNAME||'1',V_ROUTINE_NAME,
                                                                    V_RUN_MODE,
                                                                    V_PROCESS_STATUS,
                                                                    V_ERROR_MESSAGE,
                                                                    V_RUN_DATE ,
                                                                    SYSDATE,
                                                                    V_BATCH_NUM ,
                                                                    V_UNIX_ID,
                                                                    V_ALG_NUM );

                    END;
                ELSE
                   WZ_SUM := '';

                BEGIN
                    SELECT 
                        CASE WHEN W.PHM_PRODUCT_LINE_CODE = '117' 
                             THEN 'i1SR' 
                             WHEN W.PHM_PRODUCT_LINE_CODE = '116' 
                             THEN 'i2SR'
                             WHEN W.PHM_PRODUCT_LINE_CODE = '115' 
                             THEN 'i2' END 
                        INTO MTYPE
                    FROM 
                        PHM_WAM_OUTPUT_1 W
                    WHERE 
                        WZPROBE IS NOT NULL 
                    AND 
                        PHM_PATTERNS_SK = V_ALG_NUM 
                    AND 
                        W.FLAGA='YES' 
                    AND 
                        W.SN = X.SN
                    AND 
                        W.BATCH_NUM = V_BATCH_NUM 
                    AND 
                        TRUNC(W.FLAGDATE) =TRUNC(X.FLAGDATE) 
                    AND 
                        ROWNUM <2;

                    IHN3_MSG := 'WAM1 ' || MTYPE || ' WZ Multi Probe';

                    UPDATE
                        PHM_WAM_OUTPUT_1 
                    SET 
                        IHN_LEVEL3_DESC =  IHN3_MSG 
                    WHERE  
                        SN = X.SN 
                    AND
                        TRUNC(FLAGDATE) = TRUNC(X.FLAGDATE)
                    AND  
                        HM_PATTERNS_SK = V_ALG_NUM  
                    AND 
                        ATCH_NUM = V_BATCH_NUM  
                    AND 
                        ( FLAGA = 'YES');

                    EXCEPTION
                    WHEN OTHERS 
                    THEN
                        V_PROCESS_STATUS := 'ERRORED';
                        V_ERROR_MESSAGE := 'NOT ABLE TO UPDATE PHM_WAM_OUTPUT_1 FOR SN '||X.SN|| ' FOR DATE '||V_DATE||NULL||' FOR THE BATCH_NUM ' || V_BATCH_NUM||', RUN_DATE ' ||V_RUN_DATE ||'- DUE TO ' || SQLERRM;
                        DBMS_OUTPUT.PUT_LINE(V_ERROR_MESSAGE);
                        PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                                        V_PROD_FAMILY,
                                                                        V_PROCESS_TYPE,
                                                                        V_ROUTINE_TYPE,
                                                                        VALGNAME||'1',
                                                                        V_ROUTINE_NAME,
                                                                        V_RUN_MODE,
                                                                        V_PROCESS_STATUS,
                                                                        V_ERROR_MESSAGE,
                                                                        V_RUN_DATE ,
                                                                        SYSDATE,
                                                                        V_BATCH_NUM ,
                                                                        V_UNIX_ID,
                                                                        V_ALG_NUM );
                    END;
                END IF;
            END;
        END LOOP;

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('IHN DONE'||TO_CHAR(SYSDATE,'HH:MI:SS'));

        FOR Y IN CUR_ALG_OUTPUT
        LOOP
            BEGIN
            IF (Y.SN IS NOT NULL) 
            THEN
                BEGIN
                IF (Y.FLAGA = 'YES') THEN
                    V_FLAGGED_PL := NULL;
                    V_FLAGGED_EXP_CODE := NULL;
                    PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(Y.PHM_PATTERNS_SK, 
                                                                  Y.PL, 
                                                                  Y.IHN_LEVEL3_DESC, 
                                                                  V_FLAGGED_PL, 
                                                                  V_FLAGGED_EXP_CODE);
                ELSE 
                    V_FLAGGED_PL := NULL;
                    V_FLAGGED_EXP_CODE := NULL;
                END IF;

                PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(Y.CUSTOMER,
                                                                         Y.CUSTOMERNUMBER,
                                                                         -1,
                                                                         Y.SN,
                                                                         Y.COUNTRY,
                                                                         Y.REGION,
                                                                         Y.PHM_ALGORITHM_DEFINITIONS_SK,
                                                                         Y.PHM_THRESHOLDS_SK,
                                                                         Y.FLAGDATE,
                                                                         NULL,
                                                                         Y.FLAGA,
                                                                         Y.IHN_LEVEL3_DESC,
                                                                         NULL,
                                                                         Y.CREATED_BY,
                                                                         NULL, 
                                                                         V_BATCH_NUM,
                                                                         Y.PHM_PATTERNS_SK,
                                                                         V_RUN_DATE,
                                                                         V_PROCESS_ID, 
                                                                         V_FLAGGED_PL, 
                                                                         V_FLAGGED_EXP_CODE);
                EXCEPTION
                WHEN OTHERS THEN
                    V_PROCESS_STATUS := 'ERRORED';
                    V_ERROR_MESSAGE := 'NOT ABLE TO LOAD DATE TO COMMON OUTPUT TABLE FOR SN '||Y.SN|| ' FOR DATE '||V_DATE||' FOR THRESHOLDS_SK '|| Y.PHM_THRESHOLDS_SK||' FOR THE BATCH_NUM ' || V_BATCH_NUM||', RUN_DATE ' ||V_RUN_DATE ||'- DUE TO ' || SQLERRM;
                    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                                    V_PROD_FAMILY,
                                                                    V_PROCESS_TYPE,
                                                                    V_ROUTINE_TYPE,
                                                                    VALGNAME||'1',
                                                                    V_ROUTINE_NAME,
                                                                    V_RUN_MODE,
                                                                    V_PROCESS_STATUS,
                                                                    V_ERROR_MESSAGE,
                                                                    V_RUN_DATE ,
                                                                    SYSDATE,
                                                                    V_BATCH_NUM ,
                                                                    V_UNIX_ID,
                                                                    V_ALG_NUM );
                    COMMIT;
                END;
            END IF;
            END;
            V_INSERT_COUNT := V_INSERT_COUNT + 1;
            IF MOD(V_INSERT_COUNT,10000) = 0 THEN COMMIT; END IF;
        END LOOP;

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('OUTPUT DONE '||TO_CHAR(SYSDATE,'HH:MI:SS'));

        V_RUN_DATE_STR := 'TO_DATE('''||TO_CHAR(V_RUN_DATE,'DDMMYYYY')||''',''DDMMYYYY'')';
        V_PID := '.1';


        INSERT_STRING := '
            INSERT INTO 
                PHM_WAM_ALG_CHART_OUTPUT 
            VALUE
            (
                SELECT 
                    Z.CUSTOMER, 
                    Z.CUSTOMERNUMBER, 
                    Z.DEVICE_ID, 
                    Z.SN, 
                    Z.COUNTRY, 
                    Z.REGION,
                    '||V_ALG_SK||', 
                    0, 
                    Z.FLAGDATE,
                    Z.DEVICE_VALUE, 
                    Z.WZPROBE,
                    NULL,
                    NULL,
                    NULL,
                    '''||VALGNAME||''',
                    SYSDATE,
                    NULL,
                    Z.BATCH_NUM,
                    '||V_ALG_NUM||',
                    '||V_PROCESS_ID||'
                FROM 
                    PHM_WAM_OUTPUT_1 Z 
                WHERE 
                    Z.BATCH_NUM = '''||V_BATCH_NUM||''' 
                AND 
                    PHM_ALGORITHM_DEFINITIONS_SK ='''||V_ALG_SK||''' 
                AND  
                    Z.DEVICE_VALUE<>0)';

        BEGIN
            dbms_output.put_line(INSERT_STRING);
            EXECUTE IMMEDIATE   INSERT_STRING;
            EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(SQLERRM);
        END;

        DBMS_OUTPUT.PUT_LINE('CHART DONE '||TO_CHAR(SYSDATE,'HH:MI:SS'));

ELSE  /* LOGGING THE ERROR MESSAGE IF BASIC ALGORITHM DETAILS NOT FOUND  */
    V_ERROR_MESSAGE := 'NOT ABLE TO FIND BASIC DETAILS OF ALGORITHM '||V_ALG_NUM||' DUT TO '||SQLERRM;
    PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG (V_PROCESS_ID,
                                                    V_ALG_NUM,
                                                    V_RUN_DATE,
                                                    V_BATCH_NUM,
                                                    V_ERROR_MESSAGE,
                                                    VALGNAME);
END IF;

IF V_PROCESS_STATUS <> 'ERRORED' 
THEN /* UPDATING THE ALGORITHM STATUS TO COMPLETED IF NOT ERROR OCCURS  */
    V_PROCESS_STATUS := 'COMPLETED';
    V_ERROR_MESSAGE := '';
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                    V_PROD_FAMILY,
                                                    V_PROCESS_TYPE,
                                                    V_ROUTINE_TYPE,
                                                    VALGNAME,
                                                    V_ROUTINE_NAME,
                                                    V_RUN_MODE,
                                                    V_PROCESS_STATUS,
                                                    V_ERROR_MESSAGE,
                                                    V_RUN_DATE ,
                                                    SYSDATE,
                                                    V_BATCH_NUM ,
                                                    V_UNIX_ID,
                                                    V_ALG_NUM );
END IF;

COMMIT;
EXCEPTION
WHEN OTHERS THEN
    V_PROCESS_STATUS := 'ERRORED';
    V_ERROR_MESSAGE := 'ALGORITHM PROCES HAS FAILED FOR ALGORITHM '||V_ALG_NUM||' FOR THE BATCH_NUM ' || V_BATCH_NUM||', RUN_DATE ' ||V_RUN_DATE ||'- DUE TO ' || SQLERRM;
    PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                    '',
                                                    V_PROCESS_TYPE,
                                                    V_ROUTINE_TYPE,
                                                    VALGNAME,
                                                    V_ROUTINE_NAME,
                                                    V_RUN_MODE,
                                                    V_PROCESS_STATUS,
                                                    V_ERROR_MESSAGE,
                                                    V_RUN_DATE ,
                                                    SYSDATE,
                                                    V_BATCH_NUM ,
                                                    V_UNIX_ID,
                                                    V_ALG_NUM );
END PHM_WAM_A;

CREATE OR REPLACE PUBLIC SYNONYM PHM_WAM_A FOR SVC_PHM_OWNER.PHM_WAM_A;
GRANT EXECUTE ON  SVC_PHM_OWNER.PHM_WAM_A  TO SVC_PHM_CONNECT;
GRANT EXECUTE ON  SVC_PHM_OWNER.PHM_WAM_A  TO SVC_PHM_CONNECT_ROLE;

