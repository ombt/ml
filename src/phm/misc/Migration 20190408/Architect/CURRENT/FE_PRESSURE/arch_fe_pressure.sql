CREATE OR REPLACE 
PROCEDURE SVC_PHM_OWNER.PHM_FE_PRESSURE(V_ALG_NUM NUMBER,
                                        V_RUN_DATE DATE,
                                        V_BATCH_NUM VARCHAR2, 
                                        V_UNIX_ID VARCHAR2)
IS
/******************************************************************************
   NAME:       PHM_FE_PRESSURE
   PURPOSE:   FE PRESSURE DATA WILL COLLECTED AND VERIFIED AGAINST THE THRESHOLD VALUES

   REVISIONS:
   Ver        Date        Author            Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        08/02/2015   Sivakrishna Angadi Procedure Created  
   2.0        07/12/2016   Sivakrishna Angadi Procedure updated for sun 
                                              approach and batch run  
   3.0        10/13/2017    Siva Chandaluri   Updated the procedure to use 
                                              instrumentlisting for instrument/
                                              customer/country info and populate 
                                              PL/EC Code in PHM_ALG_OUTPUT table   
******************************************************************************/

CURSOR DEVICE_STATS (V_PIPETTER VARCHAR2,
                     V_MODULESNDRM VARCHAR2,
                     V_START_DATE DATE,
                     DAYS_NUM NUMBER,
                     V_END_DATE DATE)
IS
    SELECT 
        * 
    FROM (
        SELECT 
            DEVICEID,
            MODULESNDRM,
            TRUNC(COMPLETIONDATE) DT,
            MAX(COMPLETIONDATE) FLAGDATE,
            COALESCE(
                CASE WHEN SUBSTR(P.MODULESNDRM,1,2)='I1' 
                     THEN 
                         'I1' 
                     END,
                CASE WHEN P.location IN ('INNER_REAGENT', 
                                         'MEDIAL_REAGENT', 
                                         'OUTER_REAGENT', 
                                         'R1_INNER_REAGENT', 
                                         'R1_MEDIAL_REAGENT',
                                         'R1_OUTER_REAGENT') AND
                          P.PIPETTER = ('PTRGNT1') 
                     THEN 
                          'R1' 
                     END,
                CASE WHEN P.location IN ('INNER_REAGENT', 
                                         'OUTER_REAGENT', 
                                         'R1_MEDIAL_REAGENT', 
                                         'R1_OUTER_REAGENT') AND
                          P.PIPETTER = ('PTRGNT2')
                     THEN 
                         'R1' 
                     END,
                CASE WHEN P.location IN ('INNER_REAGENT', 
                                         'MEDIAL_REAGENT', 
                                         'OUTER_REAGENT', 
                                         'R1_INNER_REAGENT', 
                                         'R1_OUTER_REAGENT', 
                                         'R1_MEDIAL_REAGENT') AND
                          P.PIPETTER = ('RGNT1') 
                     THEN 
                         'R1' 
                     END,
                CASE WHEN P.location IN ( 'RV2') AND
                          P.PIPETTER = ('RGNT1')
                     THEN 
                         'R1' 
                     END,
                CASE WHEN P.location IN ('RV48') AND
                          P.PIPETTER = ('RGNT1')
                     THEN 
                         'R2' 
                     END,
                CASE WHEN P.location IN ('R2_INNER_REAGENT', 
                                         'R2_OUTER_REAGENT', 
                                         'R2_MEDIAL_REAGENT') AND
                          P.PIPETTER = ('RGNT1') 
                     THEN 
                         'R2' 
                     END,
                CASE WHEN P.location IN ('MEDIAL_REAGENT', 
                                         'R2_INNER_REAGENT') AND
                          P.PIPETTER = ('RGNT2')
                     THEN 
                         'R2'
                     END,
                CASE WHEN P.location IN ('MEDIAL_REAGENT', 
                                         'R2_INNER_REAGENT') AND
                          P.PIPETTER = ('RGNT3')
                     THEN 
                         'R2'
                     END,
                CASE WHEN P.location IN ('RV24') AND
                          P.PIPETTER IN ('PTSAMP1', 
                                         'PTSAMP2')
                     THEN 
                         'SAMP'
                     END,
                CASE WHEN P.location IN ('RV2') AND
                          P.PIPETTER IN ('PTSAMP1', 
                                         'PTSAMP2')
                     THEN 
                         'R1'
                     END,
                CASE WHEN P.location IN ('ISH_SAMPLE', 
                                         'LAS_SAMPLE') AND
                          P.PIPETTER IN ('SAMP')
                     THEN 
                         'SAMP'
                     END,
                CASE WHEN P.location IN ('STAT_SAMPLE') AND
                          P.PIPETTER IN ('SAMP')
                     THEN 
                         'STATSAMP'
                     ELSE 
                         P.PIPETTER 
                     END
                ) PIPETTER,
            MEDIAN(FRONTENDPRESSURE) MED_PRSR,
            MAX(COMPLETIONDATE) MAX_COMP_DATE
        FROM 
            SVC_PHM_ODS.PHM_ODS_PRESSURES_IA P
        WHERE 
            LOADDATE BETWEEN 
                V_START_DATE - DAYS_NUM 
            AND 
                V_END_DATE 
        GROUP BY 
            DEVICEID,
            MODULESNDRM,
            TRUNC(COMPLETIONDATE),
            COALESCE( 
                CASE WHEN SUBSTR(P.MODULESNDRM,1,2)='I1' 
                     THEN 
                         'I1' 
                     END,
                CASE WHEN P.location IN ('INNER_REAGENT', 
                                         'MEDIAL_REAGENT', 
                                         'OUTER_REAGENT', 
                                         'R1_INNER_REAGENT', 
                                         'R1_MEDIAL_REAGENT',
                                         'R1_OUTER_REAGENT') AND
                          P.PIPETTER = ('PTRGNT1') 
                     THEN 
                         'R1'
                     END,
                CASE WHEN P.location IN ('INNER_REAGENT', 
                                         'OUTER_REAGENT', 
                                         'R1_MEDIAL_REAGENT', 
                                         'R1_OUTER_REAGENT') AND
                          P.PIPETTER = ('PTRGNT2')
                     THEN 
                         'R1'
                     END,
                CASE WHEN P.location IN ('INNER_REAGENT', 
                                         'MEDIAL_REAGENT', 
                                         'OUTER_REAGENT', 
                                         'R1_INNER_REAGENT', 
                                         'R1_OUTER_REAGENT', 
                                         'R1_MEDIAL_REAGENT') AND
                          P.PIPETTER = ('RGNT1') 
                     THEN 
                         'R1'
                     END,
                CASE WHEN P.location IN ('RV2') AND
                          P.PIPETTER = ('RGNT1')
                     THEN 
                         'R1'
                     END,
                CASE WHEN P.location IN ('RV48') AND
                          P.PIPETTER = ('RGNT1')
                     THEN 
                         'R2'
                     END,
                CASE WHEN P.location IN ('R2_INNER_REAGENT', 
                                         'R2_OUTER_REAGENT', 
                                         'R2_MEDIAL_REAGENT') AND
                          P.PIPETTER = ('RGNT1') 
                     THEN 
                         'R2'
                     END,
                CASE WHEN P.location IN ('MEDIAL_REAGENT', 
                                         'R2_INNER_REAGENT') AND
                          P.PIPETTER = ('RGNT2')
                     THEN 
                         'R2'
                     END,
                CASE WHEN P.location IN ('MEDIAL_REAGENT', 
                                         'R2_INNER_REAGENT') AND
                          P.PIPETTER = ('RGNT3')
                     THEN 
                         'R2'
                     END,
                CASE WHEN P.location IN ('RV24') AND
                          P.PIPETTER IN ('PTSAMP1', 'PTSAMP2')
                     THEN 
                         'SAMP'
                     END,
                CASE WHEN P.location IN ('RV2') AND
                          P.PIPETTER IN ('PTSAMP1', 
                                         'PTSAMP2')
                     THEN 
                         'R1'
                     END,
                CASE WHEN P.location IN ('ISH_SAMPLE', 
                                         'LAS_SAMPLE') AND
                          P.PIPETTER IN ('SAMP')
                     THEN 
                         'SAMP'
                     END,
                CASE WHEN P.location IN ('STAT_SAMPLE') AND
                          P.PIPETTER IN ('SAMP')
                     THEN 
                         'STATSAMP'
                     ELSE 
                         P.PIPETTER 
                     END
            ) 
    ) 
    WHERE 
        PIPETTER = V_PIPETTER 
    ORDER BY 
        1,2,3,4;

--CURSOR DEVICE_AND_DATE_LIST
--IS 
--SELECT ODS.MODULESNDRM,IMI.COUNTRYNAME,IMI.AREAREGION,IMI.CUSTOMERNAME,IMI.CUSTOMERNUMBER,IMI.MODULETYPE,
--MIN(TRUNC(ODS.COMPLETIONDATE)) CUR_MIN_DATE,MAX(ODS.COMPLETIONDATE) CUR_MAX_DATE
--FROM SVC_PHM_ODS.PHM_ODS_PRESSURES_IA ODS, IDAOWNER.IDAMODULEINFORMATION IMI 
--WHERE RUN_DATE = V_RUN_DATE AND BATCH_NUM = V_BATCH_NUM AND IMI.MODULESN = ODS.MODULESNDRM 
--AND IMI.CUSTOMERNUMBER IS NOT NULL AND IMI.MODULETYPE LIKE 'I%'
--AND IMI.EFFECTIVEFROMDATE <= SYSDATE AND IMI.EFFECTIVETODATE >= SYSDATE 
--GROUP BY ODS.MODULESNDRM, IMI.COUNTRYNAME,IMI.AREAREGION,IMI.CUSTOMERNAME,IMI.CUSTOMERNUMBER,IMI.MODULETYPE
--ORDER BY 1,2;

CURSOR DEVICE_AND_DATE_LIST
IS 
    SELECT  
        IA.DEVICEID, 
        UPPER(IA.MODULESNDRM) MODULESNDRM, 
        MAX (IL.PL) PL, 
        MAX (IL.CUSTOMER_NUM) CUSTOMERNUMBER,
        MAX (IL.CUSTOMER) CUSTOMERNAME, 
        MAX (PC.COUNTRY) COUNTRYNAME, 
        MAX (PC.AREAREGION) AREA, 
        MAX (IL.CITY) CITY, 
        MIN(TRUNC(IA.COMPLETIONDATE)) CUR_MIN_DATE, 
        MAX(IA.COMPLETIONDATE) CUR_MAX_DATE
    FROM 
        SVC_PHM_ODS.PHM_ODS_PRESSURES_IA IA, 
        INSTRUMENTLISTING IL, 
        PHM_COUNTRY PC
    WHERE 
        IA.BATCH_NUM = V_BATCH_NUM 
    AND 
        IA.RUN_DATE = V_RUN_DATE 
    AND 
        UPPER (IA.MODULESNDRM) = UPPER (IL.SN) 
    AND 
        PC.COUNTRY_CODE = IL.COUNTRY_CODE
    GROUP BY 
        IA.DEVICEID, 
        IA.MODULESNDRM
    ORDER BY 2;  


CURSOR ALL_THRESHOLDS (VALGNUM NUMBER)
IS 
    SELECT 
        PTT.PHM_THRESHOLDS_SK,
        PTT.THRESHOLD_NUMBER,
        PTT.THRESHOLD_NUMBER_UNIT,
        PTT.THRESHOLD_NUMBER_DESC,
        PP.PHM_PATTERNS_SK,
        PP.PATTERN_DESCRIPTION,
        PTT.THRESHOLD_ALERT,
        PTT.THRESHOLD_TYPE, 
        PTT.THRESHOLD_DATA_DAYS
    FROM 
        PHM_PATTERNS PP, 
        PHM_THRESHOLDS PTT 
    WHERE 
        PP.PHM_PATTERNS_SK = PTT.PHM_PATTERNS_SK
    AND 
        PP.PHM_ALGORITHM_DEFINITIONS_SK = PTT.PHM_ALGORITHM_DEFINITIONS_SK
    AND 
        PP.PHM_ALGORITHM_DEFINITIONS_SK = VALGNUM;


VCOUNTRY    IDAOWNER.IDAMODULEINFORMATION.COUNTRYNAME%TYPE ;
VCITY       IDAOWNER.IDAMODULEINFORMATION.CITY%TYPE ;
VCUSTNAME   IDAOWNER.IDAMODULEINFORMATION.CUSTOMERNAME%TYPE ; 
VCUST_NUM   IDAOWNER.IDAMODULEINFORMATION.CUSTOMERNUMBER%TYPE ;
VTYPE       IDAOWNER.IDAMODULEINFORMATION.MODULETYPE%TYPE ;

FLAG                VARCHAR(5);
NUM_DAYS            NUMBER(2);
VMAX_THRESHOLD_UNIT NUMBER(3);
VIHN4_CALL_MESSAGE  VARCHAR(150);
VALGEXEC_SEQ        VARCHAR(25);
V_END_DATE          DATE;  
V_CUR_MIN_DATE      DATE; 
V_CUR_MAX_DATE      DATE;
VALGNAME            VARCHAR(25);
V_ERROR_MESSAGE     VARCHAR(2000);
V_PROCESS_TYPE      VARCHAR(25); 
V_PROCESS_STATUS    VARCHAR(25) := 'STARTED';
V_PROD_FAMILY       VARCHAR2(50);
V_RUN_MODE          VARCHAR2(10);
V_ROUTINE_NAME      VARCHAR(35);
V_ROUTINE_TYPE      VARCHAR(35) := 'Oracle Procedure';
V_ALG_TYPE          VARCHAR2(50);
V_DATE              DATE;
V_PROCESS_ID        NUMBER(25);
V_INSERT_COUNT      NUMBER(25);
V_FLAGGED_PL        VARCHAR2(10);
V_FLAGGED_EXP_CODE  VARCHAR2(10);

BEGIN

    V_END_DATE := SYSDATE;
    
    PHM_ALGORITHM_UTILITIES_1.PHM_GET_ALG_DETAILS(V_ALG_NUM, 
                                                  VALGNAME,
                                                  V_PROCESS_TYPE,
                                                  V_ROUTINE_NAME,
                                                  V_RUN_MODE,
                                                  V_PROD_FAMILY);
    
    IF VALGNAME IS NOT NULL 
    THEN
    BEGIN
        DELETE FROM 
            PHM_FE_PRSR 
        WHERE 
            BATCH_NUM = V_BATCH_NUM 
        AND 
            RUN_DATE = V_RUN_DATE;
        DELETE FROM 
            PHM_ALG_OUTPUT 
        WHERE 
            PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_NUM  
        AND  
            BATCH_NUM = V_BATCH_NUM;
        
        COMMIT;

        EXCEPTION WHEN OTHERS 
        THEN
            V_PROCESS_STATUS := 'ERRORED';
            V_ERROR_MESSAGE := 'NOT ABLE TO DELETE THE DATA OF PREVIOUS RUN FOR RUN_DATE '||V_RUN_DATE||' FOR BATCH_NUM ' ||V_BATCH_NUM ||' DUE TO  : '||SQLERRM;
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
        END;
    
        COMMIT; 
         
        IF V_PROCESS_STATUS <>  'ERRORED' 
        THEN
            V_PROCESS_ID := PHM_ALGORITHM_UTILITIES_1.PHM_GET_PROCESS_ID();
      
            V_PROCESS_STATUS := 'STARTED';

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
     
            SELECT 
                MIN(COMPLETIONDATE),
                MAX(COMPLETIONDATE) 
            INTO 
                V_CUR_MIN_DATE, 
                V_CUR_MAX_DATE
            FROM 
                SVC_PHM_ODS.PHM_ODS_PRESSURES_IA
            WHERE 
                BATCH_NUM = V_BATCH_NUM 
            AND 
                RUN_DATE = V_RUN_DATE;
    
            FOR AP IN ALL_THRESHOLDS (V_ALG_NUM)
            LOOP
                FOR DS IN DEVICE_STATS (AP.PATTERN_DESCRIPTION,
                                        ' ',
                                        TRUNC(V_CUR_MIN_DATE),
                                        AP.THRESHOLD_DATA_DAYS,
                                        TRUNC(V_CUR_MAX_DATE))
                LOOP
                BEGIN 
                    INSERT INTO 
                        PHM_FE_PRSR VALUES (V_RUN_DATE,
                                            V_BATCH_NUM,
                                            AP.PHM_THRESHOLDS_SK,
                                            DS.DEVICEID,
                                            DS.MODULESNDRM, 
                                            DS.PIPETTER,
                                            DS.MAX_COMP_DATE,
                                            TRUNC(DS.MED_PRSR,5),
                                            0,
                                            VALGNAME, 
                                            SYSDATE, 
                                            V_UNIX_ID);
                    V_INSERT_COUNT := V_INSERT_COUNT + 1;
                    IF MOD(V_INSERT_COUNT,10000) = 0 THEN COMMIT; END IF;                                 

                    EXCEPTION 
                    WHEN OTHERS 
                    THEN
                        V_PROCESS_STATUS := 'ERRORED';
                        V_ERROR_MESSAGE := 'NOT ABLE TO INSERT DATA INTO PHM_FE_PRSR FOR THE BATCH_NUM' || V_BATCH_NUM||', RUN_DATE ' ||V_RUN_DATE ||'- DUE TO ' || SQLERRM;
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
                    END;                                            
                END LOOP;
            END LOOP;

            COMMIT;

            FOR AP IN ALL_THRESHOLDS(V_ALG_NUM)
            LOOP
                FOR DD IN DEVICE_AND_DATE_LIST
                LOOP
                    VIHN4_CALL_MESSAGE :='';
                    FLAG := 'NO';
                    NUM_DAYS := 0;
                    V_FLAGGED_PL := NULL;
                    V_FLAGGED_EXP_CODE := NULL;

                    BEGIN 
                       FOR X IN (
                           SELECT * 
                           FROM 
                               PHM_FE_PRSR 
                           WHERE 
                               BATCH_NUM = V_BATCH_NUM 
                           AND 
                               RUN_DATE = V_RUN_DATE 
                           AND 
                               MODULESNDRM = DD.MODULESNDRM 
                           AND 
                               PHM_THRESHOLDS_SK = AP.PHM_THRESHOLDS_SK 
                           ORDER BY 
                               COMPLETIONDATE )
                        LOOP
                            V_DATE := X.COMPLETIONDATE;
                            IF X.AVG_FE_PRSR > AP.THRESHOLD_NUMBER 
                            THEN
                                NUM_DAYS := NUM_DAYS + 1;
                                IF NUM_DAYS >= AP.THRESHOLD_NUMBER_UNIT 
                                THEN
                                    FLAG := 'YES';
                                    VIHN4_CALL_MESSAGE := AP.THRESHOLD_ALERT;
                                    PHM_ALGORITHM_UTILITIES_1.PHM_GET_PL_EXP_CODE(AP.PHM_PATTERNS_SK, 
                                                                                  DD.PL
                                                                                  , VIHN4_CALL_MESSAGE, 
                                                                                  V_FLAGGED_PL, 
                                                                                  V_FLAGGED_EXP_CODE);  
                                ELSE
                                    FLAG := 'NO';
                                END IF;
                            ELSE
                                FLAG := 'NO';     
                                NUM_DAYS := 0;
                            END IF; 
              
                            IF X.COMPLETIONDATE >= DD.CUR_MIN_DATE 
                            THEN 
                                PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_OUTPUT_IN_EXP_PL(DD.CUSTOMERNAME,
                                                                                         DD.CUSTOMERNUMBER,
                                                                                         X.DEVICEID,
                                                                                         X.MODULESNDRM,
                                                                                         DD.COUNTRYNAME,
                                                                                         DD.AREA,
                                                                                         V_ALG_NUM,
                                                                                         AP.PHM_THRESHOLDS_SK,
                                                                                         X.COMPLETIONDATE,
                                                                                         X.AVG_FE_PRSR,
                                                                                         FLAG,VIHN4_CALL_MESSAGE,
                                                                                         VALGEXEC_SEQ,
                                                                                         VALGNAME,
                                                                                         '',
                                                                                         V_BATCH_NUM,
                                                                                         AP.PHM_PATTERNS_SK,
                                                                                         V_RUN_DATE,
                                                                                         V_PROCESS_ID, 
                                                                                         V_FLAGGED_PL, 
                                                                                         V_FLAGGED_EXP_CODE);
                            END IF;             
                            V_INSERT_COUNT := V_INSERT_COUNT + 1;
                            IF MOD(V_INSERT_COUNT,10000) = 0 THEN COMMIT; END IF;
                        END LOOP;

                        EXCEPTION WHEN OTHERS 
                        THEN
                            V_PROCESS_STATUS := 'ERRORED';
                            V_ERROR_MESSAGE := 'NOT ABLE TO PROCES THE THRESHOLD CONDITION FOR SN '||DD.MODULESNDRM|| ' FOR DATE '||V_DATE||' FOR THRESHOLDS_SK '|| AP.PHM_THRESHOLDS_SK||' FOR THE BATCH_NUM ' || V_BATCH_NUM||', RUN_DATE ' ||V_RUN_DATE ||'- DUE TO ' || SQLERRM;
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
                    END;
                END LOOP;
            END LOOP;
        END IF; 
    
        IF V_PROCESS_STATUS <> 'ERRORED' 
        THEN 
            V_PROCESS_STATUS := 'COMPLETED';
            V_ERROR_MESSAGE :='';
            PHM_ALGORITHM_UTILITIES_1.PHM_PROCESS_AUDIT_LOG(V_PROCESS_ID,
                                                            V_PROD_FAMILY,
                                                            V_PROCESS_TYPE,
                                                            V_ROUTINE_TYPE,
                                                            VALGNAME,
                                                            V_ROUTINE_NAME,
                                                            V_RUN_MODE,
                                                            V_PROCESS_STATUS,
                                                            ' ',
                                                            V_RUN_DATE ,
                                                            SYSDATE,
                                                            V_BATCH_NUM ,
                                                            V_UNIX_ID,
                                                            V_ALG_NUM );
        END IF; 
      
    COMMIT;
END IF;   
   
EXCEPTION WHEN OTHERS 
THEN
    V_ERROR_MESSAGE := 'NOT ABLE TO EXECUTE PHM_FE_PRESSURS ALGORITHM DUE TO   :'||SQLERRM;
    V_PROCESS_STATUS := 'ERRORED';
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
    COMMIT;
END PHM_FE_PRESSURE;

CREATE OR REPLACE PUBLIC SYNONYM PHM_FE_PRESSURE FOR SVC_PHM_OWNER.PHM_FE_PRESSURE;
GRANT EXECUTE ON  SVC_PHM_OWNER.PHM_FE_PRESSURE  TO SVC_PHM_CONNECT;
GRANT EXECUTE ON  SVC_PHM_OWNER.PHM_FE_PRESSURE  TO SVC_PHM_CONNECT_ROLE;
