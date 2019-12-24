-- PROCEDURE PHM_GET_ALG_DETAILS (V_ALG_SK IN NUMBER, 
--                                VALGNAME OUT VARCHAR2,
--                                V_PROCESS_TYPE OUT VARCHAR2,
--                                V_ROUTINE_NAME OUT VARCHAR2,
--                                V_RUN_MODE OUT VARCHAR2,
--                                V_PROD_FAMILY OUT VARCHAR2)
-- AS
-- VSQLERRORMSG  VARCHAR2(1000);
-- BEGIN
-- 
--         SELECT ALGORITHM_NAME,ALGORITHM_TYPE,ALGORITHM_MODE,SQL_CODE_COMMAND,PRODUCT_FAMILY_NAME
--         INTO VALGNAME,V_PROCESS_TYPE,V_RUN_MODE,V_ROUTINE_NAME,V_PROD_FAMILY
--         FROM PHM_ALGORITHM_DEFINITIONS AD,PHM_PRODUCT_FAMILY PF WHERE PHM_ALGORITHM_DEFINITIONS_SK = V_ALG_SK
--         AND PF.PHM_PROD_FAMILY_SK = AD.PHM_PROD_FAMILY_SK;
-- 
-- 
-- EXCEPTION
--  WHEN OTHERS THEN
-- 
--    V_ROUTINE_NAME := NULL;
--    V_RUN_MODE     := NULL;
--    V_PROCESS_TYPE := NULL;
--    VSQLERRORMSG :=  ' GETTING ODS DETILS FAILED FOR V_PROD_ODS_ROUTINE_SK '||V_ALG_SK||', WITH ERROR :'|| SQLERRM;
--    PHM_ALGORITHM_UTILITIES_1.PHM_ALGORITHM_RUNLOG ('',V_ALG_SK,'',11,VSQLERRORMSG,'PHM_GET_ODS_DETAILS');
-- END;
-- /*************************** PHM_GET_ALG_DETAILS END **************************/

select 
    algorithm_name as valgname,
    algorithm_type as v_process_type,
    algorithm_mode as v_run_mode,
    sql_code_command as v_routine_name,
    product_family_name  as v_prod_family
from 
    phm_algorithm_definitions ad,
    phm_product_family pf 
where 
    phm_algorithm_definitions_sk =  1040 -- v_alg_sk
and 
    pf.phm_prod_family_sk = ad.phm_prod_family_sk
