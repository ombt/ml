-- 
-- svc_phm_owner.phm_algorithm_definitions
--
-- "ALGORITHM_COMMAND",5,,,"Y","VARCHAR2 (500 Char)",,"None",2,159,0.5,,,,False
-- "ALGORITHM_DESCRIPTION",6,,,"Y","VARCHAR2 (2000 Char)",,"None",150,0,0.00667,,,,False
-- "ALGORITHM_MODE",21,,,"Y","VARCHAR2 (15 Char)",,"None",1,160,1,,,,False
-- "ALGORITHM_NAME",4,,,"N","VARCHAR2 (200 Byte)",,"Height Balanced",163,0,0.00305,,,,False
-- "ALGORITHM_NUMBER",3,,,"N","INTEGER",,"Frequency",160,0,0.00305,,,,False
-- "ALGORITHM_OWNER",7,,,"N","VARCHAR2 (30 Char)",,"None",4,0,0.25,,,,False
-- "ALGORITHM_TYPE",20,,,"Y","VARCHAR2 (25 Char)",,"None",1,160,1,,,,False
-- "APP_AUDITS_CREATE_DATE",24,,,"Y","TIMESTAMP(6)",,"None",164,0,0.0061,,,,False
-- "APP_AUDITS_CREATE_USER",17,,,"N","VARCHAR2 (50 Char)",,"None",6,0,0.16667,,,,False
-- "APP_AUDITS_MODIFY_DATE",25,,,"Y","TIMESTAMP(6)",,"None",161,0,0.00621,,,,False
-- "APP_AUDITS_MODIFY_USER",18,,,"N","VARCHAR2 (50 Char)",,"None",4,0,0.25,,,,False
-- "AREA_OF_TRACK",16,,,"Y","VARCHAR2 (50 Char)",,"None",0,164,0,,,,False
-- "DELETE_FLAG",23,,,"Y","CHAR (1 Char)","'N'","Frequency",1,0,0.00305,,,,False
-- "ENABLED_YN",15,,,"Y","CHAR (1 Byte)","'N'","Frequency",2,0,0.00305,,,,False
-- "KEEP_RESULTS_NUM_DAYS",14,,,"Y","INTEGER",,"None",1,1,1,,,,False
-- "NEW_REC",22,,,"Y","CHAR (1 Char)",,"None",1,5,1,,,,False
-- "PHM_ALGORITHM_CATEGORIES_SK",2,,,"N","INTEGER",,"Frequency",20,0,0.00305,,,,False
-- "PHM_ALGORITHM_DEFINITIONS_SK",1,1,"1","N","INTEGER",,"Height Balanced",229,0,0.00218,,,,False
-- "PHM_PROD_FAMILY_SK",19,,,"Y","NUMBER",,"Frequency",6,0,0.00305,,,,False
-- "PROACTIVE_YN",11,,,"Y","CHAR (1 Char)",,"None",1,159,1,,,,False
-- "REMAINING_USEFUL_LIFE_INT",12,,,"Y","INTEGER",,"None",3,1,0.33333,,,,False
-- "REMAINING_USEFUL_LIFE_UNIT",13,,,"Y","VARCHAR2 (15 Char)",,"None",5,1,0.2,,,,False
-- "SQL_CODE",8,,"1","Y","CLOB",,"None",0,160,0,,,,False
-- "SQL_CODE_COMMAND",9,,,"Y","VARCHAR2 (500 Char)",,"None",5,159,0.2,,,,False
-- "SQL_DESCRIPTION",10,,,"Y","VARCHAR2 (500 Char)",,"None",5,159,0.2,,,,False
--
-- svc_phm_owner.phm_algorithm_ihns
--
-- "APP_AUDITS_CREATE_DATE",11,,,"Y","TIMESTAMP(6)",,"None",218,0,0.00459,,,,False
-- "APP_AUDITS_CREATE_USER",6,,,"N","VARCHAR2 (50 Char)",,"None",12,0,0.08333,,,,False
-- "APP_AUDITS_MODIFY_DATE",12,,,"Y","TIMESTAMP(6)",,"None",361,0,0.00277,,,,False
-- "APP_AUDITS_MODIFY_USER",7,,,"N","VARCHAR2 (50 Char)",,"None",5,0,0.2,,,,False
-- "DELETE_FLAG",10,,,"Y","CHAR (1 Char)","'N'","Frequency",1,0,0.00075,,,,False
-- "IHN_CODE",4,,,"Y","VARCHAR2 (200 Char)",,"Frequency",191,8,0.00076,,,,False
-- "IHN_LEVEL_1_DESCRIPTION",5,,,"Y","VARCHAR2 (2000 Char)",,"None",4,467,0.25,,,,False
-- "IHN_LEVEL_2_DESCRIPTION",8,,,"Y","VARCHAR2 (2000 Char)",,"None",42,467,0.02381,,,,False
-- "IHN_LEVEL_3_DESCRIPTION",9,,,"Y","VARCHAR2 (2000 Char)",,"None",0,667,0,,,,False
-- "ISSUE_DESCRIPTION",13,,,"Y","VARCHAR2 (250 Byte)",,"None",333,334,0.003,,,,False
-- "PHM_ALGORITHM_DEFINITIONS_SK",2,,,"N","INTEGER",,"Frequency",162,0,0.00075,,,,False
-- "PHM_ALGORITHM_IHN_SK",1,1,"1","N","INTEGER",,"Height Balanced",733,0,0.00136,,,,False
-- "PHM_PATTERNS_SK",3,,,"N","INTEGER",,"Height Balanced",667,0,0.0015,,,,False
--
-- svc_phm_owner.phm_algorithm_routines
--
-- "APP_AUDITS_CREATE_DATE",25,,,"Y","TIMESTAMP(6)",,"None",345,0,0.0029,,,,False
-- "APP_AUDITS_CREATE_USER",24,,,"Y","VARCHAR2 (50 Char)",,"None",9,0,0.11111,,,,False
-- "APP_AUDITS_MODIFY_DATE",27,,,"Y","TIMESTAMP(6)",,"None",361,0,0.00277,,,,False
-- "APP_AUDITS_MODIFY_USER",26,,,"Y","VARCHAR2 (50 Char)",,"None",5,0,0.2,,,,False
-- "DELETE_FLAG",23,,,"Y","CHAR (1 Char)","'N'","Frequency",1,0,0.00075,,,,False
-- "DEPLOYED_DATE",19,,,"Y","TIMESTAMP(6)",,"None",26,613,0.03846,,,,False
-- "DEPLOYED_USER",17,,,"Y","VARCHAR2 (100 Char)",,"Frequency",1,613,0.00926,,,,False
-- "DEPLOYMENT_LOG",18,,,"Y","VARCHAR2 (4000 Char)",,"None",1,613,1,,,,False
-- "DEPLOYMENT_STATUS",20,,,"Y","VARCHAR2 (50 Char)",,"None",1,613,1,,,,False
-- "ENABLE_YN",22,,,"Y","CHAR (1 Char)","'N'","Frequency",2,0,0.00075,,,,False
-- "LAST_DEPLOYED_DATE",14,,,"Y","TIMESTAMP(6)",,"None",17,650,0.05882,,,,False
-- "LAST_DEPLOYMENT_STATUS",21,,,"Y","VARCHAR2 (100 Char)",,"None",1,653,1,,,,False
-- "MIME_TYPE",12,,,"Y","VARCHAR2 (250 Char)",,"None",2,643,0.5,,,,False
-- "PHM_ALGORITHM_ROUTINE_SK",1,,,"Y","NUMBER (38)",,"None",667,0,0.0015,,,,False
-- "PHM_PATTERNS_SK",2,,,"Y","NUMBER (38)",,"Height Balanced",667,0,0.0015,,,,False
-- "PHM_PRODUCT_ROUTINE_SK",3,,,"Y","NUMBER",,"Frequency",17,517,0.00333,,,,False
-- "PHM_REUSABLE_ROUTINE_SK",4,,,"Y","NUMBER",,"Frequency",8,543,0.00403,,,,False
-- "PROCEDURE_SOURCE",16,,,"Y","VARCHAR2 (100 Char)",,"Frequency",3,0,0.00075,,,,False
-- "REQUEST_DEPLOY",13,,,"Y","CHAR (1 Char)",,"None",0,667,0,,,,False
-- "REQUESTED_USER",15,,,"Y","VARCHAR2 (12 Char)",,"None",0,667,0,,,,False
-- "ROUTINE_DESCRIPTION",7,,,"Y","VARCHAR2 (2000 Char)",,"None",655,0,0.00153,,,,False
-- "ROUTINE_FILE_CONTENT",10,,"1","Y","BLOB",,"None",0,613,0,,,,False
-- "ROUTINE_FILE_NAME",9,,,"Y","VARCHAR2 (500 Char)",,"None",24,643,0.04167,,,,False
-- "ROUTINE_INVOKE_COMMAND",11,,,"Y","VARCHAR2 (100 Char)",,"Frequency",24,509,0.00316,,,,False
-- "ROUTINE_NAME",6,,,"Y","VARCHAR2 (200 Byte)",,"Height Balanced",667,0,0.00182,,,,False
-- "ROUTINE_TYPE",5,,,"Y","VARCHAR2 (100 Char)",,"Frequency",1,507,0.00313,,,,False
-- "RUN_MODE",8,,,"Y","VARCHAR2 (25 Char)",,"None",2,0,0.5,,,,False
--

select
    -- ad.algorithm_command,
    ad.algorithm_description,
    -- ad.algorithm_mode,
    ad.algorithm_name,
    ad.algorithm_number,
    ad.algorithm_owner,
    -- ad.algorithm_type,
    ad.app_audits_create_date,
    ad.app_audits_create_user,
    ad.app_audits_modify_date,
    ad.app_audits_modify_user,
    -- ad.area_of_track,
    -- ad.delete_flag,
    ad.enabled_yn,
    -- ad.keep_results_num_days,
    -- ad.new_rec,
    ad.phm_algorithm_categories_sk,
    ad.phm_algorithm_definitions_sk,
    ad.phm_prod_family_sk,
    -- ad.proactive_yn,
    -- ad.remaining_useful_life_int,
    -- ad.remaining_useful_life_unit,
    -- ad.sql_code,
    -- ad.sql_code_command,
    -- ad.sql_description,
    -- ai.app_audits_create_date,
    -- ai.app_audits_create_user,
    -- ai.app_audits_modify_date,
    -- ai.app_audits_modify_user,
    -- ai.delete_flag,
    -- ai.ihn_code,
    -- ai.ihn_level_1_description,
    -- ai.ihn_level_2_description,
    -- ai.ihn_level_3_description,
    -- ai.issue_description,
    ai.phm_algorithm_definitions_sk,
    ai.phm_algorithm_ihn_sk,
    ai.phm_patterns_sk,
    -- ar.app_audits_create_date,
    -- ar.app_audits_create_user,
    -- ar.app_audits_modify_date,
    -- ar.app_audits_modify_user,
    -- ar.delete_flag,
    -- ar.deployed_date,
    -- ar.deployed_user,
    -- ar.deployment_log,
    -- ar.deployment_status,
    ar.enable_yn,
    -- ar.last_deployed_date,
    -- ar.last_deployment_status,
    -- ar.mime_type,
    ar.phm_algorithm_routine_sk,
    ar.phm_patterns_sk,
    ar.phm_product_routine_sk,
    ar.phm_reusable_routine_sk,
    ar.procedure_source,
    -- ar.request_deploy,
    -- ar.requested_user,
    ar.routine_description,
    ar.routine_file_content,
    ar.routine_file_name,
    ar.routine_invoke_command,
    ar.routine_name,
    ar.routine_type,
    ar.run_mode,
    null as dummy
from
    svc_phm_owner.phm_algorithm_definitions ad
inner join
    svc_phm_owner.phm_algorithm_ihns ai
on
    ad.phm_algorithm_definitions_sk = ai.phm_algorithm_definitions_sk
inner join
    svc_phm_owner.phm_algorithm_routines ar
on
    ai.phm_patterns_sk = ar.phm_patterns_sk
and
    ar.enable_yn = 'Y'
where
    ad.algorithm_name like '%Trigger%'
or
    ad.algorithm_name like '%Pick%'
or
    ad.algorithm_name like '%HC%'

