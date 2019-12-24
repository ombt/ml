-- select 
--     * 
-- from 
--     svc_phm_owner.phm_algorithm_routines 
-- where 
--     routine_name like '%Cuvette%'
-- 
-- select 
--     count(*) 
-- from 
--     svc_phm_owner.phm_threshold_parameter
-- where 
--     routine_name like '%Cuvette%'
-- 

select count(*) from svc_phm_ods.phm_ods_pressures_dis;

select count(*) from svc_phm_ods.phm_ods_results_cc;


