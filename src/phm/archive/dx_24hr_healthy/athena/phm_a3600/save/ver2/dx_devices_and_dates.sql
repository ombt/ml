-- 
-- INSERT INTO dx.dx_a3600_error
-- (
--   a3600_countrycode,
--   a3600_customernumber,
--   a3600_deviceid,
--   a3600_fileversion,
--   a3600_iom_productline,
--   a3600_iom_serial,
--   a3600_layoutinstance,
--   a3600_nodeid,
--   a3600_nodetype,
--   a3600_productline,
--   a3600_serialnumber,
--   date_,
--   derived_created_dt,
--   duplicate,
--   errorcode,
--   file_path,
--   hash_,
--   hr_,
--   laboratory,
--   list_nbr,
--   list_sale_sz,
--   moreinfo,
--   nodeid,
--   nodetype,
--   "off-line",
--   output_created_dt,
--   parsed_created_dt,
--   pkey,
--   sampleid,
--   software_version,
--   system_id,
--   TIMESTAMP,
--   timestamp_iso,
--   tresataid__customer,
--   tresataid__customer_a,
--   transaction_date
-- )
-- VALUES
-- (
--   'a3600_countrycode_value',
--   'a3600_customernumber_value',
--   'a3600_deviceid_value',
--   'a3600_fileversion_value',
--   'a3600_iom_productline_value',
--   'a3600_iom_serial_value',
--   'a3600_layoutinstance_value',
--   'a3600_nodeid_value',
--   'a3600_nodetype_value',
--   'a3600_productline_value',
--   'a3600_serialnumber_value',
--   date__value,
--   'derived_created_dt_value',
--   'duplicate_value',
--   'errorcode_value',
--   'file_path_value',
--   'hash__value',
--   hr__value,
--   'laboratory_value',
--   'list_nbr_value',
--   'list_sale_sz_value',
--   'moreinfo_value',
--   'nodeid_value',
--   'nodetype_value',
--   'off-line_value',
--   'output_created_dt_value',
--   'parsed_created_dt_value',
--   'pkey_value',
--   'sampleid_value',
--   'software_version_value',
--   'system_id_value',
--   'timestamp_value',
--   timestamp_iso_value,
--   'tresataid__customer_value',
--   'tresataid__customer_a_value',
--   'transaction_date_value'
-- );
-- 

--    -- cursor device_and_dates (
--       -- v_nodetype1     varchar2, = Low Temperature CM 0665,81,MODULE,CM
--       -- v_errorcode1    varchar2) = Low Temperature CM 0665,81,ERROR_CODE_VALUE,0665
--    -- is
-- -- Heater Error SM 909B,82,MODULE,SM
-- -- Heater Error SM 909B,82,ERROR_CODE_VALUE,909B
-- -- Carrier Runaway From Load Gate IOM 04E2,83,MODULE,IOM
-- -- Carrier Runaway From Load Gate IOM 04E2,83,ERROR_CODE_VALUE,04E2
-- -- Carrier At STAT Input Gate in Incorrect Status ISR 5012,84,MODULE,ISR
-- -- Carrier At STAT Input Gate in Incorrect Status ISR 5012,84,ERROR_CODE_VALUE,5012
-- -- Carrier Runaway From Unload Gate IOM 04F8,88,MODULE,IOM
-- -- Carrier Runaway From Unload Gate IOM 04F8,88,ERROR_CODE_VALUE,04F8
-- 
-- select 
--     asi.productlineref,
--     asi.deviceid,
--     asi.systemsn,
--     aln.sn,
--     ae.nodetype,
--     ae.errorcode,
--     max (ae.completiondate) max_compl_date,
--     trunc (min (ae.completiondate)) min_compl_date
-- from 
--     svc_phm_ods.phm_ods_a3600_errors ae,
--     a3600_layout_nodes_pl_sn aln,
--     idaowner.a3600systeminformation asi
-- where
--     -- batch_num = v_batch_num
-- -- and 
--     run_date = to_date('2019-08-01', 'YYYY-MM-DD') -- v_run_date
-- -- and 
--     -- ae.errorcode = '04F8' -- v_errorcode1
-- -- and 
--     ae.layout_nodes_id = aln.layout_nodes_id
-- and 
--     aln.systeminfoid = asi.systeminfoid
-- and 
--     aln.sn is not null
-- and 
--     aln.canid = ae.nodeid
-- and 
--     asi.current_row = 'Y'
-- -- and 
--     -- (('IOM' != '%' and ae.nodetype = 'IOM') or
--      -- ('IOM' = '%' and ae.nodetype like 'IOM'))
-- group by 
--     asi.productlineref,
--     asi.deviceid,
--     asi.systemsn,
--     aln.sn,
--     ae.nodetype,
--     ae.errorcode
-- order by 
--     asi.systemsn, 
--     ae.nodetype, 
--     ae.errorcode

select 
    ae.a3600_iom_productline,
    ae.a3600_deviceid,
    ae.a3600_iom_serial,
    ae.a3600_serialnumber,
    ae.a3600_nodetype,
    ae.errorcode,
    max (ae.timestamp_iso) as max_compl_date,
    date_trunc('day', min(ae.timestamp_iso)) as min_compl_date
from 
    dx.dx_a3600_error ae
where
    ae.transaction_date = '2019-08-01' -- v_run_date
and 
    ae.errorcode = '0665' -- v_errorcode1
and 
    (('CM' != '%' and ae.a3600_nodetype = 'CM') or
     ('CM' = '%' and ae.a3600_nodetype like 'CM'))
group by 
    ae.a3600_iom_productline,
    ae.a3600_deviceid,
    ae.a3600_iom_serial,
    ae.a3600_serialnumber,
    ae.a3600_nodetype,
    ae.errorcode
order by 
    ae.a3600_iom_serial,
    ae.a3600_nodetype,
    ae.errorcode
