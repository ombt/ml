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

with phm_ods_a3600_errors as (
select 
    a36err.transaction_date,
    a36err.file_path,
    a36err.errorcode,
    a36err.a3600_nodeid,
    a36err.a3600_nodetype,
    a36err.nodeid,
    a36err.nodetype,
    a36err.a3600_layoutinstance,
    a36err.TIMESTAMP,
    a36err.timestamp_iso,
    a36err.sampleid,
    a36err.moreinfo,
    a36err."off-line",
    a36err.a3600_countrycode,
    a36err.a3600_customernumber,
    a36err.a3600_deviceid,
    a36err.a3600_fileversion,
    a36err.a3600_iom_productline,
    a36err.a3600_iom_serial,
    a36err.a3600_productline,
    a36err.a3600_serialnumber,
    a36err.date_,
    a36err.derived_created_dt,
    a36err.duplicate,
    a36err.hash_,
    a36err.hr_,
    a36err.laboratory,
    a36err.list_nbr,
    a36err.list_sale_sz,
    a36err.output_created_dt,
    a36err.parsed_created_dt,
    a36err.pkey,
    a36err.software_version,
    a36err.system_id,
    a36err.tresataid__customer,
    a36err.tresataid__customer_a,
    null as dummy
from
    dx.dx_a3600_error a36err
where 
    -- '<START_DATE>' <= a36err.transaction_date
    '2019-07-01' <= a36err.transaction_date
and
    -- a36err.transaction_date < '<END_DATE>'
    a36err.transaction_date < '2019-07-07'
order by 
    a36err.transaction_date
)
select

from 
    phm_ods_a3600_errors
