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

select
    ae.transaction_date,
    ae.a3600_countrycode,
    ae.a3600_customernumber,
    cast(ae.a3600_nodeid as integer) as a3600_nodeid,
    ae.a3600_nodetype,
    ae.a3600_layoutinstance,
    ae.a3600_serialnumber,
    ae.a3600_deviceid,
    ae.a3600_productline,
    ae.a3600_fileversion,
    ae.a3600_iom_productline,
    ae.a3600_iom_serial,
    ae.a3600_nodetype,
    ae.a3600_productline,

    -- ae.date_,
    -- ae.derived_created_dt,
    -- ae.duplicate,
    -- ae.errorcode,
    -- ae.file_path,
    -- ae.hash_,
    -- ae.hr_,
    -- ae.laboratory,
    -- ae.list_nbr,
    -- ae.list_sale_sz,
    -- ae.moreinfo,
    -- -- ae.nodeid,
    -- -- ae.nodetype,
    -- ae."off-line",
    -- ae.output_created_dt,
    -- ae.parsed_created_dt,
    -- ae.pkey,
    -- ae.sampleid,
    -- ae.software_version,
    -- ae.system_id,
    -- ae.TIMESTAMP,
    -- ae.timestamp_iso,
    -- ae.tresataid__customer,
    -- ae.tresataid__customer_a,
    -- -- ae.transaction_date,
    null as dummy
from 
    dx.dx_a3600_error ae
where
    '2019-07-01' <= ae.transaction_date
and
    ae.transaction_date < '2019-07-02'
order by
    ae.a3600_countrycode,
    ae.a3600_customernumber,
    cast(ae.a3600_nodeid as integer)

