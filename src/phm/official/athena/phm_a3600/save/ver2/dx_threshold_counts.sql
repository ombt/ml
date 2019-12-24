
   -- cursor device_and_dates (
      -- v_nodetype1     varchar2, = Low Temperature CM 0665,81,MODULE,CM
      -- v_errorcode1    varchar2) = Low Temperature CM 0665,81,ERROR_CODE_VALUE,0665
   -- is
-- Heater Error SM 909B,82,MODULE,SM
-- Heater Error SM 909B,82,ERROR_CODE_VALUE,909B
-- Carrier Runaway From Load Gate IOM 04E2,83,MODULE,IOM
-- Carrier Runaway From Load Gate IOM 04E2,83,ERROR_CODE_VALUE,04E2
-- Carrier At STAT Input Gate in Incorrect Status ISR 5012,84,MODULE,ISR
-- Carrier At STAT Input Gate in Incorrect Status ISR 5012,84,ERROR_CODE_VALUE,5012
-- Carrier Runaway From Unload Gate IOM 04F8,88,MODULE,IOM
-- Carrier Runaway From Unload Gate IOM 04F8,88,ERROR_CODE_VALUE,04F8

   -- CURSOR THRESHOLD_COUNTS (
      -- V_SN1             VARCHAR2,
      -- V_NODETYPE1       VARCHAR2,
      -- V_ERRORCODE1      VARCHAR2,
      -- V_START_DATE      DATE,
      -- V_END_DATE        DATE,
      -- V_DATA_DAYS       NUMBER,
      -- V_SAMP_ID_CHK1    VARCHAR2,
      -- V_SAMP_ID_CHK2    VARCHAR2)
   -- IS

select 
    ae2.a3600_deviceid,
    ae2.a3600_iom_serial,
    ae2.a3600_iom_productline,
    ae2.a3600_serialnumber,
    date_trunc('day', ae2.timestamp_iso) as flag_date,
    ae2.a3600_nodetype,
    ae2.errorcode,
    ae2.a3600_nodeid,
    ae2.a3600_layoutinstance,
    ac.tubestoday,
    max(ae2.timestamp_iso) as max_compl_date,
    count(ae2.errorcode) as error_count,
    (count(ae2.errorcode) * 100.0 / ac.tubestoday) as error_percentage
from 
    dx.dx_a3600_error ae2
    dx.dx_a3600_counter ac,
    {
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
        ae.transaction_date = '<V_RUN_DATE>'
    and 
        ae.errorcode = '<V_ERRORCODE1>'
    and 
        (('<V_NODETYPE1>' != '%' and ae.a3600_nodetype = '<V_NODETYPE1>') or
         ('<V_NODETYPE1>' = '%' and ae.a3600_nodetype like '<V_NODETYPE1>'))
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
    ) dd
where
    toupper(trim(ae2.a3600_serialnumber)) = toupper(trim(dd.a3600_serialnumber))
and
    toupper(trim(ac.a3600_serialnumber)) = toupper(trim(dd.a3600_serialnumber))
and
    ac.a3600_nodetype = ae2.a3600_nodetype
and
    ac.counter_date = date_trunc('day', ae2.timestamp_iso)
and
    ac.a3600_nodeid = ae.a3600_nodeid
and
    ac.a3600_layoutinstance = ae2.a3600_layoutinstance
and
    (('<V_NODETYPE1>' != '%' and ae2.nodetype = '<V_NODETYPE1>')
or
    ('<V_NODETYPE1>' = '%' and ae2.nodetype like '<V_NODETYPE1>'))
and
    ae.a3600_errorcode = '<V_ERRORCODE1>'
and
    coalesce(ae.sampleid, '<V_SAMP_ID_CHK1>') like coalesce('<V_SAMP_ID_CHK2>', ae.sampleid)
and
    ae2.timestamp_iso between 
        dd.min_compl_date - interval '<V_DATA_DAYS>' day + inteval '1' day
    and
        dd.max_compl_date
group by 
    ae2.a3600_deviceid,
    ae2.a3600_iom_serial,
    ae2.a3600_iom_productline,
    ae2.a3600_serialnumber,
    date_trunc('day', ae2.timestamp_iso),
    ae2.a3600_nodetype,
    ae2.errorcode,
    ae2.a3600_nodeid,
    ae2.a3600_layoutinstance,
    ac.tubestoday
