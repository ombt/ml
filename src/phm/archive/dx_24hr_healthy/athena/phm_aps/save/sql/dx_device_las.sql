-- 
--         SELECT 
--             MAX(A.DEVICE_ID) DEVICE_ID, 
--             MAX(A.IOM_SN) IOM_SN, 
--             MAX(A.PL) PL, 
--             MAX(A.SN) SN, 
--             A.DT, 
--             MAX(A.TIMESTAMP) MAX_TIMESTAMP, 
--             COUNT(*) LAS_ERROR_COUNT 
--         FROM (
--             SELECT 
--                 AE.DEVICE_ID, 
--                 PSM.IOM_SN, 
--                 PSM.PL, 
--                 PSM.SN, 
--                 TRUNC(AE.TIMESTAMP) DT, 
--                 AE.TIMESTAMP 
--             from 
--                 SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, 
--                 SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM 
--             WHERE 
--                 -- AE.MESSAGE = LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' 
--             (
--                 (AE.MESSAGE LIKE 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (AE.MESSAGE LIKE 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (AE.MESSAGE LIKE 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (AE.MESSAGE LIKE 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (AE.MESSAGE LIKE 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (AE.MESSAGE LIKE 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (AE.MESSAGE LIKE 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (AE.MESSAGE LIKE 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) 
--             )
--             AND 
--                 TIMESTAMP BETWEEN 
--                     -- V_START_DATE 
--                     to_timestamp('09/18/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
--                 AND 
--                     -- V_END_DATE
--                     to_timestamp('09/19/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
--             AND 
--                 -- PSM.MESSAGE LIKE LAS_PATTERN_TEXT || ': Error 205: Unreadable Barcode' || '%' 
--             (
--                 (PSM.MESSAGE LIKE 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (PSM.MESSAGE LIKE 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (PSM.MESSAGE LIKE 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (PSM.MESSAGE LIKE 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (PSM.MESSAGE LIKE 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (PSM.MESSAGE LIKE 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (PSM.MESSAGE LIKE 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
--                 (PSM.MESSAGE LIKE 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) 
--             )
--             and 
--                 AE.SN = PSM.IOM_SN
--         ) A, 
--         (
--             SELECT 
--                 DEVICE_ID, 
--                 SN, 
--                 TRUNC(TIMESTAMP) DT, 
--                 TIMESTAMP 
--             from 
--                 SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE 
--             WHERE 
--                 MESSAGE like 'Interface Module Unreadable Barcode -%' 
--             AND 
--                 TIMESTAMP BETWEEN 
--                     -- V_START_DATE 
--                     to_timestamp('09/18/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
--                 AND 
--                     -- V_END_DATE
--                     to_timestamp('09/19/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
--         ) B
--     WHERE 
--         A.TIMESTAMP = B.TIMESTAMP
--     GROUP BY 
--         A.DT 
--     ORDER BY 1, 2, 3;


-- INSERT INTO dx.dx_aps_error
-- (
--   countrycode,
--   customernumber,
--   date_,
--   derived_created_dt,
--   duplicate,
--   file_path,
--   hash_,
--   hr_,
--   laboratory,
--   list_nbr,
--   list_sale_sz,
--   message,
--   output_created_dt,
--   parsed_created_dt,
--   pkey,
--   productline,
--   serialnumber,
--   software_version,
--   status,
--   system_id,
--   TIMESTAMP,
--   timestamp_iso,
--   tresataid__customer,
--   tresataid__customer_a,
--   transaction_date
-- )
-- VALUES
-- (
--   'countrycode_value',
--   'customernumber_value',
--   date__value,
--   'derived_created_dt_value',
--   'duplicate_value',
--   'file_path_value',
--   'hash__value',
--   hr__value,
--   'laboratory_value',
--   'list_nbr_value',
--   'list_sale_sz_value',
--   'message_value',
--   'output_created_dt_value',
--   'parsed_created_dt_value',
--   'pkey_value',
--   'productline_value',
--   'serialnumber_value',
--   'software_version_value',
--   'status_value',
--   'system_id_value',
--   'timestamp_value',
--   timestamp_iso_value,
--   'tresataid__customer_value',
--   'tresataid__customer_a_value',
--   'transaction_date_value'
-- );

-- INSERT INTO dx_phm.phm_aps_pl_sn_mapping
-- (
--   pl_sn_map_id,
--   iom_sn,
--   message,
--   pl,
--   sn,
--   upload_date
-- )
-- VALUES
-- (
--   pl_sn_map_id_value,
--   'iom_sn_value',
--   'message_value',
--   'pl_value',
--   'sn_value',
--   upload_date_value
-- );

-- select
--     ae.countrycode,
--     ae.customernumber,
--     ae.date_,
--     ae.derived_created_dt,
--     ae.duplicate,
--     ae.file_path,
--     ae.hash_,
--     ae.hr_,
--     ae.laboratory,
--     ae.list_nbr,
--     ae.list_sale_sz,
--     ae.message,
--     ae.output_created_dt,
--     ae.parsed_created_dt,
--     ae.pkey,
--     ae.productline,
--     ae.serialnumber,
--     ae.software_version,
--     ae.status,
--     ae.system_id,
--     ae.TIMESTAMP,
--     ae.timestamp_iso,
--     ae.tresataid__customer,
--     ae.tresataid__customer_a,
--     ae.transaction_date,
--     am.pl_sn_map_id,
--     am.iom_sn,
--     am.message,
--     am.pl,
--     am.sn,
--     ae2.message
-- from 
--     dx.dx_aps_error ae
-- inner join
--     dx_phm.phm_aps_pl_sn_mapping am
-- on
--     ae.serialnumber = am.iom_sn
-- and (
--     (am.message like 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (am.message like 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (am.message like 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (am.message like 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (am.message like 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (am.message like 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (am.message like 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (am.message like 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) 
--     )
-- inner join
--     dx.dx_aps_error ae2
-- on
--     ae2.timestamp = ae.timestamp
-- and
--     ae2.message like 'Interface Module Unreadable Barcode -%' 
-- and 
--     '2019-07-01' <= ae2.transaction_date
-- and
--     ae2.transaction_date < '2019-07-02'
-- where (
--     (ae.message like 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (ae.message like 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (ae.message like 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (ae.message like 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (ae.message like 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (ae.message like 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (ae.message like 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
--     (ae.message like 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) )
-- and 
--     '2019-07-01' <= ae.transaction_date
-- and
--     ae.transaction_date < '2019-07-02'

select
    max(raw.iom_sn) as iom_sn,
    max(raw.pl) as pl,
    max(raw.sn) as modulesn,
    max(raw.tstamp) as max_tstamp,
    raw.message as message,
    raw.dt as tdate,
    count(*) as las_error_count
from (
    select
        upper(trim(am.iom_sn)) as iom_sn,
        am.pl as pl,
        am.message as message,
        upper(trim(am.sn)) as sn,
        ae.timestamp_iso as tstamp,
        date_trunc('day', ae.timestamp_iso) as dt
    from 
        dx.dx_aps_error ae
    inner join
        dx_phm.phm_aps_pl_sn_mapping am
    on
        upper(trim(ae.serialnumber)) = upper(trim(am.iom_sn))
    and (
        (am.message like 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (am.message like 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (am.message like 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (am.message like 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (am.message like 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (am.message like 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (am.message like 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (am.message like 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) 
        )
    inner join
        dx.dx_aps_error ae2
    on
        ae2.timestamp = ae.timestamp
    and
        ae2.message like 'Interface Module Unreadable Barcode -%' 
    and 
        '2019-07-01' <= ae2.transaction_date
    and
        ae2.transaction_date < '2019-07-02'
    where (
        (ae.message like 'LAS1' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (ae.message like 'LAS2' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (ae.message like 'LAS3' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (ae.message like 'LAS4' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (ae.message like 'LAS5' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (ae.message like 'LAS6' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (ae.message like 'LAS7' || ': Error 205: Unreadable Barcode' || '%' ) OR
        (ae.message like 'LAS8' || ': Error 205: Unreadable Barcode' || '%' ) )
    and 
        '2019-07-01' <= ae.transaction_date
    and
        ae.transaction_date < '2019-07-02'
    ) raw
group by
    raw.message,
    raw.dt
order by 1, 2

