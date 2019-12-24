-- 
--     CURSOR CURAPS_ERRORS_BCR1 (VSN VARCHAR, 
--                                V_START_DATE DATE, 
--                                V_END_DATE DATE)
--     IS
--         SELECT 
--             DEVICE_ID, 
--             MAX(PL) PL, 
--             SN, 
--             TRUNC(TIMESTAMP) DT, 
--             COUNT (*) PAT_ERRCOUNT, 
--             MAX(TIMESTAMP) FLG_DATE
--         FROM 
--             SVC_PHM_ODS.PHM_ODS_APS_ERRORS 
--         WHERE 
--             MESSAGE LIKE '%BCR%1%' 
--         AND 
--             SN = VSN
--         AND 
--             TIMESTAMP BETWEEN 
--                 V_START_DATE 
--             AND 
--                 V_END_DATE  -- TRUNC(SD_START_DATE) AND TRUNC(SD_END_DATE)
--         GROUP BY 
--             DEVICE_ID, 
--             SN, 
--             TRUNC(TIMESTAMP)
--         ORDER BY 
--             DEVICE_ID, 
--             SN, 
--             DT;

select
    max(ae.productline) as iom_pl,
    upper(trim(ae.serialnumber)) as iom_sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    max(ae.timestamp_iso) as flag_date,
    count(*) as pat_error_count
from 
    dx.dx_aps_error ae
where 
    ae.message like '%BCR%1%'
and 
    '2019-07-01' <= ae.transaction_date
and
    ae.transaction_date < '2019-07-02'
group by
    upper(trim(ae.serialnumber)),
    date_trunc('day', ae.timestamp_iso)
order by 
    upper(trim(ae.serialnumber)),
    date_trunc('day', ae.timestamp_iso)

