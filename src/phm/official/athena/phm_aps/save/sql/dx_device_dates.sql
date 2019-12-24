-- 
--    CURSOR CURAPS_DEVICES_DATES -- ( STARTDATE DATE, ENDDATE DATE)
--    IS
--        SELECT  
--            SN, 
--            MIN(TRUNC(TIMESTAMP)) DT, 
--            MAX(TIMESTAMP) DT_MAX
--        FROM 
--            SVC_PHM_ODS.PHM_ODS_APS_ERRORS
--        WHERE 
--            RUN_DATE = V_RUN_DATE 
--        AND 
--            BATCH_NUM = V_BATCH_NUM  
--        AND 
--            TRUNC(TIMESTAMP) <> TRUNC(SYSDATE) -- This check is to ignore current day data as the data for current day could be coming next day  because of the way rules were defined in AbbottLink
--        GROUP BY 
--            SN 
--        ORDER BY 1,2;

    select
        upper(trim(ae.serialnumber)) as iom_sn,
        min(date_trunc('day', ae.timestamp_iso)) as dt,
        max(ae.timestamp_iso) as max_dt
    from 
        dx.dx_aps_error ae
    where
        '2019-07-01' <= ae.transaction_date
    and
        ae.transaction_date < '2019-07-02'
    and
        date_trunc('day', ae.timestamp_iso) < date_parse('2019-07-02', '%Y-%m-%d')
group by
    upper(trim(ae.serialnumber))
order by 1, 2

