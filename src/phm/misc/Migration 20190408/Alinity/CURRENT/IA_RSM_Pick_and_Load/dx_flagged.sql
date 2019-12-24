-- 
-- SELECT activity,
--        blacklisted,
--        component,
--        country,
--        customernumber,
--        date_,
--        datetimestamp,
--        datetimestamplocal,
--        datetimestamputc,
--        derived_created_dt,
--        deviceid,
--        duplicate,
--        file_path,
--        hr_,
--        installdate,
--        installdatelocal,
--        installdateutc,
--        instrumentid,
--        inuse,
--        list_nbr,
--        list_sale_sz,
--        module,
--        moduleid,
--        moduleidname,
--        modulename,
--        moduleserialnumber,
--        modulesoftwareversion,
--        moduletype,
--        moduletypename,
--        output_created_dt,
--        parse_path,
--        parsed_created_dt,
--        pkey,
--        platformtype,
--        platformtypename,
--        primarykey,
--        productline,
--        scmserialnumber,
--        systemsoftwareversion,
--        tresataid__customer,
--        tresataid__customer_a,
--        url,
--        transaction_date
-- 
-- FROM dx.dx_214_alinity_ci_instrumentactivity;
-- 
--    SELECT *
--      BULK COLLECT INTO FLG_TBL
--      FROM (SELECT DEVICEID, Instrument
-- FROM
--        (SELECT DEVICEID, Instrument,
--               (CASE WHEN Num_Recover > 0 THEN Num_Recover / Num_RSM_Move
--                      ELSE 0 END) AS Frac_Recover,
--               (CASE WHEN Num_Engage  > 0 THEN Num_Engage  / Num_RSM_Move
--                      ELSE 0 END) AS Frac_Engage,
--               (CASE WHEN Num_Recover > 0 THEN Num_Recover / Num_Days
--                      ELSE 0 END) AS PerDay_Recover,
--               (CASE WHEN Num_Engage  > 0 THEN Num_Engage  / Num_Days
--                      ELSE 0 END) AS PerDay_Engage
--        FROM
--               (SELECT DEVICEID, Instrument, COUNT(Day) AS Num_Days,
--                      SUM(Num_Retry - 2*Num_Exceed) AS Num_Recover,
--                      SUM(Num_Engage) AS Num_Engage,
--                      SUM(Num_Scans + Num_Retry - Num_Exceed) AS Num_RSM_Move
--               FROM
--                      (SELECT TRUNC(LOGDATE_LOCAL) AS Day, DEVICEID, SYSTEMSN AS Instrument,
--                            SUM(CASE WHEN COMPONENT = 'CarrierScheduler: CarrierScanned'
--                                   THEN 1 ELSE 0 END) AS Num_Scans,
--                            SUM(CASE WHEN COMPONENT LIKE '%Load%Pick%' AND ACTIVITY LIKE 'Retry%'
--                                   THEN 1 ELSE 0 END) AS Num_Retry,
--                            SUM(CASE WHEN COMPONENT LIKE '%Load%Pick%' AND ACTIVITY LIKE 'Exceed%'
--                                   THEN 1 ELSE 0 END) AS Num_Exceed,
--                            SUM(CASE WHEN COMPONENT LIKE '%Load%Pick%' AND ACTIVITY LIKE '%engagement%'
--                                   THEN 1 ELSE 0 END) AS Num_Engage
--                      FROM SVC_PHM_ODS.PHM_ODS_CI_SCM_INSTACTIVITY --IDAQOWNER.ICQ_INSTRUMENTACTIVITY
--                      WHERE TRUNC(LOGDATE_LOCAL) >= TRUNC(SYSDATE) - 7 AND
--                            TRUNC(LOGDATE_LOCAL) <= TRUNC(SYSDATE) - 1 AND
--                            SYSTEMSN LIKE 'SCM%'
--                      GROUP BY  TRUNC(LOGDATE_LOCAL), DEVICEID, SYSTEMSN
--                      )
--               GROUP BY  DEVICEID, Instrument
--               )
--        )
-- 	WHERE 2.3 * Frac_Recover + 2.6 * Frac_Engage + 0.68 * PerDay_Recover + 0.85 * PerDay_Engage >= 3.97
-- 	ORDER BY DEVICEID, Instrument
-- 
-- 	  );

select
    deviceid, 
    instrument,
    flag_date,
    frac_recover,
    frac_engage,
    perday_recover,
    perday_engage
from (
    select 
        deviceid, 
        instrument,
        flag_date,
        case when num_recover > 0 
             then num_recover / num_rsm_move
             else 0 
             end as frac_recover,
        case when num_engage > 0 
             then num_engage / num_rsm_move
             else 0 
             end as frac_engage,
        case when num_recover > 0 
             then num_recover / num_days
             else 0 
             end as perday_recover,
        case when num_engage > 0 
             then num_engage / num_days
             else 0 
             end as perday_engage
    from (
        select 
            deviceid, 
            instrument, 
            min(flag_date) as flag_date,
            count(day) as num_days,
            sum(num_retry - 2*num_exceed) as num_recover,
            sum(num_engage) as num_engage,
            sum(num_scans + num_retry - num_exceed) as num_rsm_move
        from (
            select 
                date_trunc('day', ia.datetimestamplocal) as day,
                ia.deviceid,
                ia.scmserialnumber as instrument,
                min(ia.datetimestamplocal) as flag_date,
                sum(case when component = 'CarrierScheduler: CarrierScanned'
                         then 1 
                         else 0 
                         end) as num_scans,
                sum(case when component like '%Load%Pick%' and activity like 'Retry%'
                         then 1 
                         else 0 
                         end) as num_retry,
                sum(case when component like '%Load%Pick%' and activity like 'Exceed%'
                         then 1 
                         else 0 
                         end) as num_exceed,
                sum(case when component like '%Load%Pick%' and activity like '%engagement%'
                         then 1 
                         else 0 
                         end) as num_engage
            from 
                dx.dx_214_alinity_ci_instrumentactivity ia
            where 
                ia.scmserialnumber like 'SCM%'
            and
                '2019-10-01' <= ia.transaction_date
            and
                ia.transaction_date < '2019-10-08'
            group by
                date_trunc('day', ia.datetimestamplocal),
                ia.deviceid,
                ia.scmserialnumber
        )
        group by
            deviceid, 
            instrument
    )
)
where 
    (2.3 * frac_recover + 
     2.6 * frac_engage + 
     0.68 * perday_recover + 
     0.85 * perday_engage) >= 3.97
order by 
    deviceid, 
    instrument
