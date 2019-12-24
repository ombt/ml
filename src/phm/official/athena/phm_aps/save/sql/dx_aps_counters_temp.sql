-- INSERT INTO dx.dx_aps_counter
-- (
--   countrycode,
--   customernumber,
--   date_,
--   derived_created_dt,
--   description,
--   duplicate,
--   duration,
--   file_path,
--   hash_,
--   hr_,
--   id,
--   laboratory,
--   list_nbr,
--   list_sale_sz,
--   output_created_dt,
--   parsed_created_dt,
--   pkey,
--   productline,
--   serialnumber,
--   software_version,
--   system_id,
--   TIMESTAMP,
--   timestamp_iso,
--   tresataid__customer,
--   tresataid__customer_a,
--   TYPE,
--   value,
--   transaction_date
-- )
-- VALUES
-- (
--   'countrycode_value',
--   'customernumber_value',
--   date__value,
--   'derived_created_dt_value',
--   'description_value',
--   'duplicate_value',
--   'duration_value',
--   'file_path_value',
--   'hash__value',
--   hr__value,
--   'id_value',
--   'laboratory_value',
--   'list_nbr_value',
--   'list_sale_sz_value',
--   'output_created_dt_value',
--   'parsed_created_dt_value',
--   'pkey_value',
--   'productline_value',
--   'serialnumber_value',
--   'software_version_value',
--   'system_id_value',
--   'timestamp_value',
--   timestamp_iso_value,
--   'tresataid__customer_value',
--   'tresataid__customer_a_value',
--   'type_value',
--   value_value,
--   'transaction_date_value'
-- );

-- 
-- 
--         V_INSERT_COUNT := 0;
--         V_UPDATE_COUNT := 0;
--         V_TOTAL_COUNT := 0;
--        
--         FOR X IN CURAPS_DEVICES_DATES
--          LOOP
--         -- DBMS_OUTPUT.PUT_LINE('SN: ' || X.SN || ', X.DT: ' || X.DT || ', DT_MAX: ' || X.DT_MAX);
--             V_TOTAL_COUNT := V_TOTAL_COUNT + 1;
--             FOR Y IN (
--                 SELECT 
--                     DEVICE_ID,
--                     TRUNC(TIMESTAMP) DT, 
--                     SN, 
--                     DURATION, 
--                     DESCRIPTION, 
--                     ID, 
--                     MAX(VALUE) MAX_VALUE, 
--                     MIN(VALUE) MIN_VALUE
--                 FROM 
--                     APS_COUNTERS 
--                 WHERE 
--                     SN = X.SN 
--                 AND 
--                     TRUNC(TIMESTAMP) BETWEEN 
--                         X.DT 
--                     AND 
--                         X.DT_MAX + 1 -- TRUNC(TIMESTAMP) BETWEEN X.DT AND X.DT_MAX
--                 AND 
--                     ID IN ('normal','priority','tubes',
--                            '1','2','3','4','5','6','7','8')  
--                 AND 
--                     DURATION IN ('YTD')
--                 AND 
--                     DESCRIPTION IN ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
--                 GROUP BY 
--                     DEVICE_ID, 
--                     TRUNC(TIMESTAMP), 
--                     SN, 
--                     DURATION, 
--                     DESCRIPTION, 
--                     ID )
--             LOOP
--                 SELECT 
--                     COUNT(1) INTO V_COUNTERS_COUNT 
--                 FROM 
--                     PHM_APS_COUNTERS_TEMP
--                 WHERE 
--                     ID = Y.ID 
--                 AND 
--                     DURATION = Y.DURATION  
--                 AND 
--                     DESCRIPTION = Y.DESCRIPTION 
--                 AND 
--                     SN = Y.SN 
--                 AND 
--                     TIMESTAMP = Y.DT;
-- 
--                 -- DBMS_OUTPUT.PUT_LINE('Y.SN: ' || Y.SN || ', Y.DURATION: ' || Y.DURATION || ', Y.DT: ' || Y.DT || ', Y.ID: ' || Y.ID || ', Y.DESCRIPTION: ' || Y.DESCRIPTION || ', V_COUNTERS_COUNT: ' || V_COUNTERS_COUNT);
-- 
--                 IF V_COUNTERS_COUNT > 0 
--                 THEN
--                     UPDATE 
--                         PHM_APS_COUNTERS_TEMP 
--                     SET 
--                         MAX_VALUE = Y.MAX_VALUE, 
--                         MIN_VALUE = Y.MIN_VALUE 
--                     WHERE 
--                         ID = Y.ID 
--                     AND 
--                         DURATION = Y.DURATION  
--                     AND 
--                         DESCRIPTION = Y.DESCRIPTION 
--                     AND 
--                         SN = Y.SN 
--                     AND 
--                         TIMESTAMP = Y.DT;
--                     V_UPDATE_COUNT := V_UPDATE_COUNT + 1;
--                 ELSE
--                     INSERT INTO PHM_APS_COUNTERS_TEMP 
--                     (
--                         DEVICE_ID, 
--                         SN , 
--                         TIMESTAMP, 
--                         ID, 
--                         DURATION, 
--                         DESCRIPTION, 
--                         MIN_VALUE, 
--                         MAX_VALUE
--                     ) 
--                     VALUES
--                     (
--                         Y.DEVICE_ID, 
--                         Y.SN, 
--                         Y.DT, 
--                         Y.ID, 
--                         Y.DURATION, 
--                         Y.DESCRIPTION, 
--                         Y.MIN_VALUE, 
--                         Y.MAX_VALUE
--                     );
--                     V_INSERT_COUNT := V_INSERT_COUNT + 1;
--                 END IF;
--             END LOOP;
--         END LOOP;
-- 

-- INSERT INTO dx.dx_aps_counter
-- (
--   countrycode,
--   customernumber,
--   date_,
--   derived_created_dt,
--   description,
--   duplicate,
--   duration,
--   file_path,
--   hash_,
--   hr_,
--   id,
--   laboratory,
--   list_nbr,
--   list_sale_sz,
--   output_created_dt,
--   parsed_created_dt,
--   pkey,
--   productline,
--   serialnumber,
--   software_version,
--   system_id,
--   TIMESTAMP,
--   timestamp_iso,
--   tresataid__customer,
--   tresataid__customer_a,
--   TYPE,
--   value,
--   transaction_date

with rawdata as (
select 
    date_trunc('day', ac.timestamp_iso) as dt, 
    upper(trim(ac.serialnumber)) as iom_sn, 
    ac.duration, 
    ac.description, 
    ac.id, 
    max(ac.value) as max_value, 
    min(ac.value) as min_value,
    count(*) as rec_count
from 
    dx.dx_aps_counter ac
inner join (
    select
        upper(trim(ae.serialnumber)) as iom_sn,
        min(date_trunc('day', ae.timestamp_iso)) as dt,
        max(ae.timestamp_iso) as dt_max
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
) x
on 
    upper(trim(ac.serialnumber)) = x.iom_sn 
and 
    x.dt <= date_trunc('day', ac.timestamp_iso)
and
    date_trunc('day', ac.timestamp_iso) < (x.dt_max + interval '1' day)
where 
   ac.id in ('normal','priority','tubes', '1','2','3','4','5','6','7','8')  
and 
   ac.duration in ('YTD')
and 
   ac.description in ('InputTubeCounter','CentrifugeCounter','InstrumentBufferCounter')
group by 
    date_trunc('day', ac.timestamp_iso),
    upper(trim(ac.serialnumber)),
    ac.duration, 
    ac.description, 
    ac.id
)
select
    *
from
    rawdata

