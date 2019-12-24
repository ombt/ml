-- 
-- with all_thresholds_cte as (
-- select
--     th.phm_thresholds_sk,
--     th.threshold_number_unit,
--     th.threshold_number_desc,
--     th.phm_patterns_sk,
--     th.thresholds_sk_val,
--     th.pattern_description,
--     th.threshold_alert,
--     th.algorithm_type,
--     th.threshold_data_days,
--     th.module_type,
--     case when ((th.pattern_description = '0405') and 
--                (th.module_type = 'IOM')) or
--               ((th.pattern_description = '0605') and 
--                (th.module_type = 'CM'))
--          then
--              NULL
--          when (th.pattern_description = '5015') AND 
--               (th.module_type = 'ISR')
--          then
--              NULL
--          else
--              ' '
--          end as v_samp_id_chk1,
--     case when ((th.pattern_description = '0405') and 
--                (th.module_type = 'IOM')) or
--               ((th.pattern_description = '0605') and 
--                (th.module_type = 'CM'))
--          then
--              'amp;U__'
--          when (th.pattern_description = '5015') AND 
--               (th.module_type = 'ISR')
--          then
--              '%'
--          else
--              '%'
--          end as v_samp_id_chk2
-- from (
--     select 
--         phm_patterns_sk as phm_thresholds_sk,
--         case when threshold_number is null 
--              then 9999
--              else to_number (threshold_number)
--              end as threshold_number,
--         case when threshold_number_unit is null 
--              then 9999
--              else to_number (threshold_number_unit)
--              end as threshold_number_unit,
--         threshold_number_desc,
--         phm_patterns_sk as phm_patterns_sk,
--         phm_thresholds_sk as thresholds_sk_val,
--         pattern_description,
--         threshold_alert,
--         algorithm_type as algorithm_type,
--         case when threshold_data_days is null 
--              then 9999
--              else to_number (threshold_data_days)
--              end as threshold_data_days,
--         case when module_type = 'ALL' 
--              then '%' 
--              else module_type 
--              end as module_type
--     from (
--         select 
--             tp.phm_patterns_sk,
--             p.pattern_name as pattern_name,
--             thr.phm_thresholds_sk,
--             ihn.issue_description,
--             tp.parameter_name,
--             tp.parameter_values
--         from 
--             phm_threshold_parameter tp,
--             phm_patterns p,
--             (
--                 select 
--                     phm_patterns_sk, 
--                     issue_description
--                 from 
--                     phm_algorithm_ihns pai
--                 where 
--                     -- pai.phm_algorithm_definitions_sk = vn_alg_num
--                     pai.phm_algorithm_definitions_sk = 1040
--             ) ihn,
--             (
--                 select 
--                     phm_patterns_sk, 
--                     phm_thresholds_sk
--                 from 
--                     phm_thresholds pt
--                 where 
--                     -- pt.phm_algorithm_definitions_sk = vn_alg_num
--                     pt.phm_algorithm_definitions_sk = 1040
--             ) thr
--         where
--             tp.phm_patterns_sk = p.phm_patterns_sk
--         and 
--             nvl(tp.delete_flag, 'N') <> 'Y'
--         and 
--             -- tp.phm_d_algorithm_definitions_sk = vn_alg_num
--             tp.phm_d_algorithm_definitions_sk = 1040
--         and 
--             p.phm_patterns_sk = ihn.phm_patterns_sk
--         and 
--             p.phm_patterns_sk = thr.phm_patterns_sk
--     ) 
--     pivot (
--         max(parameter_values) for parameter_name
--         in (
--             'ALGORITHM_TYPE'        as algorithm_type,
--             'ERROR_CODE_VALUE'      as pattern_description,
--             'ERROR_COUNT'           as threshold_number,
--             'IHN_LEVEL3_DESC'       as threshold_alert,
--             'THRESHOLD_DATA_DAYS'   as threshold_data_days,
--             'THRESHOLDS_DAYS'       as threshold_number_unit,
--             'THRESHOLD_DESCRIPTION' as threshold_number_desc,
--             'MODULE'                as module_type
--         )
--     )
--     where
--         -- phm_patterns_sk in ( 105, 110 )
--         phm_patterns_sk in ( 105 )
--     order by 
--         algorithm_type, 
--         pattern_name
--     ) th
-- ),
-- device_and_dates_cte as (
-- select 
--     -- ae.batch_num,
--     asi.productlineref,
--     asi.deviceid,
--     asi.systemsn,
--     aln.sn,
--     ae.nodetype,
--     ae.errorcode,
--     max (ae.completiondate) as max_compl_date,
--     trunc (min (ae.completiondate)) as min_compl_date
-- from 
--     svc_phm_ods.phm_ods_a3600_errors ae
-- inner join
--     idaowner.a3600systeminformation asi
-- on
--     asi.current_row = 'Y'
-- inner join
--     a3600_layout_nodes_pl_sn aln
-- on
--     ae.layout_nodes_id = aln.layout_nodes_id
-- and 
--     aln.systeminfoid = asi.systeminfoid
-- and 
--     aln.sn is not null
-- and 
--     aln.canid = ae.nodeid
-- inner join
--     all_thresholds_cte ath
-- on
--     ae.errorcode = ath.pattern_description
-- and 
--     (((ath.module_type != '%') and 
--       (ae.nodetype = ath.module_type)) or 
--      ((ath.module_type = '%') and 
--       (ae.nodetype like ath.module_type)))
-- where
--     -- ae.batch_num = v_batch_num
--     -- ae.batch_num = 'BTH1000'
-- -- and 
--     -- ae.run_date = v_run_date
--     ae.run_date = trunc(sysdate - 1)
-- group by 
--     -- ae.batch_num,
--     asi.productlineref,
--     asi.deviceid,
--     asi.systemsn,
--     aln.sn,
--     ae.nodetype,
--     ae.errorcode
-- order by 
--     -- ae.batch_num,
--     asi.systemsn, 
--     ae.nodetype, 
--     ae.errorcode
-- ),
-- threshold_counts_cte as (
-- select 
--     th.phm_thresholds_sk,
--     asi.deviceid,
--     asi.systemsn,
--     n.pl,
--     n.sn,
--     trunc(ae.completiondate) as flag_date,
--     ae.nodetype,
--     ae.errorcode,
--     ae.nodeid,
--     ae.instanceid,
--     ac.tubes_today,
--     max (ae.completiondate) as max_compl_date,
--     count (ae.errorcode) as error_count,
--     trunc ( (count (ae.errorcode) * 100 / ac.tubes_today), 2) as error_percentage
-- from 
--     svc_phm_ods.phm_ods_a3600_errors ae
-- inner join
--     all_thresholds_cte th
-- on
--     ((th.module_type != '%' and ae.nodetype = th.module_type) or 
--      (th.module_type = '%' and ae.nodetype like th.module_type))
-- and 
--     nvl(ae.sampleid, th.v_samp_id_chk1) like nvl (th.v_samp_id_chk2, ae.sampleid)
-- and
--     ae.errorcode = th.pattern_description
-- inner join
--     device_and_dates_cte dd
-- on
--     dd.errorcode = th.pattern_description
-- and 
--     ((th.module_type != '%' and dd.nodetype = th.module_type) or 
--      (th.module_type = '%' and dd.nodetype like th.module_type))
-- and
--     ae.completiondate between 
--         dd.min_compl_date - th.threshold_number_unit + 1
--     and 
--         dd.max_compl_date
-- inner join
--     a3600_layout_nodes_pl_sn n
-- on
--     ae.layout_nodes_id = n.layout_nodes_id
-- and 
--     n.canid = ae.nodeid
-- and 
--     n.nodetype = ae.nodetype
-- and 
--     lower (n.sn) = lower (dd.sn)
-- inner join
--     idaowner.a3600systeminformation asi
-- on
--     asi.systeminfoid = n.systeminfoid
-- and 
--     n.systeminfoid = asi.systeminfoid
-- and 
--     asi.current_row = 'Y'
-- inner join
--     idaowner.a3600_counters ac
-- on
--     ac.layout_nodes_id = n.layout_nodes_id
-- and 
--     ac.nodetype = ae.nodetype
-- and 
--     ac.counter_date = trunc (ae.completiondate)
-- and 
--     ac.nodeid = ae.nodeid
-- and 
--     ac.instanceid = ae.instanceid
-- and 
--     ac.tubes_today <> 0
-- group by 
--     th.phm_thresholds_sk,
--     asi.deviceid,
--     asi.systemsn,
--     n.pl,
--     n.sn,
--     trunc(ae.completiondate),
--     ae.nodetype,
--     ae.errorcode,
--     ae.nodeid,
--     ae.instanceid,
--     ac.tubes_today
-- order by 
--     n.pl,
--     n.sn,
--     trunc(ae.completiondate)
-- ),
-- phm_a3600_temp_error_cte as (
-- select
--     trunc(sysdate - 1)   as run_date,
--     tc.phm_thresholds_sk as phm_thresholds_sk,
--     tc.deviceid          as device_id,
--     tc.sn                as module_sn,
--     tc.nodetype          as nodetype,
--     tc.errorcode         as errorcode,
--     tc.nodeid            as nodeid,
--     tc.instanceid        as instanceid,
--     tc.flag_date         as flag_date,
--     tc.tubes_today       as testcount,
--     tc.error_count       as errorcount,
--     tc.error_percentage  as errorpct,
--     tc.max_compl_date    as max_date_value,
--     0                    as completion_dt_ms,
--     sysdate              as date_created,
--     tc.pl                as pl
-- from
--     threshold_counts_cte tc
-- order by
--     tc.pl,
--     tc.sn,
--     tc.max_compl_date
-- )
-- select * from phm_a3600_temp_error_cte
-- 

-- get_data_query <- function(start_date,
--                            end_date,
--                            v_errorcode1, 
--                            v_nodetype1,
--                            v_samp_id_chk1, 
--                            v_samp_id_chk2, 
--                            v_data_days)
-- {
--     print(sprintf("Get Data Query for (%s,%s,%s)", start_date, end_date, v_data_days))
--     #
--     query <- "
-- select 
--     ae2.a3600_deviceid,
--     upper(trim(ae2.a3600_iom_serial)) as a3600_iom_serial_uc,
--     ae2.a3600_productline,
--     upper(trim(ae2.a3600_serialnumber)) as a3600_serialnumber_uc,
--     date_trunc('day', ae2.timestamp_iso) as flag_date,
--     ae2.a3600_nodetype,
--     ae2.errorcode,
--     ae2.a3600_nodeid,
--     ae2.a3600_layoutinstance,
--     ac.tubestoday,
--     max(ae2.timestamp_iso) as max_compl_date,
--     count(ae2.errorcode) as error_count,
--     (count(ae2.errorcode) * 100.0 / ac.tubestoday) as error_percentage
-- from 
--     dx.dx_a3600_error ae2,
--     dx.dx_a3600_counter ac,
--     (
--     select 
--         ae.a3600_productline,
--         ae.a3600_deviceid,
--         upper(trim(ae.a3600_iom_serial)) as a3600_iom_serial_uc,
--         upper(trim(ae.a3600_serialnumber)) as a3600_serialnumber,
--         ae.a3600_nodetype,
--         ae.errorcode,
--         max (ae.timestamp_iso) as max_compl_date,
--         date_trunc('day', min(ae.timestamp_iso)) as min_compl_date
--     from 
--         dx.dx_a3600_error ae
--     where
--         '<START_DATE>' <= ae.transaction_date
--     and
--         ae.transaction_date < '<END_DATE>'
--     and 
--         ae.errorcode = '<V_ERRORCODE1>'
--     and 
--         (('<V_NODETYPE1>' != '%' and ae.a3600_nodetype = '<V_NODETYPE1>') or
--          ('<V_NODETYPE1>' = '%' and ae.a3600_nodetype like '<V_NODETYPE1>'))
--     group by 
--         ae.a3600_productline,
--         ae.a3600_deviceid,
--         ae.a3600_iom_serial,
--         ae.a3600_serialnumber,
--         ae.a3600_nodetype,
--         ae.errorcode
--     order by 
--         upper(trim(ae.a3600_iom_serial)),
--         ae.a3600_nodetype,
--         ae.errorcode
--     ) dd
-- where
--     upper(trim(ae2.a3600_serialnumber)) = upper(trim(dd.a3600_serialnumber))
-- and
--     upper(trim(ac.a3600_serialnumber)) = upper(trim(dd.a3600_serialnumber))
-- and
--     ac.a3600_nodetype = ae2.a3600_nodetype
-- and
--     ac.counter_date = date_trunc('day', ae2.timestamp_iso)
-- and
--     ((ac.tubestoday is not null) and (ac.tubestoday > 0))
-- and
--     ac.a3600_nodeid = ae2.a3600_nodeid
-- and
--     ac.a3600_layoutinstance = ae2.a3600_layoutinstance
-- and
--     (('<V_NODETYPE1>' != '%' and ae2.nodetype = '<V_NODETYPE1>')
-- or
--     ('<V_NODETYPE1>' = '%' and ae2.nodetype like '<V_NODETYPE1>'))
-- and
--     ae2.errorcode = '<V_ERRORCODE1>'
-- and
--     coalesce(ae2.sampleid, <V_SAMP_ID_CHK1>) like coalesce(<V_SAMP_ID_CHK2>, ae2.sampleid)
-- and
--     ae2.timestamp_iso between 
--         dd.min_compl_date - interval '<V_DATA_DAYS>' day + interval '1' day
--     and
--         dd.max_compl_date
-- group by 
--     ae2.a3600_deviceid,
--     ae2.a3600_iom_serial,
--     ae2.a3600_productline,
--     ae2.a3600_serialnumber,
--     date_trunc('day', ae2.timestamp_iso),
--     ae2.a3600_nodetype,
--     ae2.errorcode,
--     ae2.a3600_nodeid,
--     ae2.a3600_layoutinstance,
--     ac.tubestoday
-- order by
--     upper(trim(ae2.a3600_iom_serial)),
--     ae2.a3600_productline,
--     upper(trim(ae2.a3600_serialnumber)),
--     date_trunc('day', ae2.timestamp_iso)
-- "
--     #
--     query <- gsub('<START_DATE>', 
--                   start_date, 
--                   query, 
--                   fixed=TRUE)
--     query <- gsub('<END_DATE>', 
--                   end_date, 
--                   query, 
--                   fixed=TRUE)
--     query <- gsub('<V_ERRORCODE1>', 
--                   v_errorcode1, 
--                   query, 
--                   fixed=TRUE)
--     query <- gsub('<V_NODETYPE1>', 
--                   v_nodetype1, 
--                   query, 
--                   fixed=TRUE)
--     query <- gsub('<V_SAMP_ID_CHK1>',
--                   v_samp_id_chk1, 
--                   query, 
--                   fixed=TRUE)
--     query <- gsub('<V_SAMP_ID_CHK2>',
--                   v_samp_id_chk2, 
--                   query, 
--                   fixed=TRUE)
--     query <- gsub('<V_DATA_DAYS>',
--                   v_data_days, 
--                   query, 
--                   fixed=TRUE)
--     #
--     return(query)
-- }


-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,MODULE,IOM
-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,ERROR_CODE_VALUE,0405
-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,IHN_LEVEL3_DESC,Unreadable Sample ID or Unreadable Rack ID IOM 0405_1
-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,ALGORITHM_TYPE,SD_HIGH_VOLUME
-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,ERROR_COUNT,1.5
-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,THRESHOLD_DESCRIPTION,1 SD above 30 day mean for 2 consecutive days (high volume)
-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,THRESHOLDS_DAYS,2
-- Unreadable Sample ID or Unreadable Rack ID IOM 0405_1,105,THRESHOLD_DATA_DAYS,30

--             'ALGORITHM_TYPE'        as algorithm_type,
--             'ERROR_CODE_VALUE'      as pattern_description,
--             'ERROR_COUNT'           as threshold_number,
--             'IHN_LEVEL3_DESC'       as threshold_alert,
--             'THRESHOLD_DATA_DAYS'   as threshold_data_days,
--             'THRESHOLDS_DAYS'       as threshold_number_unit,
--             'THRESHOLD_DESCRIPTION' as threshold_number_desc,
--             'MODULE'                as module_type

-- with all_thresholds_cte as (
-- select
--     th.phm_thresholds_sk,
--     th.threshold_number_unit,
--     th.threshold_number_desc,
--     th.phm_patterns_sk,
--     th.thresholds_sk_val,
--     th.pattern_description,
--     th.threshold_alert,
--     th.algorithm_type,
--     th.threshold_data_days,
--     th.module_type,
--     case when ((th.pattern_description = '0405') and 
--                (th.module_type = 'IOM')) or
--               ((th.pattern_description = '0605') and 
--                (th.module_type = 'CM'))
--          then
--              NULL
--          when (th.pattern_description = '5015') AND 
--               (th.module_type = 'ISR')
--          then
--              NULL
--          else
--              ' '
--          end as v_samp_id_chk1,
--     case when ((th.pattern_description = '0405') and 
--                (th.module_type = 'IOM')) or
--               ((th.pattern_description = '0605') and 
--                (th.module_type = 'CM'))
--          then
--              'amp;U__'
--          when (th.pattern_description = '5015') AND 
--               (th.module_type = 'ISR')
--          then
--              '%'
--          else
--              '%'
--          end as v_samp_id_chk2

--             'ALGORITHM_TYPE'        as algorithm_type,
--             'ERROR_CODE_VALUE'      as pattern_description,
--             'ERROR_COUNT'           as threshold_number,
--             'IHN_LEVEL3_DESC'       as threshold_alert,
--             'THRESHOLD_DATA_DAYS'   as threshold_data_days,
--             'THRESHOLDS_DAYS'       as threshold_number_unit,
--             'THRESHOLD_DESCRIPTION' as threshold_number_desc,
--             'MODULE'                as module_type

with all_thresholds_cte as (
select
    th.phm_thresholds_sk,
    th.threshold_number_unit,
    th.threshold_number_desc,
    th.phm_patterns_sk,
    th.thresholds_sk_val,
    th.pattern_description,
    th.threshold_alert,
    th.algorithm_type,
    th.threshold_data_days,
    th.module_type,
    case when ((th.pattern_description = '0405') and 
               (th.module_type = 'IOM')) or
              ((th.pattern_description = '0605') and 
               (th.module_type = 'CM'))
         then
             NULL
         when (th.pattern_description = '5015') AND 
              (th.module_type = 'ISR')
         then
             NULL
         else
             ' '
         end as v_samp_id_chk1,
    case when ((th.pattern_description = '0405') and 
               (th.module_type = 'IOM')) or
              ((th.pattern_description = '0605') and 
               (th.module_type = 'CM'))
         then
             '&U__'
         when (th.pattern_description = '5015') AND 
              (th.module_type = 'ISR')
         then
             '%'
         else
             '%'
         end as v_samp_id_chk2
from (
    select 
        phm_patterns_sk as phm_thresholds_sk,
        phm_patterns_sk as thresholds_sk_val,
        case when threshold_number is null 
             then 9999
             else cast (threshold_number as integer)
             end as threshold_number,
        case when threshold_number_unit is null 
             then 9999
             else cast (threshold_number_unit as integer)
             end as threshold_number_unit,
        threshold_number_desc,
        phm_patterns_sk as phm_patterns_sk,
        pattern_description,
        threshold_alert,
        algorithm_type as algorithm_type,
        case when threshold_data_days is null 
             then 9999
             else cast (threshold_data_days as integer)
             end as threshold_data_days,
        case when module_type = 'ALL' 
             then '%' 
             else module_type 
             end as module_type
    from (
        select
            ALGORITHM_NAME        as algorithm_name,
            PHM_PATTERNS_SK       as phm_patterns_sk,
            ALGORITHM_TYPE        as algorithm_type,
            ERROR_CODE_VALUE      as pattern_description,
            ERROR_COUNT           as threshold_number,
            IHN_LEVEL3_DESC       as threshold_alert,
            THRESHOLD_DATA_DAYS   as threshold_data_days,
            THRESHOLDS_DAYS       as threshold_number_unit,
            THRESHOLD_DESCRIPTION as threshold_number_desc,
            MODULE                as module_type
        from (
            select
                'Unreadable Sample ID or Unreadable Rack ID IOM 0405_1'       as ALGORITHM_NAME,
                 105                                                          as PHM_PATTERNS_SK,
                'IOM'                                                         as MODULE,
                '0405'                                                        as ERROR_CODE_VALUE,
                'Unreadable Sample ID or Unreadable Rack ID IOM 0405_1'       as IHN_LEVEL3_DESC,
                'SD_HIGH_VOLUME'                                              as ALGORITHM_TYPE,
                 1.5                                                          as ERROR_COUNT,
                '1 SD above 30 day mean for 2 consecutive days (high volume)' as THRESHOLD_DESCRIPTION,
                 2                                                            as THRESHOLDS_DAYS,
                 30                                                           as THRESHOLD_DATA_DAYS
            ) input_csv
        ) relabel
    ) th
)
select * from all_thresholds_cte

