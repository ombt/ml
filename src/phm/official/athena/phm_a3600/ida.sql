
--    CURSOR ALL_THRESHOLDS (
--       vn_ALG_NUM    NUMBER)
--    IS

--    CURSOR DEVICE_AND_DATES (
--       V_NODETYPE1     VARCHAR2,
--       V_ERRORCODE1    VARCHAR2)
--    IS

--    CURSOR THRESHOLD_COUNTS (
--       V_SN1             VARCHAR2,
--       V_NODETYPE1       VARCHAR2,
--       V_ERRORCODE1      VARCHAR2,
--       V_START_DATE      DATE,
--       V_END_DATE        DATE,
--       V_DATA_DAYS       NUMBER,
--       V_SAMP_ID_CHK1    VARCHAR2,
--       V_SAMP_ID_CHK2    VARCHAR2)
--    IS

-- FOR TE IN THRESHOLD_COUNTS (DD.SN,                    --       V_SN1             VARCHAR2,
--                             TH.MODULE_TYPE,           --       V_NODETYPE1       VARCHAR2,
--                             TH.PATTERN_DESCRIPTION,   --       V_ERRORCODE1      VARCHAR2,
--                             DD.MIN_COMPL_DATE,        --       V_START_DATE      DATE,
--                             DD.MAX_COMPL_DATE,        --       V_END_DATE        DATE,
--                             TH.THRESHOLD_NUMBER_UNIT, --       V_DATA_DAYS       NUMBER,
--                             V_SAMP_ID_CHK1,           --       V_SAMP_ID_CHK1    VARCHAR2,
--                             V_SAMP_ID_CHK2)           --       V_SAMP_ID_CHK2    VARCHAR2)

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
             'amp;U__'
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
        case when threshold_number is null 
             then 9999
             else to_number (threshold_number)
             end as threshold_number,
        case when threshold_number_unit is null 
             then 9999
             else to_number (threshold_number_unit)
             end as threshold_number_unit,
        threshold_number_desc,
        phm_patterns_sk as phm_patterns_sk,
        phm_thresholds_sk as thresholds_sk_val,
        pattern_description,
        threshold_alert,
        algorithm_type as algorithm_type,
        case when threshold_data_days is null 
             then 9999
             else to_number (threshold_data_days)
             end as threshold_data_days,
        case when module_type = 'ALL' 
             then '%' 
             else module_type 
             end as module_type
    from (
        select 
            tp.phm_patterns_sk,
            p.pattern_name as pattern_name,
            thr.phm_thresholds_sk,
            ihn.issue_description,
            tp.parameter_name,
            tp.parameter_values
        from 
            phm_threshold_parameter tp,
            phm_patterns p,
            (
                select 
                    phm_patterns_sk, 
                    issue_description
                from 
                    phm_algorithm_ihns pai
                where 
                    -- pai.phm_algorithm_definitions_sk = vn_alg_num
                    pai.phm_algorithm_definitions_sk = 1040
            ) ihn,
            (
                select 
                    phm_patterns_sk, 
                    phm_thresholds_sk
                from 
                    phm_thresholds pt
                where 
                    -- pt.phm_algorithm_definitions_sk = vn_alg_num
                    pt.phm_algorithm_definitions_sk = 1040
            ) thr
        where
            tp.phm_patterns_sk = p.phm_patterns_sk
        and 
            nvl(tp.delete_flag, 'N') <> 'Y'
        and 
            -- tp.phm_d_algorithm_definitions_sk = vn_alg_num
            tp.phm_d_algorithm_definitions_sk = 1040
        and 
            p.phm_patterns_sk = ihn.phm_patterns_sk
        and 
            p.phm_patterns_sk = thr.phm_patterns_sk
    ) 
    pivot (
        max(parameter_values) for parameter_name
        in (
            'ALGORITHM_TYPE'        as algorithm_type,
            'ERROR_CODE_VALUE'      as pattern_description,
            'ERROR_COUNT'           as threshold_number,
            'IHN_LEVEL3_DESC'       as threshold_alert,
            'THRESHOLD_DATA_DAYS'   as threshold_data_days,
            'THRESHOLDS_DAYS'       as threshold_number_unit,
            'THRESHOLD_DESCRIPTION' as threshold_number_desc,
            'MODULE'                as module_type
        )
    )
    where
        -- phm_patterns_sk in ( 105, 110 )
        phm_patterns_sk in ( 105 )
    order by 
        algorithm_type, 
        pattern_name
    ) th
),
device_and_dates_cte as (
select 
    -- ae.batch_num,
    asi.productlineref,
    asi.deviceid,
    asi.systemsn,
    aln.sn,
    ae.nodetype,
    ae.errorcode,
    max (ae.completiondate) as max_compl_date,
    trunc (min (ae.completiondate)) as min_compl_date
from 
    svc_phm_ods.phm_ods_a3600_errors ae
inner join
    idaowner.a3600systeminformation asi
on
    asi.current_row = 'Y'
inner join
    a3600_layout_nodes_pl_sn aln
on
    ae.layout_nodes_id = aln.layout_nodes_id
and 
    aln.systeminfoid = asi.systeminfoid
and 
    aln.sn is not null
and 
    aln.canid = ae.nodeid
inner join
    all_thresholds_cte ath
on
    ae.errorcode = ath.pattern_description
and 
    (((ath.module_type != '%') and 
      (ae.nodetype = ath.module_type)) or 
     ((ath.module_type = '%') and 
      (ae.nodetype like ath.module_type)))
where
    -- ae.batch_num = v_batch_num
    -- ae.batch_num = 'BTH1000'
-- and 
    -- ae.run_date = v_run_date
    ae.run_date = trunc(sysdate - 1)
group by 
    -- ae.batch_num,
    asi.productlineref,
    asi.deviceid,
    asi.systemsn,
    aln.sn,
    ae.nodetype,
    ae.errorcode
order by 
    -- ae.batch_num,
    asi.systemsn, 
    ae.nodetype, 
    ae.errorcode
),
threshold_counts_cte as (
select 
    th.phm_thresholds_sk,
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc(ae.completiondate) as flag_date,
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today,
    max (ae.completiondate) as max_compl_date,
    count (ae.errorcode) as error_count,
    trunc ( (count (ae.errorcode) * 100 / ac.tubes_today), 2) as error_percentage
from 
    svc_phm_ods.phm_ods_a3600_errors ae
inner join
    all_thresholds_cte th
on
    ((th.module_type != '%' and ae.nodetype = th.module_type) or 
     (th.module_type = '%' and ae.nodetype like th.module_type))
and 
    nvl(ae.sampleid, th.v_samp_id_chk1) like nvl (th.v_samp_id_chk2, ae.sampleid)
and
    ae.errorcode = th.pattern_description
inner join
    device_and_dates_cte dd
on
    dd.errorcode = th.pattern_description
and 
    ((th.module_type != '%' and dd.nodetype = th.module_type) or 
     (th.module_type = '%' and dd.nodetype like th.module_type))
and
    ae.completiondate between 
        dd.min_compl_date - th.threshold_number_unit + 1
    and 
        dd.max_compl_date
inner join
    a3600_layout_nodes_pl_sn n
on
    ae.layout_nodes_id = n.layout_nodes_id
and 
    n.canid = ae.nodeid
and 
    n.nodetype = ae.nodetype
and 
    lower (n.sn) = lower (dd.sn)
inner join
    idaowner.a3600systeminformation asi
on
    asi.systeminfoid = n.systeminfoid
and 
    n.systeminfoid = asi.systeminfoid
and 
    asi.current_row = 'Y'
inner join
    idaowner.a3600_counters ac
on
    ac.layout_nodes_id = n.layout_nodes_id
and 
    ac.nodetype = ae.nodetype
and 
    ac.counter_date = trunc (ae.completiondate)
and 
    ac.nodeid = ae.nodeid
and 
    ac.instanceid = ae.instanceid
and 
    ac.tubes_today <> 0
group by 
    th.phm_thresholds_sk,
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc(ae.completiondate),
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today
order by 
    n.pl,
    n.sn,
    trunc(ae.completiondate)
),
phm_a3600_temp_error_cte as (
select
    trunc(sysdate - 1)   as run_date,
    tc.phm_thresholds_sk as phm_thresholds_sk,
    tc.deviceid          as device_id,
    tc.sn                as module_sn,
    tc.nodetype          as nodetype,
    tc.errorcode         as errorcode,
    tc.nodeid            as nodeid,
    tc.instanceid        as instanceid,
    tc.flag_date         as flag_date,
    tc.tubes_today       as testcount,
    tc.error_count       as errorcount,
    tc.error_percentage  as errorpct,
    tc.max_compl_date    as max_date_value,
    0                    as completion_dt_ms,
    sysdate              as date_created,
    tc.pl                as pl
from
    threshold_counts_cte tc
order by
    tc.pl,
    tc.sn,
    tc.max_compl_date
),
v_req_start_date as (
select 
    aln.sn,
    trunc (min (ae.completiondate)) as min_compl_date,
    trunc (min (ae.completiondate)) as v_req_start_date
from 
    svc_phm_ods.phm_ods_a3600_errors ae,
    a3600_layout_nodes_pl_sn aln,
    idaowner.a3600systeminformation asi
where
    -- batch_num = v_batch_num
-- and 
    -- run_date = v_run_date
    run_date = trunc(sysdate - 1)
-- and 
    -- aln.sn = y.module_sn
and 
    ae.layout_nodes_id = aln.layout_nodes_id
and 
    aln.systeminfoid = asi.systeminfoid
and 
    aln.canid = ae.nodeid
and 
    asi.current_row = 'Y'
group by
    aln.sn
)
select * from v_req_start_date 


