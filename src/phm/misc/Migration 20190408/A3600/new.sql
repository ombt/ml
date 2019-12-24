-- 
--    CURSOR THRESHOLD_COUNTS (
--       V_SN1             VARCHAR2, DD.SN
--       V_NODETYPE1       VARCHAR2, TH.MODULE_TYPE
--       V_ERRORCODE1      VARCHAR2, TH.PATTERN_DESCRIPTION
--       V_START_DATE      DATE, DD.MIN_COMPL_DATE
--       V_END_DATE        DATE, DD.MAX_COMPL_DATE
--       V_DATA_DAYS       NUMBER, TH.THRESHOLD_NUMBER_UNIT
--       V_SAMP_ID_CHK1    VARCHAR2, V_SAMP_ID_CK1
--       V_SAMP_ID_CHK2    VARCHAR2) V_SAMP_ID_CK2
--    IS
-- 

with params as (
    select
        thresholds.phm_thresholds_sk,
        thresholds.threshold_number,
        thresholds.threshold_number_unit,
        thresholds.threshold_number_desc,
        thresholds.phm_patterns_sk,
        thresholds.thresholds_sk_val,
        thresholds.pattern_description,
        thresholds.threshold_alert,
        thresholds.algorithm_type,
        thresholds.threshold_data_days,
        thresholds.module_type,
        case when ((thresholds.pattern_description = '0405') and 
                   (thresholds.module_type = 'IOM')) or 
                  ((thresholds.pattern_description = '0605') and
                   (thresholds.module_type = 'CM'))
             then
                 null
             when (thresholds.PATTERN_DESCRIPTION = '5015') and 
                  (thresholds.MODULE_TYPE = 'ISR')
             then
                 null
             else
                 ' '
             end as v_samp_id_chk1,
        case when ((thresholds.pattern_description = '0405') and 
                   (thresholds.module_type = 'IOM')) or 
                  ((thresholds.pattern_description = '0605') and
                   (thresholds.module_type = 'CM'))
             then
                 'amp;U__'
             when (thresholds.PATTERN_DESCRIPTION = '5015') and 
                  (thresholds.MODULE_TYPE = 'ISR')
             then
                 '%'
             else
                 '%'
             end as v_samp_id_chk2
    from (
        select 
            phm_patterns_sk phm_thresholds_sk,
            case when threshold_number is null 
                 then 
                     9999
                 else 
                     to_number (threshold_number)
                 end threshold_number,
            case when threshold_number_unit is null 
                 then 
                     9999
                 else 
                     to_number (threshold_number_unit)
                 end threshold_number_unit,
            threshold_number_desc,
            phm_patterns_sk phm_patterns_sk,
            phm_thresholds_sk as thresholds_sk_val,
            pattern_description,
            threshold_alert,
            algorithm_type as algorithm_type,
            case when threshold_data_days is null 
            then 
                9999
            else 
                to_number (threshold_data_days)
            end threshold_data_days,
            case when module_type = 'ALL' 
                 then 
                     '%' 
                 else 
                     module_type 
                 end module_type
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
                        pai.phm_algorithm_definitions_sk = 1040 -- vn_alg_num
                ) ihn,
                (
                    select 
                        phm_patterns_sk, 
                        phm_thresholds_sk
                    from 
                        phm_thresholds pt
                    where 
                        pt.phm_algorithm_definitions_sk = 1040 -- vn_alg_num
                ) thr
            where     
                tp.phm_patterns_sk = p.phm_patterns_sk
            and 
                nvl (tp.delete_flag, 'N') <> 'Y'
            and 
                tp.phm_d_algorithm_definitions_sk = 1040 -- vn_alg_num
            and 
                p.phm_patterns_sk = ihn.phm_patterns_sk
            and 
                p.phm_patterns_sk = thr.phm_patterns_sk
        ) pivot ( 
            max ( parameter_values )
            for 
                parameter_name
            in (
                'ALGORITHM_TYPE' as algorithm_type,
                'ERROR_CODE_VALUE' as pattern_description,
                'ERROR_COUNT' as threshold_number,
                'IHN_LEVEL3_DESC' as threshold_alert,
                'THRESHOLD_DATA_DAYS' as threshold_data_days,
                'THRESHOLDS_DAYS' as threshold_number_unit,
                'THRESHOLD_DESCRIPTION' as threshold_number_desc,
                'MODULE' as module_type
            )
        )
        order by 
            phm_thresholds_sk,
            algorithm_type, 
            pattern_name
    ) thresholds
    where
        rownum < 10
)
select 
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc (completiondate) flag_date,
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today,
    max (ae.completiondate) max_compl_date,
    count (ae.errorcode) error_count,
    trunc ( (count (ae.errorcode) * 100 / ac.tubes_today), 2) error_percentage
from 
    svc_phm_ods.phm_ods_a3600_errors ae,
    a3600_layout_nodes_pl_sn n,
    idaowner.a3600systeminformation asi,
    idaowner.a3600_counters ac,
    (
    select 
        asi.productlineref,
        asi.deviceid,
        asi.systemsn,
        aln.sn,
        ae.nodetype,
        ae.errorcode,
        max (ae.completiondate) max_compl_date,
        trunc (min (ae.completiondate)) min_compl_date,
        parms.module_type,
        parms.pattern_description,
        parms.v_samp_id_chk1,
        parms.v_samp_id_chk2,
        parms.threshold_number_unit
    from 
        svc_phm_ods.phm_ods_a3600_errors ae,
        a3600_layout_nodes_pl_sn aln,
        idaowner.a3600systeminformation asi,
        (
            select * from params
        ) parms
    where     
        to_timestamp('08/12/2019 00:00:00', 
                     'MM/DD/YYYY HH24:MI:SS') <= run_date
    and 
        run_date < to_timestamp('08/13/2019 00:00:00', 
                                'MM/DD/YYYY HH24:MI:SS')
    and 
        ae.errorcode = parms.pattern_description
    and 
        ae.layout_nodes_id = aln.layout_nodes_id
    and 
        aln.systeminfoid = asi.systeminfoid
    and 
        aln.sn is not null
    and 
        aln.canid = ae.nodeid
    and 
        asi.current_row = 'Y'
    and 
        (((parms.module_type != '%') and 
          (ae.nodetype = parms.module_type)) or 
         ((parms.module_type = '%') and 
          (ae.nodetype like parms.module_type)))
    group by 
        asi.productlineref,
        asi.deviceid,
        asi.systemsn,
        aln.sn,
        ae.nodetype,
        ae.errorcode,
        parms.module_type,
        parms.pattern_description,
        parms.v_samp_id_chk1,
        parms.v_samp_id_chk2,
        parms.threshold_number_unit
    order by 
        asi.systemsn, 
        ae.nodetype, 
        ae.errorcode
    ) dds
where
    ae.layout_nodes_id = n.layout_nodes_id
and
    n.canid = ae.nodeid
and
    n.nodetype = ae.nodetype
and
    lower (n.sn) = lower (dds.sn)
and
    asi.systeminfoid = n.systeminfoid
and
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
    n.systeminfoid = asi.systeminfoid
and
    asi.current_row = 'Y'
and
    ac.tubes_today <> 0
and
    ((dds.module_type != '%' and ae.nodetype = dds.module_type) or 
     (dds.module_type = '%' and ae.nodetype like dds.module_type))
and
    ae.errorcode = dds.pattern_description
and
    nvl (ae.sampleid, dds.v_samp_id_chk1) like
    nvl (dds.v_samp_id_chk2, ae.sampleid)
and
    ae.completiondate between dds.min_compl_date - dds.threshold_number_unit + 1
and
    dds.max_compl_date
group by
    asi.deviceid,
    asi.systemsn,
    n.pl,
    n.sn,
    trunc (completiondate),
    ae.nodetype,
    ae.errorcode,
    ae.nodeid,
    ae.instanceid,
    ac.tubes_today

