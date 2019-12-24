
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

select 
    asi.productlineref,
    asi.deviceid,
    asi.systemsn,
    aln.sn,
    ae.nodetype,
    ae.errorcode,
    max (ae.completiondate) max_compl_date,
    trunc (min (ae.completiondate)) min_compl_date
from 
    svc_phm_ods.phm_ods_a3600_errors ae,
    a3600_layout_nodes_pl_sn aln,
    idaowner.a3600systeminformation asi
where
    -- batch_num = v_batch_num
-- and 
    run_date = to_date('2019-08-01', 'YYYY-MM-DD') -- v_run_date
-- and 
    -- ae.errorcode = '04F8' -- v_errorcode1
-- and 
    ae.layout_nodes_id = aln.layout_nodes_id
and 
    aln.systeminfoid = asi.systeminfoid
and 
    aln.sn is not null
and 
    aln.canid = ae.nodeid
and 
    asi.current_row = 'Y'
-- and 
    -- (('IOM' != '%' and ae.nodetype = 'IOM') or
     -- ('IOM' = '%' and ae.nodetype like 'IOM'))
group by 
    asi.productlineref,
    asi.deviceid,
    asi.systemsn,
    aln.sn,
    ae.nodetype,
    ae.errorcode
order by 
    asi.systemsn, 
    ae.nodetype, 
    ae.errorcode
