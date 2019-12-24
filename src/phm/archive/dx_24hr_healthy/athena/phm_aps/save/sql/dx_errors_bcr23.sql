-- 
--    CURSOR CURAPS_ERRORS_BCR23 (VSN VARCHAR2, 
--                                VSTARTDATE DATE, 
--                                VENDDATE DATE, 
--                                PATTERN_TEXT VARCHAR2)
--    IS 
--        SELECT 
--            DEVICE_ID, 
--            MAX(PSM.IOM_SN) IOM_SN, 
--            PSM.PL, 
--            PSM.SN, 
--            TRUNC(TIMESTAMP) DT, 
--            COUNT(*) PAT_ERRCOUNT, 
--            MAX(TIMESTAMP) FLG_DATE
--        FROM 
--            SVC_PHM_ODS.PHM_ODS_APS_ERRORS AE, 
--            SVC_GSR_OWNER.APS_MESSAGES_PL_SN_MAP PSM 
--        WHERE 
--  Unreadable Sample ID (BCR 2),10330,ERROR_CODE_REG_EXPR,Unreadable Sample ID (BCR 2)
--            AE.MESSAGE LIKE '%' || PATTERN_TEXT  || '%' AND PSM.MESSAGE LIKE '%' || PATTERN_TEXT  || '%' 
--        and 
--            AE.SN = PSM.IOM_SN 
--        AND 
--            AE.SN = VSN  
--        AND 
--            TIMESTAMP BETWEEN 
--                VSTARTDATE 
--            AND 
--                VENDDATE
--        GROUP BY 
--            DEVICE_ID, 
--            PSM.PL, 
--            PSM.SN, 
--            TRUNC(TIMESTAMP)
--        ORDER BY 
--            IOM_SN, 
--            SN, 
--            DT;

select
    max(am.iom_sn) as iom_sn,
    am.pl as pl,
    upper(trim(am.sn)) as module_sn,
    date_trunc('day', ae.timestamp_iso) as dt,
    count(*) as pat_error_count,
    max(ae.timestamp_iso) as flag_date
from 
    dx.dx_aps_error ae
inner join
    dx_phm.phm_aps_pl_sn_mapping am
on
    ae.serialnumber = am.iom_sn
and
    --  'Unreadable Sample ID (BCR 2)'
    --  'Unreadable Sample ID (BCR 3)'
    am.message like '%' || 'Unreadable Sample ID (BCR 2)' || '%'
where 
    ae.message like '%' || 'Unreadable Sample ID (BCR 2)' || '%' 
and 
    '2019-07-01' <= ae.transaction_date
and
    ae.transaction_date < '2019-07-02'
group by
    am.pl,
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
order by
    max(am.iom_sn),
    upper(trim(am.sn)),
    date_trunc('day', ae.timestamp_iso)
