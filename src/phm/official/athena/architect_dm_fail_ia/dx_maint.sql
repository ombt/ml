-- SELECT 
--     MA.FILEID,
--     MA.DEVICEID,
--     MA.MODULEID,
--     MA.MODULESNDRM,
--     MA.COMPLETIONDATE,
--     MA.RESULT,
--     CASE WHEN (MA.RESULT LIKE 'Completed' OR 
--                MA.RESULT LIKE 'Completata' OR
--                MA.RESULT LIKE 'Finalizado' OR
--                MA.RESULT LIKE 'Terminé' OR
--                MA.RESULT LIKE 'Abgeschlossen' OR
--                MA.RESULT LIKE 'Concluído' OR
--                MA.RESULT LIKE 'Dokonceno' OR
--                MA.RESULT LIKE '??????' OR
--                MA.RESULT LIKE '??' OR 
--                MA.RESULT LIKE '???') 
--          THEN 'Complete' 
--          WHEN (MA.RESULT LIKE 'Failed' OR
--                MA.RESULT LIKE '??' OR
--                MA.RESULT LIKE '???' OR
--                MA.RESULT LIKE 'Fallita' OR
--                MA.RESULT LIKE 'Fallido' OR
--                MA.RESULT LIKE 'Echoué' OR
--                MA.RESULT LIKE 'Fehlgeschlagen' OR
--                MA.RESULT LIKE 'Falhado' OR
--                MA.RESULT LIKE 'Chyba' OR
--                MA.RESULT LIKE '??????')
--          THEN 'Failed'
--          WHEN (MA.RESULT LIKE 'User canceled' OR
--                MA.RESULT LIKE '????' OR
--                MA.RESULT LIKE '???????????' OR
--                MA.RESULT LIKE 'Annullata' OR
--                MA.RESULT LIKE 'Cancel usuario' OR
--                MA.RESULT LIKE 'Cancelado utili.' OR
--                MA.RESULT LIKE 'Annulé par utilis.' OR
--                MA.RESULT LIKE 'Benutzerabbruch' OR
--                MA.RESULT LIKE 'Zru' OR
--                MA.RESULT LIKE 'Zrušeno uživatelem' OR
--                MA.RESULT LIKE '??????')
--           THEN 'Failed'
--           ELSE NULL 
--           END AS RESULT_TR
-- FROM
--     IDAOWNER.MAINTENANCELOGS MA
-- WHERE
--     MA."PROCEDURE" LIKE '%6041%' 
-- AND
--     MA.COMPLETIONDATE >= SYSDATE - 60 
-- and
--     rownum < 100
-- ORDER BY 
--     MA.MODULESNDRM DESC, 
--     MA.COMPLETIONDATE DESC)

with maint_cte as (
select 
    ma.architect_deviceid as deviceid,
    ma.architect_moduleid as moduleid,
    upper(trim((ma.architect_moduleserial)) as modulesn,
    ma.architect_productline as pl,
    ma.completiondate_iso as flag_date,
    ma.result,
    case when (ma.result like 'Completed' or 
               ma.result like 'Completata' or
               ma.result like 'Finalizado' or
               ma.result like 'Terminé' or
               ma.result like 'Abgeschlossen' or
               ma.result like 'Concluído' or
               ma.result like 'Dokonceno' or
               ma.result like '??????' or
               ma.result like '??' or 
               ma.result like '???') 
         then 'Complete' 
         when (ma.result like 'Failed' or
               ma.result like '??' or
               ma.result like '???' or
               ma.result like 'Fallita' or
               ma.result like 'Fallido' or
               ma.result like 'Echoué' or
               ma.result like 'Fehlgeschlagen' or
               ma.result like 'Falhado' or
               ma.result like 'Chyba' or
               ma.result like '??????')
         then 'Failed'
         when (ma.result like 'User canceled' or
               ma.result like '????' or
               ma.result like '???????????' or
               ma.result like 'Annullata' or
               ma.result like 'Cancel usuario' or
               ma.result like 'Cancelado utili.' or
               ma.result like 'Annulé par utilis.' or
               ma.result like 'Benutzerabbruch' or
               ma.result like 'Zru' or
               ma.result like 'Zrušeno uživatelem' or
               ma.result like '??????')
          then 'Failed'
          else null 
          end as result_tr
from
    dx.dx_architect_maint MA
where
    ma."procedure" LIKE '%6041%' 
and
    ma.architect_productline in ( '115', '116', '117' )
and
    '2019-08-16' <= ma.transaction_date
and
    ma.transaction_date < '2019-10-16'
order by 
    ma.architect_moduleserial desc, 
    ma.completiondate_iso desc
)
SELECT
    FINALDAT.DEVICEID,
    FINALDAT.MODULESNDRM,
    FINALDAT.RESULT_TR,
    FINALDAT.MINDATE AS FIRST_FAIL,
    FINALDAT.MAXDATE AS LAST_FAIL,
    FINALDAT.CNT AS N_FAIL,
    FINALDAT.N_DAYS_WITH_FAIL,
    TRUNC(SYSDATE,'DDD') - TRUNC(FINALDAT.MINDATE,'DDD') AS N_DAYS_SINCE_FIRST_FAIL
FROM (
    SELECT
        DAT.DEVICEID,
        DAT.MODULESNDRM,
        DAT.RESULT_TR,
        DAT.MINDATE,
        DAT.MAXDATE,
        DAT.CNT,
        DAT.N_DAYS_WITH_FAIL,
        ROW_NUMBER() OVER (PARTITION BY DAT.MODULESNDRM ORDER BY DAT.MAXDATE DESC) AS RN
    FROM (
        SELECT
            RESULTS.DEVICEID,
            RESULTS.MODULESNDRM,
            RESULTS.RESULT_TR,
            MIN(RESULTS.COMPLETIONDATE) AS MINDATE,
            MAX(RESULTS.COMPLETIONDATE) AS MAXDATE,
            COUNT(DISTINCT(TRUNC(RESULTS.COMPLETIONDATE,'DDD'))) AS N_DAYS_WITH_FAIL,
            COUNT(*) AS CNT
        FROM (
            SELECT
                M.*,
                ROW_NUMBER() OVER (
                    ORDER BY 
                        M.MODULESNDRM,
                        M.COMPLETIONDATE DESC
                ) AS A,
                ROW_NUMBER() OVER (
                    PARTITION BY M.RESULT_TR 
                        ORDER BY 
                            M.MODULESNDRM,
                            M.COMPLETIONDATE DESC
                ) AS B,
                (
                    ROW_NUMBER() OVER (
                        ORDER BY 
                        M.MODULESNDRM,M.COMPLETIONDATE DESC
                    ) - 
                    ROW_NUMBER() OVER (
                        PARTITION BY M.RESULT_TR 
                            ORDER BY 
                                M.MODULESNDRM,
                                M.COMPLETIONDATE DESC
                    )
                ) AS DI
            FROM (
                SELECT *
                FROM
                    SVC_PHM_ODS.PHM_ODS_DM_FAIL_IA MA
                ORDER BY 
                    MA.MODULESNDRM,
                    MA.COMPLETIONDATE DESC
                ) M
            ORDER BY 
                M.MODULESNDRM,
                M.COMPLETIONDATE DESC
            ) RESULTS
        GROUP BY
            RESULTS.DEVICEID,
            RESULTS.MODULESNDRM,
            RESULTS.RESULT_TR,
            RESULTS.DI
        ORDER BY 
            RESULTS.MODULESNDRM
        ) DAT
    ) FINALDAT
    WHERE 
        FINALDAT.RN = 1 
    AND 
        FINALDAT.RESULT_TR = 'Failed' 
    AND 
        FINALDAT.N_DAYS_WITH_FAIL > 1
)
