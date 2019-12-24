SELECT
    MODULESN,
    DeviceId 
FROM (
    SELECT
        DeviceId, 
        MODULESN, 
        TO_CHAR(LOGDATE_LOCAL, 'YYYY-MM-DD') AS date_only,
        CASE
            WHEN CORRECTEDCOUNT <= 30 AND peak_adj_signal / dark_adj_signal <= 0.44 
            THEN 1
            WHEN CORRECTEDCOUNT <= 50 AND peak_adj_signal / dark_adj_signal <= 0.40 
            THEN 1
            WHEN CORRECTEDCOUNT <= 70 AND peak_adj_signal / dark_adj_signal <= 0.35 
            THEN 1
            ELSE 0
            END AS shapeflag
    FROM (
        SELECT
            i.DeviceId, 
            i.MODULESN, 
            i.LOGDATE_LOCAL, 
            i.CORRECTEDCOUNT,
            SUM(CASE WHEN r.signal > i.DARKAVERAGE AND r.time IN (4, 5, 6, 7)
                     THEN r.signal - i.DARKAVERAGE 
                     ELSE 0 
                     END) AS peak_adj_signal,
            SUM(CASE WHEN r.signal > i.DARKAVERAGE
                     THEN r.signal - i.DARKAVERAGE 
                     ELSE 0 
                     END) AS dark_adj_signal
        FROM 
            SVC_PHM_ODS.PHM_ODS_ICQ_RESULTS i --IDAQOWNER.ICQ_RESULTS i
        INNER JOIN (
            SELECT 
                *
            FROM 
                SVC_PHM_ODS.PHM_ODS_ICQ_RESULTS_READS  --IDAQOWNER.ICQ_RESULTS_READS
            UNPIVOT (signal FOR time IN
            (S01 AS  1, S02 AS  2, S03 AS  3, S04 AS  4, S05 AS  5, S06 AS  6,
             S07 AS  7, S08 AS  8, S09 AS  9, S10 AS 10, S11 AS 11, S12 AS 12,
             S13 AS 13, S14 AS 14, S15 AS 15, S16 AS 16, S17 AS 17, S18 AS 18,
             S19 AS 19, S20 AS 20, S21 AS 21, S22 AS 22, S23 AS 23, S24 AS 24,
             S25 AS 25, S26 AS 26, S27 AS 27, S28 AS 28, S29 AS 29, S30 AS 30))
            ) r
        ON
            i.ID = r.ICQ_RESULTS_ID
        WHERE 
            TRUNC(i.LOGDATE_LOCAL) = TRUNC(SYSDATE) - 1     
        AND
            TRUNC(r.LOGDATE_LOCAL) = TRUNC(SYSDATE) - 1     
        AND
            LOWER(i.SAMPLEID) NOT LIKE  '%saline%'         
        AND
            LOWER(i.SAMPLEID) NOT LIKE  '%buf%'            
        AND
            LOWER(i.OPERATORID) NOT LIKE 'fse'             
        AND
            ASSAYNUMBER NOT LIKE  '%213%'                  
        AND
            ASSAYNUMBER NOT LIKE  '%216%'            
        GROUP BY
            i.DeviceId, 
            i.MODULESN, 
            i.LOGDATE_LOCAL, 
            i.CORRECTEDCOUNT
        )
    )
GROUP BY
    DeviceId, 
    MODULESN, 
    date_only
HAVING
    COUNT(*) >= 50 
AND
    AVG(shapeflag) > 0.01;

