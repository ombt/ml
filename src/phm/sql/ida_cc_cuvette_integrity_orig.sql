SELECT
    final2.MODULESN
FROM (
    SELECT
        middle2.*,
        (middle2.NUM_SAMPEVENTS_GT20000_PERCUV / 
         middle2.NUM_SAMPEVENTS_PERCUV) AS PERC_SAMPEVENTS_GT20000_PERCUV,
        CASE WHEN (middle2.NUM_SAMPEVENTS_GT20000_PERCUV / 
                   middle2.NUM_SAMPEVENTS_PERCUV) > 0.2
             THEN 1
             ELSE 0
             END AS GT20000_GT20PERC_SAMPEVENTS
    FROM (
        SELECT
            inner2.MODULESN,
            inner2.CUVETTENUMBER,
            COUNT(inner2.CUVETTENUMBER) AS NUM_SAMPEVENTS_PERCUV,
            SUM(inner2.CHECK_GT20000) AS NUM_SAMPEVENTS_GT20000_PERCUV
        FROM (
            SELECT
                SDP.SYSTEMSN,
                SDP.LOGDATE_LOCAL,
                SDP.DISPENSEBEGINAVERAGE,
                SDP.SAMPLEKEY,
                SDP.TESTNUMBER,
                SDP.REPLICATESTART,
                SDP.REPLICATENUMBER,
                DPM.MODULESN,
                DPM.SYSTEMSN,
                DPM.LOGDATE_LOCAL,
                DPM.SAMPLEKEY,
                DPM.TOSHIBATESTNUMBER,
                DPM.STARTINGREPLICATENUMBER,
                DPM.REPLICATENUMBER,
                R.SYSTEMSN,
                R.TESTID AS RESULTS_TESTID,
                R.CUVETTENUMBER,
                CASE WHEN SDP.DISPENSEBEGINAVERAGE > 20000
                     THEN 1
                     ELSE 0
                     END AS CHECK_GT20000
            FROM
                IDAQOWNER.ICQ_CCSAMPLEDISPCI SDP
            LEFT JOIN 
                IDAQOWNER.ICQ_CCDISPENSEPM DPM
            ON 
                SDP.SYSTEMSN = DPM.SYSTEMSN
            AND 
                DPM.LOGDATE_LOCAL 
                BETWEEN 
                    SDP.LOGDATE_LOCAL - INTERVAL '0.1' SECOND 
                AND 
                    SDP.LOGDATE_LOCAL + INTERVAL '0.1' SECOND
            AND 
                SDP.SAMPLEKEY = DPM.SAMPLEKEY
            AND 
                SDP.TESTNUMBER = DPM.TOSHIBATESTNUMBER
            AND 
                SDP.REPLICATESTART = DPM.STARTINGREPLICATENUMBER
            AND 
                SDP.REPLICATENUMBER = DPM.REPLICATENUMBER
            LEFT JOIN 
                IDAQOWNER.ICQ_RESULTS R
            ON 
                DPM.SYSTEMSN = R.SYSTEMSN
            AND 
                DPM.TESTID = R.TESTID
            AND 
                R.CUVETTENUMBER IS NOT NULL
            WHERE
                TO_TIMESTAMP('11/05/2018 00:00:00', 
                             'MM/DD/YYYY HH24:MI:SS') <= SDP.LOGDATE_LOCAL
            AND 
                SDP.LOGDATE_LOCAL < TO_TIMESTAMP('11/06/2018 00:00:00', 
                                                 'MM/DD/YYYY HH24:MI:SS')
        ) inner2        
        GROUP BY
            inner2.MODULESN,
            inner2.CUVETTENUMBER
        ORDER BY
            inner2.MODULESN,
            inner2.CUVETTENUMBER
        ) middle2
    WHERE
        middle2.NUM_SAMPEVENTS_PERCUV > 20
    AND 
        middle2.CUVETTENUMBER BETWEEN 1 AND 11
    ) final2
WHERE
    final2.GT20000_GT20PERC_SAMPEVENTS = 1
GROUP BY
    final2.MODULESN
HAVING
    COUNT(final2.MODULESN) <= 8
ORDER BY
    final2.MODULESN

