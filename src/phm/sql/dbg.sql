SELECT
    EVALS.MODULESN
FROM (
    SELECT
        INNER1.MODULESN,
        COUNT(INNER1.MODULESN) AS NUM_RETRIES
    FROM (
        SELECT
            M.MODULESN
        FROM
            IDAQOWNER.ICQ_MESSAGEHISTORY M
        WHERE
            M.LOGDATE_LOCAL >= TRUNC(SYSDATE) - 1
        AND 
            M.LOGDATE_LOCAL < TRUNC(SYSDATE)
        AND 
            M.AIMCODE = '5756'
        AND 
            M.AIMSUBCODE = 'D298'
    ) INNER1
    LEFT JOIN (
        SELECT
            R.MODULESN,
            COUNT(R.CORRECTEDCOUNT) AS NUM_RESULTS
        FROM
            IDAQOWNER.ICQ_RESULTS R
        WHERE
            R.LOGDATE_LOCAL >= TRUNC(SYSDATE) - 1
        AND 
            R.LOGDATE_LOCAL < TRUNC(SYSDATE)
        AND 
            R.CORRECTEDCOUNT IS NOT NULL
        GROUP BY
            R.MODULESN
        ) INNER2
    ON 
        INNER1.MODULESN = INNER2.MODULESN
    WHERE
        INNER2.NUM_RESULTS >= 10
    GROUP BY
        INNER1.MODULESN
    ) EVALS
WHERE
    EVALS.NUM_RETRIES >= 8        
order by
    EVALS.MODULESN


