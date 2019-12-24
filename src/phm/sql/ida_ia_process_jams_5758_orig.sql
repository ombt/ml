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
            to_timestamp('02/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS') <= m.logdate_local
        and 
            m.logdate_local < to_timestamp('02/28/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
        AND 
            M.AIMCODE = '5758'
        AND 
            M.AIMSUBCODE = 'D299'
        ) INNER1
    LEFT JOIN (
        SELECT
            R.MODULESN,
            COUNT(R.CORRECTEDCOUNT) AS NUM_RESULTS
        FROM
            IDAQOWNER.ICQ_RESULTS R
        WHERE
            to_timestamp('02/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
        and 
            r.logdate_local < to_timestamp('02/28/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
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
    EVALS.NUM_RETRIES >= 4

