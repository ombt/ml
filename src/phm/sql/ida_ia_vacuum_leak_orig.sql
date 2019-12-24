SELECT
    evalflags.MODULESN,
    evalflags.SevenDAYGROUP
FROM (
    SELECT
        flags.MODULESN,
        SUM(CASE WHEN trunc(flags.LogDate) >= trunc(to_timestamp('12/02/2018 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
                 AND TRUNC(flags.LogDate) < TRUNC(to_timestamp('12/09/2018 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
                 THEN 1
                 ELSE 0
                 END) AS SevenDAYGROUP
    FROM (
        SELECT
            evals.MODULESN,
            evals.LogDate,
            evals.meanPercentDiff
        FROM (
            SELECT
                raws.MODULESN,
                (trunc(raws.LOGDATE_LOCAL)) AS LogDate,
                AVG(raws.PercentDiff) AS meanPercentDiff
            FROM (
                SELECT
                    t1.MODULESN, 
                    t1.LOGDATE_LOCAL, 
                    t1.VACUUMSTATENAME, 
                    t1.VERIFYVACUUMSUBSTATENAME, 
                    100*(t1.ADCVALUELEAKTEST-t1.ADCVALUE)/t1.ADCVALUE AS PercentDiff, 
                    t1.ADCVALUE, 
                    t1.ADCVALUELEAKTEST
                FROM 
                    IDAQOWNER.ICQ_VACUUMPRESSUREDATA  t1 
                WHERE  
                to_timestamp('12/02/2018 00:00:00', 
                             'MM/DD/YYYY HH24:MI:SS') <= t1.logdate_local
            and 
                t1.logdate_local < to_timestamp('12/09/2018 00:00:00', 
                                                'MM/DD/YYYY HH24:MI:SS')
                AND  
                    t1.VACUUMSTATENAME =  'ConcludeLeakTest'
                ) raws
            GROUP BY
                raws.MODULESN,
                trunc(raws.LOGDATE_LOCAL) 
            ORDER BY
                raws.MODULESN,
                trunc(raws.LOGDATE_LOCAL)
            ) evals
        WHERE
            meanPercentDiff>=7
        ) flags 
    GROUP BY
        flags.MODULESN
    ORDER BY
        flags.MODULESN)evalFlags
WHERE 
    evalFlags.SevenDAYGROUP>=2

