SELECT
    evals.MODULESN
FROM (
    SELECT
        I.MODULESN,
        CASE WHEN I.VACUUMSTATENAME || 
                  I.VERIFYVACUUMSUBSTATENAME = 'VerifyVacuum'
             THEN 'DisableVacuum'
             END AS Vac_STATE,
        MIN(I.ADCVALUE) AS MIN_ADCVALUE,
        COUNT(I.ADCVALUE) AS NUM_EVALS
    FROM
        IDAQOWNER.ICQ_VACUUMPRESSUREDATA  I
    WHERE
        to_timestamp('02/01/2019 00:00:00', 
                     'MM/DD/YYYY HH24:MI:SS') <= i.logdate_local
    and 
        i.logdate_local < to_timestamp('02/02/2019 00:00:00', 
                                       'MM/DD/YYYY HH24:MI:SS')
    AND 
        I.VACUUMSTATENAME = 'VerifyVacuum'
    AND 
        I.VERIFYVACUUMSUBSTATENAME = 'DisableVacuum'
    GROUP BY
        I.MODULESN,
        I.VACUUMSTATENAME,
        I.VERIFYVACUUMSUBSTATENAME
    ORDER BY
        I.MODULESN,
        I.VACUUMSTATENAME,
        I.VERIFYVACUUMSUBSTATENAME        
    ) evals
WHERE
    evals.MIN_ADCVALUE > 2160
AND 
    evals.NUM_EVALS >= 30
