SELECT
    evals.DEVICEID,
    evals.MODULESN,
    evals.MEAN_ADC,
    evals.NUM_READINGS
FROM (
    SELECT
        V.DEVICEID,
        V.MODULESN,
        AVG(V.ADCVALUE) AS MEAN_ADC,
        COUNT(V.ADCVALUE) AS NUM_READINGS
    FROM 
        IDAQOWNER.ICQ_VACUUMPRESSUREDATA V
    WHERE
        TO_TIMESTAMP('01/01/2019 00:00:00', 
                     'MM/DD/YYYY HH24:MI:SS') <= V.LOGDATE_LOCAL
    AND 
        V.LOGDATE_LOCAL < TO_TIMESTAMP('02/01/2019 00:00:00', 
                                       'MM/DD/YYYY HH24:MI:SS')
    AND
        V.VACUUMSTATENAME = 'VacuumBledOff'
    GROUP BY
        V.DEVICEID,
        V.MODULESN
    ORDER BY
        V.DEVICEID,
        V.MODULESN
    ) evals
WHERE (
    evals.MEAN_ADC <= 3549
AND 
    evals.NUM_READINGS >= 3 )

