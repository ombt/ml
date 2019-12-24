SELECT
    evals.DEVICEID,
    evals.MODULESN,
    evals.ASPIRATIONS,
    evals.NUMFLAGS
FROM ( 
    SELECT
        PM.DEVICEID,
        PM.MODULESN,
        COUNT(PM.PIPETTORMECHANISMNAME) AS ASPIRATIONS,
        SUM(CASE WHEN PM.FRONTENDPRESSURE > 27000 OR 
                      PM.FRONTENDPRESSURE < 21000
                 THEN 1
                 ELSE 0
                 END) AS NUMFLAGS
    FROM
        IDAQOWNER.ICQ_PMEVENTS PM
    WHERE
        TO_TIMESTAMP('02/01/2019 00:00:00', 
                     'MM/DD/YYYY HH24:MI:SS') <= PM.LOGDATE_LOCAL
    AND 
        PM.LOGDATE_LOCAL < TO_TIMESTAMP('03/01/2019 00:00:00', 
                                        'MM/DD/YYYY HH24:MI:SS')
    AND 
        PM.FRONTENDPRESSURE IS NOT NULL
    AND 
        PM.PIPETTINGPROTOCOLNAME != 'NonPipettingProtocol'
    AND 
        PM.PIPETTORMECHANISMNAME IN (
            'SamplePipettorMechanism',
            'Reagent1PipettorMechanism',
            'Reagent2PipettorMechanism'
        )
    GROUP BY
        PM.DEVICEID,
        PM.MODULESN
    ) evals
WHERE
    evals.ASPIRATIONS >= 10
AND
    (evals.NUMFLAGS / evals.ASPIRATIONS) >= 0.02
ORDER BY
    evals.DEVICEID,
    evals.MODULESN
