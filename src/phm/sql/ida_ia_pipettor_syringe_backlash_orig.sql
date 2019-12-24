SELECT
    inner.MODULESN,
    inner.FLAG,
    inner.NUM_TESTS
FROM ( 
    SELECT
        IA.MODULESN,
        COUNT(regexp_substr(IA.ACTIVITY,'\PosDiff:\s(.*?)\Z',1,1,null,1)) AS NUM_TESTS,
        CASE WHEN AVG(regexp_substr(IA.ACTIVITY,'\PosDiff:\s(.*?)\Z',1,1,null,1)) > 95 
             THEN 1 
             ELSE 0 
        END AS FLAG
    FROM 
        IDAQOWNER.ICQ_INSTRUMENTACTIVITY IA
    WHERE
    to_timestamp('02/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS') <= ia.logdate_local
and 
    ia.logdate_local < to_timestamp('03/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
    AND 
        IA.ACTIVITY LIKE 'SyringeCheckResult for pipettor: SamplePipettor%'
    GROUP BY
        IA.MODULESN
    ) inner
WHERE
    inner.FLAG >= 1
AND 
    inner.NUM_TESTS >= 5

