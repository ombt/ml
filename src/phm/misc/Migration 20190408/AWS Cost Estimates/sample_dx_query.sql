SELECT evals.moduleserialnumber, pm_date
  FROM (  SELECT PM.moduleserialnumber, date(datetimestamplocal) pm_date,
                 COUNT (PM.pipettormechanismname)
                     AS ASPIRATIONS,
                 SUM (
                     CASE
                         WHEN    PM.frontendpressure > 27000
                              OR PM.frontendpressure < 21000
                         THEN
                             1
                         ELSE
                             0
                     END)
                     AS NUMFLAGS
            FROM dx.dx_205_pmevent PM
           WHERE     /*PM.datetimestamplocal >=
                     date_add ('day', -1, CURRENT_DATE)
                 AND PM.datetimestamplocal < CURRENT_DATE */
                 PM.datetimestamplocal >= date_parse('2019-02-01', '%Y-%m-%d') AND PM.datetimestamplocal < date_parse('2019-03-10', '%Y-%m-%d')
                 --PM.datetimestamputc >= date_parse('2019-02-01', '%Y-%m-%d') AND PM.datetimestamputc < date_parse('2019-03-10', '%Y-%m-%d')
                 -- date_parse('7/22/2016 6:05:04 PM','%m/%d/%Y %h:%i:%s %p')
                 AND PM.frontendpressure IS NOT NULL
                 AND PM.pipettingprotocolname != 'NonPipettingProtocol'
                 AND PM.pipettormechanismname = 'Reagent1PipettorMechanism'
        GROUP BY PM.moduleserialnumber, date(datetimestamplocal)
        ORDER BY PM.moduleserialnumber) evals
WHERE     (evals.NUMFLAGS / evals.ASPIRATIONS) >= 0.02
       AND evals.ASPIRATIONS >= 10;
