            SELECT 
                WZA.MODULESNDRM AS SN,
                WZA.WASHZONEID || '.' || WZA.POSITION AS WZPROBE,
                WZA.EVENTDATE AS FLAGDATEA,
                WZA.MAXTEMP,
                CASE WHEN 
                         WZA.MAXTEMP > 35 
                     AND 
                         LAG (WZA.MAXTEMP) OVER 
                         (
                             PARTITION BY
                                 WZA.MODULESNDRM, 
                                 WZA.WASHZONEID, 
                                 WZA.POSITION 
                             ORDER BY 
                                 WZA.EVENTDATE
                         ) > 35 
                     AND
                         LAG (WZA.MAXTEMP, 2) OVER 
                         (
                             PARTITION BY
                                 WZA.MODULESNDRM, 
                                 WZA.WASHZONEID, 
                                 WZA.POSITION 
                             ORDER BY 
                                 WZA.EVENTDATE
                         ) > 35 
                     AND
                         LAG (WZA.MAXTEMP, 3) OVER 
                         (
                             PARTITION BY
                                 WZA.MODULESNDRM, 
                                 WZA.WASHZONEID, 
                                 WZA.POSITION 
                             ORDER BY 
                                 WZA.EVENTDATE
                         ) > 35 
                     AND
                         LAG (WZA.MAXTEMP, 4) OVER 
                         (
                             PARTITION BY
                                 WZA.MODULESNDRM, 
                                 WZA.WASHZONEID, 
                                 WZA.POSITION 
                             ORDER BY 
                                 WZA.EVENTDATE
                         ) > 35
                     THEN 
                         'YES' 
                     ELSE 
                         'NO' 
                     END AS FLAGA 
            FROM ( 
                SELECT 
                    WA.MODULESNDRM,
                    WA.EVENTDATE,
                    WA.WASHZONEID -1 AS WASHZONEID,
                    '1' AS POSITION,
                    WA.POSITION1 AS REPLICATEID,
                    CASE WHEN 
                             WA.POSITION1 = LAG (WA.POSITION1) OVER 
                             (
                                 ORDER BY 
                                     WA.MODULESNDRM, 
                                     WA.POSITION1, 
                                     WA.WASHZONEID,
                                     WA.EVENTDATE
                             ) 
                         AND 
                             WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
                             (
                                 ORDER BY 
                                     WA.MODULESNDRM, 
                                     WA.POSITION1, 
                                     WA.WASHZONEID, 
                                     WA.EVENTDATE
                             ) 
                         AND 
                             WA.EVENTDATE -10 /(24*60*60) < 
                             LAG (WA.EVENTDATE) OVER 
                             (
                                 ORDER BY 
                                     WA.MODULESNDRM, 
                                     WA.POSITION1, 
                                     WA.WASHZONEID,
                                     WA.EVENTDATE
                             )
                         THEN 
                             'Probe 1 Second Temp'
                         ELSE 
                             'Probe 1 First Temp' 
                         END AS PIP_ORDER, 
                    WA.MAXTEMPPOSITION1/1000 MAXTEMP

                FROM 
                    IDAOWNER.WASHASPIRATIONS WA 
                WHERE 
                    WA.POSITION1 > 0 
                AND 
                    WA.EVENTDATE >= TRUNC(SYSDATE -1) 
                AND
                    WA.EVENTDATE <= TRUNC(SYSDATE) 
                UNION ALL 
                SELECT 
                    WA.MODULESNDRM,
                    WA.EVENTDATE,
                    WA.WASHZONEID -1 AS WASHZONEID,
                    '2' AS POSITION,
                    WA.POSITION2 AS REPLICATEID,
                    'Probe 2' AS PIP_ORDER,
                    WA.MAXTEMPPOSITION2/1000 MAXTEMP
                FROM 
                    IDAOWNER.WASHASPIRATIONS WA 
                WHERE 
                    WA.POSITION2 > 0 
                AND 
                    WA.EVENTDATE >= TRUNC(SYSDATE -1) 
                AND
                    WA.EVENTDATE <= TRUNC(SYSDATE) 
                UNION ALL 
                SELECT 
                    WA.MODULESNDRM,
                    WA.EVENTDATE,
                    WA.WASHZONEID -1 AS WASHZONEID,
                    '3' AS POSITION,
                    WA.POSITION3 AS REPLICATEID,
                    CASE WHEN 
                             WA.POSITION3 = LAG (WA.POSITION3) OVER 
                             (
                                 ORDER BY 
                                     WA.MODULESNDRM, 
                                     WA.POSITION3, 
                                     WA.WASHZONEID,
                                     WA.EVENTDATE
                             ) 
                         AND 
                             WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
                             (
                                 ORDER BY 
                                     WA.MODULESNDRM, 
                                     WA.POSITION3, 
                                     WA.WASHZONEID,
                                     WA.EVENTDATE
                             ) 
                         AND 
                             WA.EVENTDATE -10 /(24*60*60) < LAG (WA.EVENTDATE) OVER 
                             (
                                 ORDER BY 
                                     WA.MODULESNDRM, 
                                     WA.POSITION3, 
                                     WA.WASHZONEID,
                                     WA.EVENTDATE
                             )
                         THEN 
                             'Probe 3 Second Temp'
                         ELSE 
                             'Probe 3 First Temp' ENDAS PIP_ORDER,
                    WA.MAXTEMPPOSITION3/1000 MAXTEMP
                FROM 
                    IDAOWNER.WASHASPIRATIONS WA 
                WHERE 
                    WA.POSITION3 > 0 
                AND 
                    WA.EVENTDATE >= TRUNC(SYSDATE -1)
                AND
                    WA.EVENTDATE <= TRUNC(SYSDATE)
            ) WZA 
            WHERE 
                NOT WZA.PIP_ORDER = 'Probe 3 Second Temp' 
            AND
                NOT WZA.PIP_ORDER = 'Probe 1 First Temp' 
