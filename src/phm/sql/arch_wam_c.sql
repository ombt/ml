-- idalogdetails,begindatedata
-- idalogdetails,begindatedatab4delete
-- idalogdetails,datatype
-- idalogdetails,enddatedata
-- idalogdetails,enddatedatab4delete
-- idalogdetails,errorcode
-- idalogdetails,fileid
-- idalogdetails,filename
-- idalogdetails,filestructure
-- idalogdetails,futurerecordcount
-- idalogdetails,invalidrecordcount
-- idalogdetails,linesread
-- idalogdetails,loadendtime
-- idalogdetails,loadseconds
-- idalogdetails,loadstarttime
-- idalogdetails,logdetailid
-- idalogdetails,pastrecordcount
-- idalogdetails,processcode
-- idalogdetails,recordscreated
-- idalogdetails,validrecordcount
-- 
-- idalogfiles,deviceid
-- idalogfiles,epochdate
-- idalogfiles,epochdatevalue
-- idalogfiles,errorcode
-- idalogfiles,fileid
-- idalogfiles,filename
-- idalogfiles,filesourcedate
-- idalogfiles,loadendtime
-- idalogfiles,loadseconds
-- idalogfiles,loadstarttime
-- idalogfiles,processflag
-- 
-- washaspirations,deviceid
-- washaspirations,eventdate
-- washaspirations,fileid
-- washaspirations,loaddate
-- washaspirations,maxtempposition1
-- washaspirations,maxtempposition2
-- washaspirations,maxtempposition3
-- washaspirations,moduleid
-- washaspirations,modulesndrm
-- washaspirations,position1
-- washaspirations,position2
-- washaspirations,position3
-- washaspirations,tempdeltaposition1
-- washaspirations,tempdeltaposition2
-- washaspirations,tempdeltaposition3
-- washaspirations,washzoneid

-- SELECT 
--     ABC.COUNTRY,
--     ABC.CITY, 
--     ABC.CUSTOMER,
--     ABC.SN AS MODULESN,
--     ABC.WZPROBE,
--     to_char(
--     CASE WHEN MIN(ABC.FLAGDATEA) IS NOT NULL AND
--               MIN(ABC.FLAGDATEB) IS NULL AND
--               MIN(ABC.FLAGDATEC) IS NULL
--          THEN 
--              MIN(ABC.FLAGDATEA)
--          WHEN MIN(ABC.FLAGDATEB) IS NOT NULL AND
--               MIN(ABC.FLAGDATEA) IS NULL AND
--               MIN(ABC.FLAGDATEC) IS NULL
--          THEN 
--              MIN(ABC.FLAGDATEB)
--          WHEN MIN(ABC.FLAGDATEC) IS NOT NULL AND
--               MIN(ABC.FLAGDATEA) IS NULL AND
--               MIN(ABC.FLAGDATEB) IS NULL
--          THEN 
--              MIN(ABC.FLAGDATEC) 
--          WHEN MIN(ABC.FLAGDATEA) IS NOT NULL AND
--               MIN(ABC.FLAGDATEB) IS NOT NULL AND
--               MIN(ABC.FLAGDATEC) IS NULL
--          THEN 
--              LEAST(MIN(ABC.FLAGDATEA), 
--                    MIN(ABC.FLAGDATEB))
--          WHEN MIN(ABC.FLAGDATEB) IS NOT NULL AND
--               MIN(ABC.FLAGDATEC) IS NOT NULL AND
--               MIN(ABC.FLAGDATEA) IS NULL
--          THEN 
--              LEAST(MIN(ABC.FLAGDATEB), 
--                    MIN(ABC.FLAGDATEC))
--          WHEN MIN(ABC.FLAGDATEC) IS NOT NULL AND
--               MIN(ABC.FLAGDATEA) IS NOT NULL AND
--               MIN(ABC.FLAGDATEB) IS NULL
--          THEN 
--              LEAST(MIN(ABC.FLAGDATEC), 
--                    MIN(ABC.FLAGDATEA)) 
--          WHEN MIN(ABC.FLAGDATEA) IS NOT NULL AND
--               MIN(ABC.FLAGDATEB) IS NOT NULL AND 
--               MIN(ABC.FLAGDATEC) IS NOT NULL
--          THEN 
--              LEAST(MIN(ABC.FLAGDATEA), 
--                    MIN(ABC.FLAGDATEB), 
--                    MIN(ABC.FLAGDATEC)) 
--          END, 'YYYYMMDDHH24MISS') as flag_date,
--     ABC.FLAGA, 
--     ABC.FLAGB,
--     ABC.FLAGC,
--     IDAM.PRODUCTLINE as PL2
-- FROM (
--     SELECT 
--         CASE WHEN MAX(A.FLAGA) = 'YES'
--              THEN 
--                  A.COUNTRY 
--              WHEN MAX(B.FLAGB) = 'YES'
--              THEN 
--                  B.COUNTRY 
--              WHEN MAX(C.FLAGC) = 'YES'
--              THEN 
--                  C.COUNTRY 
--              END AS COUNTRY,
--         CASE WHEN MAX(A.FLAGA) = 'YES'
--              THEN 
--                  A.CITY 
--              WHEN MAX(B.FLAGB) = 'YES'
--              THEN 
--                  B.CITY 
--              WHEN MAX(C.FLAGC) = 'YES'
--              THEN 
--                  C.CITY 
--              END AS CITY,
--         CASE WHEN MAX(A.FLAGA) = 'YES'
--              THEN 
--                  A.CUSTOMER 
--              WHEN MAX(B.FLAGB) = 'YES'
--              THEN 
--                  B.CUSTOMER 
--              WHEN MAX(C.FLAGC) = 'YES'
--              THEN 
--                  C.CUSTOMER 
--              END AS CUSTOMER,
--         CASE WHEN MAX(A.FLAGA) = 'YES'
--              THEN 
--                  A.SN 
--              WHEN MAX(B.FLAGB) = 'YES'
--              THEN 
--                  B.SN 
--              WHEN MAX(C.FLAGC) = 'YES'
--              THEN 
--                  C.SN 
--              END AS SN,
--         CASE WHEN MAX(A.FLAGA) = 'YES'
--              THEN 
--                  A.WZPROBEA 
--              WHEN MAX(B.FLAGB) = 'YES'
--              THEN 
--                  B.WZPROBEB 
--              WHEN MAX(C.FLAGC) = 'YES'
--              THEN 
--                  C.WZPROBEC 
--              END AS WZPROBE, 
--         MIN(A.FLAGDATEA) AS FLAGDATEA,
--         MIN(B.FLAGDATEB) AS FLAGDATEB,
--         MIN(C.FLAGDATEC) AS FLAGDATEC,
--         CASE WHEN MAX(A.FLAGA) = 'YES'
--              THEN 'YES' 
--              ELSE 'NO' 
--              END AS FLAGA,
--         CASE WHEN MAX(B.FLAGB) = 'YES'
--              THEN 'YES' 
--              ELSE 'NO' 
--              END AS FLAGB,
--         CASE WHEN MAX(C.FLAGC) = 'YES'
--              THEN 'YES' 
--              ELSE 'NO' 
--              END AS FLAGC 
--     FROM (
--         SELECT 
--             RAWA.COUNTRY, 
--             RAWA.CITY,
--             RAWA.CUSTOMER,
--             RAWA.SN,
--             RAWA.WZPROBE AS WZPROBEA,
--             MAX(RAWA.FLAGA) AS FLAGA,
--             MIN(RAWA.FLAGDATEA) AS FLAGDATEA
--         FROM (
--             SELECT 
--                 IA.AREA,
--                 IA.COUNTRYNAME COUNTRY, 
--                 IA.CITY,
--                 substr(IA.CUSTOMERNAME,1,22) CUSTOMER,
--                 IA.CUSTOMERNAME CUSTOMER_NAME,
--                 WZA.MODULESNDRM AS SN,
--                 WZA.WASHZONEID || '.' || WZA.POSITION AS WZPROBE,
--                 WZA.EVENTDATE AS FLAGDATEA,
--                 WZA.MAXTEMP,
--                 CASE WHEN 
--                          WZA.MAXTEMP > 35 
--                      AND 
--                          LAG (WZA.MAXTEMP) OVER 
--                          (
--                              PARTITION BY
--                                  WZA.MODULESNDRM, 
--                                  WZA.WASHZONEID, 
--                                  WZA.POSITION 
--                              ORDER BY 
--                                  WZA.EVENTDATE
--                          ) > 35 
--                      AND
--                          LAG (WZA.MAXTEMP, 2) OVER 
--                          (
--                              PARTITION BY
--                                  WZA.MODULESNDRM, 
--                                  WZA.WASHZONEID, 
--                                  WZA.POSITION 
--                              ORDER BY 
--                                  WZA.EVENTDATE
--                          ) > 35 
--                      AND
--                          LAG (WZA.MAXTEMP, 3) OVER 
--                          (
--                              PARTITION BY
--                                  WZA.MODULESNDRM, 
--                                  WZA.WASHZONEID, 
--                                  WZA.POSITION 
--                              ORDER BY 
--                                  WZA.EVENTDATE
--                          ) > 35 
--                      AND
--                          LAG (WZA.MAXTEMP, 4) OVER 
--                          (
--                              PARTITION BY
--                                  WZA.MODULESNDRM, 
--                                  WZA.WASHZONEID, 
--                                  WZA.POSITION 
--                              ORDER BY 
--                                  WZA.EVENTDATE
--                          ) > 35
--                      THEN 
--                          'YES' 
--                      ELSE 
--                          'NO' 
--                      END AS FLAGA 
--             FROM ( 
--                 SELECT 
--                     WA.MODULESNDRM,
--                     WA.EVENTDATE,
--                     WA.WASHZONEID -1 AS WASHZONEID,
--                     '1' AS POSITION,
--                     WA.POSITION1 AS REPLICATEID,
--                     CASE WHEN 
--                              WA.POSITION1 = LAG (WA.POSITION1) OVER 
--                              (
--                                  ORDER BY 
--                                      WA.MODULESNDRM, 
--                                      WA.POSITION1, 
--                                      WA.WASHZONEID,
--                                      WA.EVENTDATE
--                              ) 
--                          AND 
--                              WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
--                              (
--                                  ORDER BY 
--                                      WA.MODULESNDRM, 
--                                      WA.POSITION1, 
--                                      WA.WASHZONEID, 
--                                      WA.EVENTDATE
--                              ) 
--                          AND 
--                              WA.EVENTDATE -10 /(24*60*60) < 
--                              LAG (WA.EVENTDATE) OVER 
--                              (
--                                  ORDER BY 
--                                      WA.MODULESNDRM, 
--                                      WA.POSITION1, 
--                                      WA.WASHZONEID,
--                                      WA.EVENTDATE
--                              )
--                          THEN 
--                              'Probe 1 Second Temp'
--                          ELSE 
--                              'Probe 1 First Temp' 
--                          END AS PIP_ORDER, 
--                     WA.MAXTEMPPOSITION1/1000 MAXTEMP
-- 
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WA 
--                 WHERE 
--                     WA.POSITION1 > 0 
--                 AND 
--                     WA.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND
--                     WA.EVENTDATE <= TRUNC(SYSDATE) 
--                 UNION ALL 
--                 SELECT 
--                     WA.MODULESNDRM,
--                     WA.EVENTDATE,
--                     WA.WASHZONEID -1 AS WASHZONEID,
--                     '2' AS POSITION,
--                     WA.POSITION2 AS REPLICATEID,
--                     'Probe 2' AS PIP_ORDER,
--                     WA.MAXTEMPPOSITION2/1000 MAXTEMP
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WA 
--                 WHERE 
--                     WA.POSITION2 > 0 
--                 AND 
--                     WA.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND
--                     WA.EVENTDATE <= TRUNC(SYSDATE) 
--                 UNION ALL 
--                 SELECT 
--                     WA.MODULESNDRM,
--                     WA.EVENTDATE,
--                     WA.WASHZONEID -1 AS WASHZONEID,
--                     '3' AS POSITION,
--                     WA.POSITION3 AS REPLICATEID,
--                     CASE WHEN 
--                              WA.POSITION3 = LAG (WA.POSITION3) OVER 
--                              (
--                                  ORDER BY 
--                                      WA.MODULESNDRM, 
--                                      WA.POSITION3, 
--                                      WA.WASHZONEID,
--                                      WA.EVENTDATE
--                              ) 
--                          AND 
--                              WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
--                              (
--                                  ORDER BY 
--                                      WA.MODULESNDRM, 
--                                      WA.POSITION3, 
--                                      WA.WASHZONEID,
--                                      WA.EVENTDATE
--                              ) 
--                          AND 
--                              WA.EVENTDATE -10 /(24*60*60) < LAG (WA.EVENTDATE) OVER 
--                              (
--                                  ORDER BY 
--                                      WA.MODULESNDRM, 
--                                      WA.POSITION3, 
--                                      WA.WASHZONEID,
--                                      WA.EVENTDATE
--                              )
--                          THEN 
--                              'Probe 3 Second Temp'
--                          ELSE 
--                              'Probe 3 First Temp' END 
--                          AS PIP_ORDER,
--                     WA.MAXTEMPPOSITION3/1000 MAXTEMP
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WA 
--                 WHERE 
--                     WA.POSITION3 > 0 
--                 AND 
--                     WA.EVENTDATE >= TRUNC(SYSDATE -1)
--                 AND
--                     WA.EVENTDATE <= TRUNC(SYSDATE)
--             ) WZA 
--             INNER JOIN 
--                 IDAOWNER.IDAMODULEINFORMATION IA 
--             ON 
--                 WZA.MODULESNDRM = IA.MODULESN 
--             AND 
--                 IA.CREATEDATE = 
--                 (
--                     SELECT 
--                         MAX(CREATEDATE) 
--                     from
--                         IDAOWNER.IDAMODULEINFORMATION 
--                     where 
--                         MODULESN = IA.MODULESN 
--                     AND 
--                         CREATEDATE <= SYSDATE)
--             AND
--                 WZA.EVENTDATE > IA.EFFECTIVEFROMDATE 
--             AND 
--                 WZA.EVENTDATE < IA.EFFECTIVETODATE 
--             WHERE 
--                 NOT WZA.PIP_ORDER = 'Probe 3 Second Temp' 
--             AND
--                 NOT WZA.PIP_ORDER = 'Probe 1 First Temp' 
--             AND
--                 IA.AREA IS NOT NULL 
--             AND 
--                 IA.CUSTOMERNAME NOT LIKE '%ABBOTT%' 
--             AND 
--                 IA.CUSTOMERNAME NOT LIKE '%Flextronics%' 
--         ) RAWA
--         WHERE 
--             RAWA.FLAGA = 'YES' 
--         GROUP BY 
--             RAWA.COUNTRY,
--             RAWA.CITY,
--             RAWA.CUSTOMER,
--             RAWA.SN, 
--             RAWA.WZPROBE 
--     ) A 
--     FULL JOIN (
--         SELECT 
--             RAWB.COUNTRY,
--             RAWB.CITY,
--             RAWB.CUSTOMER,
--             RAWB.SN,
--             RAWB.WZPROBE AS WZPROBEB,
--             MAX(RAWB.FLAGB) AS FLAGB,
--             MIN(RAWB.FLAGDATEB) AS FLAGDATEB
--         FROM (
--             SELECT 
--                 IB.AREA,
--                 IB.COUNTRYNAME COUNTRY,
--                 IB.CITY,
--                 substr(IB.CUSTOMERNAME,1,22) CUSTOMER,
--                 IB.CUSTOMERNAME CUSTOMER_NAME,
--                 WZB.MODULESNDRM AS SN,
--                 WZB.WASHZONEID || '.' || WZB.POSITION AS WZPROBE,
--                 WZB.EVENTDATE AS FLAGDATEB, 
--                 WZB.AMBIENTTEMP,
--                 CASE WHEN 
--                          WZB.AMBIENTTEMP < 14 
--                      AND 
--                          LAG (WZB.AMBIENTTEMP) OVER 
--                          (
--                              PARTITION BY
--                                  WZB.MODULESNDRM, 
--                                  WZB.WASHZONEID, 
--                                  WZB.POSITION 
--                              ORDER BY 
--                                  WZB.EVENTDATE
--                          ) < 14
--                      THEN 
--                          'YES' 
--                      ELSE 
--                          'NO' 
--                      END AS FLAGB 
--             FROM (
--                 SELECT 
--                     WB.MODULESNDRM,
--                     WB.EVENTDATE, 
--                     WB.WASHZONEID -1 AS WASHZONEID,
--                     '1' AS POSITION, 
--                     WB.POSITION1 AS REPLICATEID,
--                     CASE WHEN 
--                              WB.POSITION1 = LAG (WB.POSITION1) OVER 
--                              (
--                                  ORDER BY 
--                                      WB.MODULESNDRM, 
--                                      WB.POSITION1, 
--                                      WB.WASHZONEID,
--                                      WB.EVENTDATE
--                              ) 
--                          AND 
--                              WB.WASHZONEID = LAG (WB.WASHZONEID) OVER 
--                              (
--                                  ORDER BY 
--                                      WB.MODULESNDRM, 
--                                      WB.POSITION1, 
--                                      WB.WASHZONEID,
--                                      WB.EVENTDATE
--                              )
--                          AND 
--                              WB.EVENTDATE -10 /(24*60*60) < LAG (WB.EVENTDATE) OVER 
--                              (
--                                  ORDER BY 
--                                      WB.MODULESNDRM, 
--                                      WB.POSITION1, 
--                                      WB.WASHZONEID,
--                                      WB.EVENTDATE
--                              )
--                          THEN 
--                              'Probe 1 Second Temp'
--                          ELSE 
--                              'Probe 1 First Temp' 
--                          END AS PIP_ORDER,
--                     ( WB.MAXTEMPPOSITION1 - WB.TEMPDELTAPOSITION1)/1000 AS AMBIENTTEMP
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WB 
--                 WHERE 
--                     WB.POSITION1 > 0 
--                 AND 
--                     WB.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND
--                     WB.EVENTDATE <= TRUNC(SYSDATE) 
--                 UNION ALL 
--                 SELECT 
--                     WB.MODULESNDRM,
--                     WB.EVENTDATE,
--                     WB.WASHZONEID -1 AS WASHZONEID,
--                     '2' AS POSITION,
--                     WB.POSITION2 AS REPLICATEID,
--                     'Probe 2' AS PIP_ORDER,
--                     (WB.MAXTEMPPOSITION2 - WB.TEMPDELTAPOSITION2)/1000 AS AMBIENTTEMP
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WB 
--                 WHERE 
--                     WB.POSITION2 > 0 
--                 AND 
--                     WB.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND
--                     WB.EVENTDATE <= TRUNC(SYSDATE) 
--                 UNION ALL 
--                 SELECT 
--                     WB.MODULESNDRM, 
--                     WB.EVENTDATE,
--                     WB.WASHZONEID -1 AS WASHZONEID,
--                     '3' AS POSITION,
--                     WB.POSITION3 AS REPLICATEID,
--                     CASE WHEN 
--                              WB.POSITION3 = LAG (WB.POSITION3) OVER 
--                              (
--                                  ORDER BY 
--                                      WB.MODULESNDRM, 
--                                      WB.POSITION3, 
--                                      WB.WASHZONEID,
--                                      WB.EVENTDATE
--                              ) 
--                          AND 
--                              WB.WASHZONEID = LAG (WB.WASHZONEID) OVER 
--                              (
--                                  ORDER BY 
--                                      WB.MODULESNDRM, 
--                                      WB.POSITION3, 
--                                      WB.WASHZONEID,
--                                      WB.EVENTDATE
--                              ) 
--                          AND 
--                              WB.EVENTDATE -10 /(24*60*60) < LAG (WB.EVENTDATE) OVER 
--                              (
--                                  ORDER BY 
--                                      WB.MODULESNDRM, 
--                                      WB.POSITION3, 
--                                      WB.WASHZONEID,
--                                      WB.EVENTDATE
--                              )
--                         THEN 
--                             'Probe 3 Second Temp'
--                         ELSE 
--                             'Probe 3 First Temp' 
--                         END AS PIP_ORDER,
--                         (WB.MAXTEMPPOSITION3 - WB.TEMPDELTAPOSITION3)/1000 AS AMBIENTTEMP
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WB 
--                 WHERE 
--                     WB.POSITION3 > 0 
--                 AND 
--                     WB.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND
--                     WB.EVENTDATE <= TRUNC(SYSDATE)
--             ) WZB 
--         INNER JOIN 
--             IDAOWNER.IDAMODULEINFORMATION IB 
--         ON 
--             WZB.MODULESNDRM = IB.MODULESN 
--         AND 
--             IB.CREATEDATE = 
--             (
--                 SELECT 
--                     MAX(CREATEDATE) 
--                 from
--                     IDAOWNER.IDAMODULEINFORMATION 
--                 where 
--                     MODULESN = IB.MODULESN 
--                 AND 
--                     CREATEDATE <= SYSDATE
--             ) 
--         AND
--             WZB.EVENTDATE > IB.EFFECTIVEFROMDATE 
--         AND 
--             WZB.EVENTDATE < IB.EFFECTIVETODATE 
--         WHERE 
--             NOT WZB.PIP_ORDER = 'Probe 3 Second Temp' 
--         AND
--             NOT WZB.PIP_ORDER = 'Probe 1 First Temp' 
--         AND
--             IB.AREA IS NOT NULL 
--         AND 
--             IB.CUSTOMERNAME NOT LIKE '%ABBOTT%' 
--         AND 
--             IB.CUSTOMERNAME NOT LIKE '%Flextronics%' 
--         ) RAWB
--         WHERE 
--             RAWB.FLAGB = 'YES' 
--         GROUP BY 
--             RAWB.COUNTRY,
--             RAWB.CITY,
--             RAWB.CUSTOMER, 
--             RAWB.SN,
--             RAWB.WZPROBE 
--         ) B
--     ON 
--         A.COUNTRY = B.COUNTRY 
--     AND 
--         A.CITY = B.CITY 
--     AND 
--         A.CUSTOMER = B.CUSTOMER 
--     AND 
--         A.SN = B.SN 
--     AND 
--         A.WZPROBEA = B.WZPROBEB 
--     FULL JOIN (
--         SELECT 
--             RAWC.COUNTRY,
--             RAWC.CITY,
--             RAWC.CUSTOMER, 
--             RAWC.SN,
--             RAWC.WZPROBE AS WZPROBEC,
--             MAX(RAWC.FLAGC) AS FLAGC,
--             MIN(RAWC.FLAGDATEC) AS FLAGDATEC
--         FROM (
--             SELECT 
--                 IC.AREA,
--                 IC.COUNTRYNAME COUNTRY,
--                 IC.CITY,
--                 substr(IC.CUSTOMERNAME,1,22) CUSTOMER,
--                 IC.CUSTOMERNAME CUSTOMER_NAME,
--                 WZC.MODULESNDRM AS SN,
--                 WZC.WASHZONEID || '.' || WZC.POSITION AS WZPROBE,
--                 WZC.EVENTDATE AS FLAGDATEC,
--                 WZC.TEMPDELTA, 
--                 CASE WHEN 
--                          WZC.EVENTDATE -1/24 < LAG (WZC.EVENTDATE,19) OVER 
--                          (
--                              PARTITION BY 
--                                  WZC.MODULESNDRM, 
--                                  WZC.WASHZONEID, 
--                                  WZC.POSITION
-- 
--                              ORDER BY 
--                                  WZC.EVENTDATE
--                          ) 
--                      THEN 
--                          'YES' 
--                      ELSE 
--                          'NO' 
--                      END AS FLAGC 
--             FROM ( 
--                 SELECT 
--                     WC.MODULESNDRM, 
--                     WC.EVENTDATE,
--                     WC.WASHZONEID -1 AS WASHZONEID,
--                     '1' AS POSITION,
--                     WC.POSITION1 AS REPLICATEID,
--                     CASE WHEN 
--                              WC.POSITION1 = LAG (WC.POSITION1) OVER 
--                              (
--                                  ORDER BY 
--                                      WC.MODULESNDRM, 
--                                      WC.POSITION1, 
--                                      WC.WASHZONEID,
--                                      WC.EVENTDATE
--                              ) 
--                          AND 
--                              WC.WASHZONEID = LAG (WC.WASHZONEID) OVER 
--                              (
--                                  ORDER BY 
--                                      WC.MODULESNDRM, 
--                                      WC.POSITION1, 
--                                      WC.WASHZONEID,
--                                      WC.EVENTDATE
--                              )
--                          AND 
--                              WC.EVENTDATE -10 /(24*60*60) < LAG (WC.EVENTDATE) OVER 
--                              (
--                                  ORDER BY 
--                                      WC.MODULESNDRM, 
--                                      WC.POSITION1, 
--                                      WC.WASHZONEID,
--                                      WC.EVENTDATE
--                              )
--                         THEN 
--                             'Probe 1 Second Temp'
--                         ELSE 
--                             'Probe 1 First Temp' 
--                         END AS PIP_ORDER,
--                     WC.TEMPDELTAPOSITION1/1000 TEMPDELTA
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WC 
--                 WHERE 
--                     WC.POSITION1 > 0 
--                 AND 
--                     WC.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND
--                     WC.EVENTDATE <= TRUNC(SYSDATE) 
--                 UNION ALL 
--                 SELECT 
--                     WC.MODULESNDRM,
--                     WC.EVENTDATE, 
--                     WC.WASHZONEID -1 AS WASHZONEID,
--                     '2' AS POSITION,
--                     WC.POSITION2 AS REPLICATEID,
--                     'Probe 2' AS PIP_ORDER, 
--                     WC.TEMPDELTAPOSITION2/1000 TEMPDELTA
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WC 
-- 
--                 WHERE 
--                     WC.POSITION2 > 0 
--                 AND 
--                     WC.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND 
--                     WC.EVENTDATE <= TRUNC(SYSDATE) 
--                 UNION ALL 
--                 SELECT 
--                     WC.MODULESNDRM,
--                     WC.EVENTDATE,
--                     WC.WASHZONEID -1 AS WASHZONEID,
--                     '3' AS POSITION, 
--                     WC.POSITION3 AS REPLICATEID,
--                     CASE WHEN 
--                              WC.POSITION3 = LAG (WC.POSITION3) OVER 
--                              (
--                                  ORDER BY 
--                                      WC.MODULESNDRM, 
--                                      WC.POSITION3, 
--                                      WC.WASHZONEID, 
--                                      WC.EVENTDATE
--                              ) 
--                          AND 
--                              WC.WASHZONEID = LAG (WC.WASHZONEID) OVER 
--                              (
--                                  ORDER BY 
--                                      WC.MODULESNDRM, 
--                                      WC.POSITION3, 
--                                      WC.WASHZONEID,
--                                      WC.EVENTDATE
--                              ) 
--                          AND 
--                              WC.EVENTDATE -10 /(24*60*60) < LAG (WC.EVENTDATE) OVER 
--                              (
--                                  ORDER BY 
--                                      WC.MODULESNDRM, 
--                                      WC.POSITION3, 
--                                      WC.WASHZONEID, 
--                                      WC.EVENTDATE
--                              )
--                         THEN 
--                             'Probe 3 Second Temp'
--                         ELSE 
--                             'Probe 3 First Temp' 
--                         END AS PIP_ORDER,
--                         WC.TEMPDELTAPOSITION3/1000 TEMPDELTA 
--                 FROM 
--                     IDAOWNER.WASHASPIRATIONS WC 
--                 WHERE 
--                     WC.POSITION3 > 0 
--                 AND 
--                     WC.EVENTDATE >= TRUNC(SYSDATE -1) 
--                 AND
--                     WC.EVENTDATE <= TRUNC(SYSDATE)
--             ) WZC 
--             INNER JOIN 
--                 IDAOWNER.IDAMODULEINFORMATION IC 
--             ON 
--                 WZC.MODULESNDRM = IC.MODULESN 
--             AND 
--                 IC.CREATEDATE = (
--                     SELECT 
--                         MAX(CREATEDATE) 
--                     from
--                         IDAOWNER.IDAMODULEINFORMATION 
--                     where 
--                         MODULESN = IC.MODULESN 
--                     AND 
--                         CREATEDATE <= SYSDATE
--                 ) 
--             AND 
--                 WZC.EVENTDATE > IC.EFFECTIVEFROMDATE 
--             AND 
--                 WZC.EVENTDATE < IC.EFFECTIVETODATE 
--             WHERE 
--                 NOT WZC.PIP_ORDER = 'Probe 3 Second Temp' 
--             AND 
--                 NOT WZC.PIP_ORDER = 'Probe 1 First Temp' 
--             AND
--                 IC.AREA IS NOT NULL 
--             AND 
--                 WZC.TEMPDELTA < 3 
--             AND 
--                 IC.CUSTOMERNAME NOT LIKE '%ABBOTT%' 
--             AND 
--                 IC.CUSTOMERNAME NOT LIKE '%Flextronics%' 
--         ) RAWC
--     WHERE 
--         RAWC.FLAGC = 'YES' 
--     GROUP BY 
--         RAWC.COUNTRY, 
--         RAWC.CITY,
--         RAWC.CUSTOMER,
--         RAWC.SN,
--         RAWC.WZPROBE 
--     ) C
--     ON 
--         A.COUNTRY = C.COUNTRY 
--     AND 
--         A.CITY = C.CITY 
--     AND 
--         A.CUSTOMER = C.CUSTOMER 
--     AND 
--         A.SN = C.SN 
--     AND 
--         A.WZPROBEA = C.WZPROBEC 
--     GROUP BY 
--         A.COUNTRY,
--         B.COUNTRY,
--         C.COUNTRY, 
--         A.CITY,
--         B.CITY,
--         C.CITY,
--         A.CUSTOMER,
--         B.CUSTOMER,
--         C.CUSTOMER,
--         A.SN,
--         B.SN,
--         C.SN, 
--         A.WZPROBEA,
--         B.WZPROBEB,
--         C.WZPROBEC
--     ) ABC
-- INNER JOIN
--     IDAOWNER.IDAMODULES IDAM
-- ON
--     ABC.SN = IDAM.MODULESN
-- GROUP BY 
--     ABC.COUNTRY,
--     ABC.CITY,
--     ABC.CUSTOMER,
--     ABC.SN,
--     ABC.WZPROBE,
--     ABC.FLAGA, 
--     ABC.FLAGB,
--     ABC.FLAGC,
--     IDAM.PRODUCTLINE

-- select 
--     wa.deviceid,
--     wa.eventdate,
--     wa.fileid,
--     wa.loaddate,
--     wa.maxtempposition1,
--     wa.maxtempposition2,
--     wa.maxtempposition3,
--     wa.moduleid,
--     wa.modulesndrm,
--     wa.position1,
--     wa.position2,
--     wa.position3,
--     wa.tempdeltaposition1,
--     wa.tempdeltaposition2,
--     wa.tempdeltaposition3,
--     wa.washzoneid
-- from
--     idaowner.washaspirations wa
-- where
--     (sysdate - 1) < wa.eventdate

SELECT 
    RAWC.SN AS MODULESN,
    -- RAWC.WZPROBE AS WZPROBEC,
    IDAM.PRODUCTLINE AS PL,
    MAX(RAWC.FLAGC) AS FLAGC,
    TO_CHAR(MIN(RAWC.FLAGDATEC), 'YYYYMMDDHH24MISS') AS FLAG_DATE
FROM (
    SELECT 
        WZC.MODULESNDRM AS SN,
        WZC.WASHZONEID || '.' || WZC.POSITION AS WZPROBE,
        WZC.EVENTDATE AS FLAGDATEC,
        WZC.TEMPDELTA, 
        CASE WHEN 
                 WZC.EVENTDATE -1/24 < LAG (WZC.EVENTDATE,19) OVER 
                 (
                     PARTITION BY 
                         WZC.MODULESNDRM, 
                         WZC.WASHZONEID, 
                         WZC.POSITION
-- 
                     ORDER BY 
                         WZC.EVENTDATE
                 ) 
             THEN 
                 'YES' 
             ELSE 
                 'NO' 
             END AS FLAGC 
    FROM ( 
        SELECT 
            WC.MODULESNDRM, 
            WC.EVENTDATE,
            WC.WASHZONEID -1 AS WASHZONEID,
            '1' AS POSITION,
            WC.POSITION1 AS REPLICATEID,
            CASE WHEN 
                     WC.POSITION1 = LAG (WC.POSITION1) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION1, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     ) 
                 AND 
                     WC.WASHZONEID = LAG (WC.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION1, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     )
                 AND 
                     WC.EVENTDATE -10 /(24*60*60) < LAG (WC.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION1, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     )
                THEN 
                    'Probe 1 Second Temp'
                ELSE 
                    'Probe 1 First Temp' 
                END AS PIP_ORDER,
            WC.TEMPDELTAPOSITION1/1000 TEMPDELTA
        FROM 
            IDAOWNER.WASHASPIRATIONS WC 
        WHERE 
            WC.POSITION1 > 0 
        AND 
            WC.EVENTDATE >= TRUNC(SYSDATE -1) 
        AND
            WC.EVENTDATE <= TRUNC(SYSDATE) 
        UNION ALL 
        SELECT 
            WC.MODULESNDRM,
            WC.EVENTDATE, 
            WC.WASHZONEID -1 AS WASHZONEID,
            '2' AS POSITION,
            WC.POSITION2 AS REPLICATEID,
            'Probe 2' AS PIP_ORDER, 
            WC.TEMPDELTAPOSITION2/1000 TEMPDELTA
        FROM 
            IDAOWNER.WASHASPIRATIONS WC 
        WHERE 
            WC.POSITION2 > 0 
        AND 
            WC.EVENTDATE >= TRUNC(SYSDATE -1) 
        AND 
            WC.EVENTDATE <= TRUNC(SYSDATE) 
        UNION ALL 
        SELECT 
            WC.MODULESNDRM,
            WC.EVENTDATE,
            WC.WASHZONEID -1 AS WASHZONEID,
            '3' AS POSITION, 
            WC.POSITION3 AS REPLICATEID,
            CASE WHEN 
                     WC.POSITION3 = LAG (WC.POSITION3) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION3, 
                             WC.WASHZONEID, 
                             WC.EVENTDATE
                     ) 
                 AND 
                     WC.WASHZONEID = LAG (WC.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION3, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     ) 
                 AND 
                     WC.EVENTDATE -10 /(24*60*60) < LAG (WC.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION3, 
                             WC.WASHZONEID, 
                             WC.EVENTDATE
                     )
                THEN 
                    'Probe 3 Second Temp'
                ELSE 
                    'Probe 3 First Temp' 
                END AS PIP_ORDER,
                WC.TEMPDELTAPOSITION3/1000 TEMPDELTA 
        FROM 
            IDAOWNER.WASHASPIRATIONS WC 
        WHERE 
            WC.POSITION3 > 0 
        AND 
            WC.EVENTDATE >= TRUNC(SYSDATE -1) 
        AND
            WC.EVENTDATE <= TRUNC(SYSDATE)
    ) WZC 
    WHERE 
        NOT WZC.PIP_ORDER = 'Probe 3 Second Temp' 
    AND 
        NOT WZC.PIP_ORDER = 'Probe 1 First Temp' 
    AND 
        WZC.TEMPDELTA < 3 
) RAWC
INNER JOIN
    IDAOWNER.IDAMODULES IDAM
ON
    RAWC.SN = IDAM.MODULESN
WHERE 
    RAWC.FLAGC = 'YES' 
GROUP BY 
    RAWC.SN, 
    -- RAWC.WZPROBE,
    IDAM.PRODUCTLINE