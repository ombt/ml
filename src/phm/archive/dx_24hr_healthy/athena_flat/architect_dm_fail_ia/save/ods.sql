SELECT 
          MA.FILEID,
          MA.DEVICEID,
          MA.MODULEID,
          MA.MODULESNDRM,
          MA.COMPLETIONDATE,
          CASE WHEN (
                                MA.RESULT LIKE 'Completed' OR 
                                MA.RESULT LIKE 'Completata' OR
                                MA.RESULT LIKE 'Finalizado' OR
                                MA.RESULT LIKE 'Terminé' OR
                                MA.RESULT LIKE 'Abgeschlossen' OR
                                MA.RESULT LIKE 'Concluído' OR
                                MA.RESULT LIKE 'Dokonceno' OR
                                MA.RESULT LIKE '??????' OR
                                MA.RESULT LIKE '??' OR 
                                MA.RESULT LIKE '???') 
                                THEN 'Complete' 
                              WHEN (
                                MA.RESULT LIKE 'Failed' OR
                                MA.RESULT LIKE '??' OR
                                MA.RESULT LIKE '???' OR
                                MA.RESULT LIKE 'Fallita' OR
                                MA.RESULT LIKE 'Fallido' OR
                                MA.RESULT LIKE 'Echoué' OR
                                MA.RESULT LIKE 'Fehlgeschlagen' OR
                                MA.RESULT LIKE 'Falhado' OR
                                MA.RESULT LIKE 'Chyba' OR
                                MA.RESULT LIKE '??????')
                                  THEN 'Failed'
                              WHEN (
                                MA.RESULT LIKE 'User canceled' OR
                                MA.RESULT LIKE '????' OR
                                MA.RESULT LIKE '???????????' OR
                                MA.RESULT LIKE 'Annullata' OR
                                MA.RESULT LIKE 'Cancel usuario' OR
                                MA.RESULT LIKE 'Cancelado utili.' OR
                                MA.RESULT LIKE 'Annulé par utilis.' OR
                                MA.RESULT LIKE 'Benutzerabbruch' OR
                                MA.RESULT LIKE 'Zru' OR
                                MA.RESULT LIKE 'Zrueno uivatelem' OR
                                MA.RESULT LIKE '??????')
                                THEN 'Failed'
                                ELSE NULL END AS RESULT_TR
        FROM
          IDAOWNER.MAINTENANCELOGS MA
        WHERE
          MA."PROCEDURE" LIKE '%6041%' AND
          MA.COMPLETIONDATE >= SYSDATE - 60 AND
          MA.FILEID IN (SELECT FILEID FROM IDAOWNER.IDALOGFILES WHERE LOADENDTIME BETWEEN V_START_DATE AND V_END_DATE)
 
        ORDER BY MA.MODULESNDRM DESC, MA.COMPLETIONDATE DESC)
