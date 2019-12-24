use_suppression <- TRUE
#
generate_suppression <- function(params, 
                                 rel_db_con, 
                                 options,
                                 test_period)
{
#     query <- "
# select
#     distinct(work.sn) as modulesn
# from (
# SELECT
#   DISTINCT(UPPER(CALCULATEDSN)) AS SN
# FROM
#   TICKETHEADER TH
# INNER JOIN TICKETWORKDONE TWD
#   ON TH.TICKET_SQ = TWD.TICKET_SQ
# WHERE 
#   TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
#     AND TH.BESTPL = '205'
#         AND TWD.WORKDONE_CODE LIKE 'CW%' 
# UNION ALL
# SELECT
#   DISTINCT(UPPER(CALCULATEDSN)) AS SN
# FROM
#   TICKETHEADER TH
# INNER JOIN TICKETPRODUCT TP
#   ON TH.TICKET_SQ = TP.TICKET_SQ
# WHERE 
#   TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
#     AND TH.BESTPL = '205'
#       AND TP.ACTION_TAKEN IN ('N110', 'N120')
#         AND TP.LIST_NUM LIKE ' A-30104916%'  
# ) work
# "
    query <- "
SELECT
  DISTINCT(UPPER(CALCULATEDSN)) AS MODULESN
FROM
  TICKETHEADER TH
INNER JOIN TICKETPRODUCT TP
  ON TH.TICKET_SQ = TP.TICKET_SQ
WHERE 
  TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '205'
      AND TP.ACTION_TAKEN IN ('N110', 'N120')
        AND TP.LIST_NUM LIKE ' A-30104916%'  
"
    #
    return(exec_query(params, rel_db_con, query, options, test_period))
}
