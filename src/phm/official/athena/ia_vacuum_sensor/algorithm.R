# Alinity IA Vacuum Sensor
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    upper(trim(v.moduleserialnumber)) as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_alinity_i_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    upper(trim(v.moduleserialnumber))
having (
    count(v.adcvalue) >= 3
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
from
    dx.dx_205_alinity_i_result dxr
where
    dxr.moduleserialnumber is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
chart_data_query_template <- "
select
    upper(trim(v.moduleserialnumber)) as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    avg(v.adcvalue) as chart_data_value,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_alinity_i_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    upper(trim(v.moduleserialnumber))
having 
    count(v.adcvalue) >= 3"
#
# number of days to check
#
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- "205"
#
# configuration type, athena or spark
#
config_type <- "dx"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$MEAN_ADC
    #
    return(flagged_results)
}
#
use_suppression <- TRUE
#
generate_suppression <- function(params, 
                                 rel_db_con, 
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
    #
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
    return(exec_query(params, rel_db_con, query, test_period))
}
