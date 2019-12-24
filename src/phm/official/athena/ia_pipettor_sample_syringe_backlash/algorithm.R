#
# Alinity IA Pipettor Sample Syringe Backlash
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select 
    final.moduleserialnumber as modulesn,
    date_format(final.flag_date,'%Y%m%d%H%i%s') as flag_date,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner1.moduleserialnumber,
        max(inner1.datetimestamplocal) as flag_date,
        count(inner1.moduleserialnumber) as num_tests,
        avg(cast (inner1.backlash as double)) as avg_backlash
    from (
        select
            upper(trim(ia.moduleserialnumber)) as moduleserialnumber,
            ia.datetimestamplocal,
            regexp_extract(ia.activity,'^.*PosDiff: *(\\d+).*$',1) as backlash
        from 
            dx.dx_205_alinity_i_instrumentactivity ia
        where
            '<START_DATE>' <= ia.transaction_date
        and 
            ia.transaction_date < '<END_DATE>'
        and 
            ia.activity like '<I_POSDIFF_THRESHOLD_ACTIVITY>'
        ) inner1
    group by
        inner1.moduleserialnumber
    ) final
where
    final.avg_backlash > <I_POSDIFF_THRESHOLD_MAX>
and 
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>"
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
use_suppression <- TRUE
#
chart_data_query_template <- "
select 
    final.moduleserialnumber as modulesn,
    date_format(final.flag_date,'%Y%m%d%H%i%s') as flag_date,
    final.num_tests,
    final.avg_backlash as chart_data_value
from (
    select
        inner1.moduleserialnumber,
        max(inner1.datetimestamplocal) as flag_date,
        count(inner1.moduleserialnumber) as num_tests,
        avg(cast (inner1.backlash as double)) as avg_backlash
    from (
        select
            upper(trim(ia.moduleserialnumber)) as moduleserialnumber,
            ia.datetimestamplocal,
            regexp_extract(ia.activity,'^.*PosDiff: *(\\d+).*$',1) as backlash
        from 
            dx.dx_205_alinity_i_instrumentactivity ia
        where
            '<START_DATE>' <= ia.transaction_date
        and 
            ia.transaction_date < '<END_DATE>'
        and 
            ia.activity like '<I_POSDIFF_THRESHOLD_ACTIVITY>'
        ) inner1
    group by
        inner1.moduleserialnumber
    ) final
where
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>"
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
    flagged_results$CHART_DATA_VALUE <- flagged_results$AVG_BACKLASH
    #
    return(flagged_results)
}
#
generate_suppression <- function(params, 
                                 rel_db_con, 
                                 test_period)
{
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
        AND TP.LIST_NUM LIKE '7-77650%'
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}
