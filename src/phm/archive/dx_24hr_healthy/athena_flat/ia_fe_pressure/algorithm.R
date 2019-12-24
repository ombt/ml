#
# Alinity IA FE Pressure
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    evals.modulesn,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date,
    evals.mechname,
    evals.aspirations,
    evals.numflags,
    100.0 * (cast (evals.numflags as double) / 
             cast (evals.aspirations as double)) as pct_asps
from ( 
    select
        upper(trim(pm.moduleserialnumber)) as modulesn,
        pm.pipettormechanismname as mechname,
        max(pm.datetimestamplocal) as flag_date,
        count(pm.pipettormechanismname) as aspirations,
        sum(case when pm.frontendpressure > <MAX_VALUE> or 
                      pm.frontendpressure < <MIN_VALUE>
                 then 1
                 else 0
            end) as numflags
    from
        dx.dx_205_alinity_i_pmevent pm
    where
        '<START_DATE>' <= pm.transaction_date
    and 
        pm.transaction_date < '<END_DATE>'
    and 
        pm.frontendpressure is not null
    and 
        pm.pipettingprotocolname != 'NonPipettingProtocol'
    and 
        pm.pipettormechanismname = '<PIPMECHNAME>'
    group by
        upper(trim(pm.moduleserialnumber)),
        pm.pipettormechanismname
    ) evals
where (
    evals.aspirations >= <ASPS>
and
    (cast (evals.numflags as double) / 
     cast (evals.aspirations as double) ) >= <PCTASPS>
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
use_suppression <- TRUE
#
chart_data_query_template <- "
select
    evals.modulesn,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date,
    evals.mechname,
    evals.aspirations,
    evals.numflags,
    100.0 * (cast (evals.numflags as double) / 
             cast (evals.aspirations as double)) as chart_data_value
from ( 
    select
        upper(trim(pm.moduleserialnumber)) as modulesn,
        pm.pipettormechanismname as mechname,
        max(pm.datetimestamplocal) as flag_date,
        count(pm.pipettormechanismname) as aspirations,
        sum(case when pm.frontendpressure > <MAX_VALUE> or 
                      pm.frontendpressure < <MIN_VALUE>
                 then 1
                 else 0
            end) as numflags
    from
        dx.dx_205_alinity_i_pmevent pm
    where
        '<START_DATE>' <= pm.transaction_date
    and 
        pm.transaction_date < '<END_DATE>'
    and 
        pm.frontendpressure is not null
    and 
        pm.pipettingprotocolname != 'NonPipettingProtocol'
    and 
        pm.pipettormechanismname = '<PIPMECHNAME>'
    group by
        upper(trim(pm.moduleserialnumber)),
        pm.pipettormechanismname
    ) evals
where
    evals.aspirations >= <ASPS>"
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
    flagged_results$CHART_DATA_VALUE <- flagged_results$PCT_ASPS
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
        AND TP.LIST_NUM LIKE '7-204217%'
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}
