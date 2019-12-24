#
# Alinity IA Optics Dark Count
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    upper(trim(dxr.moduleserialnumber)) as modulesn,
    dxr.productline as pl,
    date_format(max(dxr.datetimestamplocal),'yyyyMMddHHmmss') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx_205_alinity_i_result dxr
where
    to_date('<START_DATE>', 'yyyy-MM-dd') <= dxr.datetimestamplocal
and 
    dxr.datetimestamplocal < to_date('<END_DATE>', 'yyyy-MM-dd')
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(trim(dxr.moduleserialnumber)) like 'AI%'
group by
    upper(trim(dxr.moduleserialnumber)),
    dxr.productline
having
    count(dxr.testid) >= <TESTID>
and
    max(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_MAX>
and
    stddev(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_SD>"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn,
    dxr.productline as pl
from
    dx_205_alinity_i_result dxr
where
    dxr.moduleserialnumber is not null
and
    to_date('<MODULESN_START_DATE>', 'yyyy-MM-dd') <= dxr.datetimestamplocal
and 
    dxr.datetimestamplocal < to_date('<MODULESN_END_DATE>', 'yyyy-MM-dd')"
#
use_suppression <- TRUE
#
chart_data_query_template <- "
select
    upper(trim(dxr.moduleserialnumber)) as modulesn,
    dxr.productline as pl,
    date_format(max(dxr.datetimestamplocal),'yyyyMMddHHmmss') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as chart_data_value,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx_205_alinity_i_result dxr
where
    to_date('<START_DATE>', 'yyyy-MM-dd') <= dxr.datetimestamplocal
and 
    dxr.datetimestamplocal < to_date('<END_DATE>', 'yyyy-MM-dd')
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(trim(dxr.moduleserialnumber)) like 'AI%'
group by
    upper(trim(dxr.moduleserialnumber)),
    dxr.productline
having
    count(dxr.testid) >= 1"
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
config_type <- "spark"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$MAX_IDC
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
INNER JOIN TICKETWORKDONE TWD
  ON TH.TICKET_SQ = TWD.TICKET_SQ
WHERE 
  TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '205'
        AND (TWD.WORKDONE_CODE LIKE 'FA3%' 
             OR TWD.WORKDONE_CODE LIKE 'FAG%'
             OR TWD.WORKDONE_CODE LIKE 'FB3%'
             OR TWD.WORKDONE_CODE LIKE 'FBG%')
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}
#
#
spark_load_data <- function(db_conn,
                            param_sets, 
                            test_period)
{
    library(DBI)
    #
    results_tbl <- "dx_205_alinity_i_result"
    results_uri_template <- "s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/205-alinity-i/Result/transaction_date=<START_DATE>"
    results_uri <- query_subs(results_uri_template, test_period, "VALUE")
    #
    read_in <- spark_read_parquet(db_conn, 
                                  results_tbl, 
                                  results_uri)
}


