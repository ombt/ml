#
# Alinity IA Optics Dark Count
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    dxr.moduleserialnumber as modulesn,
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
    upper(dxr.moduleserialnumber) like 'AI%'
group by
    dxr.moduleserialnumber
having
    count(dxr.testid) >= <TESTID>
and
    max(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_MAX>
and
    stddev(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_SD>
order by
    dxr.moduleserialnumber"
#
modulesn_query_template <- "
select
    distinct(dxr.moduleserialnumber) as modulesn
from
    dx_205_alinity_i_result dxr
where
    to_date('<MODULESN_START_DATE>', 'yyyy-MM-dd') <= dxr.datetimestamplocal
and 
    dxr.datetimestamplocal < to_date('<MODULESN_END_DATE>', 'yyyy-MM-dd')"
#
reliability_query_template <- NA
#
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

