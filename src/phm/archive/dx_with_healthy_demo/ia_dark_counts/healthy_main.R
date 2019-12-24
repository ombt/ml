#
# Alinity IA Optics Dark Count
#
#####################################################################
#
# required libraries
#
library(getopt)
library(DBI)
library(RJDBC)
library(dplyr)
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source libs
#
common_utils_path <- file.path(".", "healthy_common_utils.R")
if ( ! file.exists(common_utils_path)) {
    if (nchar(Sys.getenv("DEV_ROOT")) == 0) {
        stop("No 'healthy_common_utils.R' found")
    }
    common_utils_path <- file.path(Sys.getenv("DEV_ROOT"),
                                   "rlib",
                                   "healthy_common_utils.R")
    if ( ! file.exists(common_utils_path)) {
        stop("No DEV_ROOT 'healthy_common_utils.R' found")
    }
}
source(common_utils_path)
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select
    dxr.moduleserialnumber as modulesn,
    date_format(max(dxr.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc,
    case when ((count(dxr.testid) >= <TESTID>) and
               (max(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_MAX>) and
               (stddev(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_SD>))
    then 1
    else 0
    end as flagged
from
    dx.dx_205_alinity_i_result dxr
where
    '<START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<END_DATE>'
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(dxr.moduleserialnumber) like 'AI%'
group by
    dxr.moduleserialnumber
order by
    dxr.moduleserialnumber"
#
sample_count_query_template <- "
select
    dxr.moduleserialnumber as modulesn,
    count(dxr.testid) as sample_count,
    case when (count(dxr.testid) >= <MINIMUM_SAMPLE_COUNT>)
    then 1
    else 0
    end as enough_counts
from
    dx.dx_205_alinity_i_result dxr
where
    '<START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<END_DATE>'
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(dxr.moduleserialnumber) like 'AI%'
group by
    dxr.moduleserialnumber
order by
    dxr.moduleserialnumber"
#
master_list_query_template <- "
select
    distinct(dxr.moduleserialnumber) as modulesn
from
    dx.dx_205_alinity_i_result dxr
where
    '<MASTER_LIST_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MASTER_LIST_END_DATE>'
and
    upper(dxr.moduleserialnumber) like 'AI%'
order by
    dxr.moduleserialnumber"
#
post_processing <- function(flagged_results,
                            params, 
                            db_conn, 
                            query, 
                            options, 
                            test_period, 
                            flagged)
{
    return(flagged_post_processing(flagged_results, 
                                   ifelse(flagged_results$FLAGGED, 
                                          TRUE, 
                                          FALSE)))
}
#
#####################################################################
#
# start algorithm
#
main(1, 
     flagged_query_template, 
     sample_count_query_template, 
     master_list_query_template,
     FALSE, 
     "205")
#
q(status=0)
