# Alinity IA Vacuum Sensor
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
common_utils_path <- file.path(".", "common_utils.R")
if ( ! file.exists(common_utils_path)) {
    if (nchar(Sys.getenv("DEV_ROOT")) == 0) {
        stop("No 'common_utils.R' found")
    }
    common_utils_path <- file.path(Sys.getenv("DEV_ROOT"),
                                   "rlib",
                                   "common_utils.R")
    if ( ! file.exists(common_utils_path)) {
        stop("No DEV_ROOT 'common_utils.R' found")
    }
}
source(common_utils_path)
#
#####################################################################
#
# algorithm specific
#
one_query_template <- "
select
    v.moduleserialnumber as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings,
    case when ((count(v.adcvalue) >= <I_VACUUM_NUMREADINGS_MIN>) and 
               (avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>))
    then 1
    else 0
    end as flagged
from 
    dx.dx_205_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    v.moduleserialnumber
order by
    v.moduleserialnumber"
#
post_processing <- function(results,
                            params, 
                            db_conn, 
                            query, 
                            options, 
                            test_period, 
                            flagged)
{
    return(flagged_post_processing(results, 
                                   ifelse(results$FLAGGED, 
                                          TRUE, 
                                          FALSE)))
}
#
#####################################################################
#
# start algorithm
#
main(1, one_query_template, FALSE, "205", "spark")
#
q(status=0)
