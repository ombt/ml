#
# Alinity IA Vacuum Pump
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
    evals.moduleserialnumber as modulesn,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date,
    evals.num_evals,
    evals.min_adcvalue,
    case when ((evals.min_adcvalue > <VACUUMPUMP_MINADC>) and 
               (evals.num_evals >= <VACUUMPUMP_NUMEVALS>))
    then 1
    else 0
    end as flagged
from (
    select
        rawdata.moduleserialnumber,
        max(rawdata.datetimestamplocal) as flag_date,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.moduleserialnumber,
            vpd.datetimestamplocal,
            vpd.adcvalue
        from
            dx.dx_205_alinity_i_vacuumpressuredata vpd 
        where  
            '<START_DATE>' <= vpd.transaction_date
        and 
            vpd.transaction_date < '<END_DATE>'
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.moduleserialnumber
    ) evals
order by
    evals.moduleserialnumber"
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
main(1, one_query_template, FALSE, "205")
#
q(status=0)
