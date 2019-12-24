#
# Alinity IA FE Pressure
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
    evals.modulesn,
    date_format(evals.flag_date,'%Y%m%d%%H%i%s') as flag_date,
    evals.mechname,
    evals.aspirations,
    evals.numflags,
    case when ((evals.aspirations >= <ASPS>) and
               ((cast (evals.numflags as double) / 
                cast (evals.aspirations as double) ) >= <PCTASPS>))
    then 1
    else 0
    end as flagged
from ( 
    select
        pm.moduleserialnumber as modulesn,
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
        pm.moduleserialnumber,
        pm.pipettormechanismname
    ) evals
order by
    evals.modulesn"
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
