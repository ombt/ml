#
# Alinity IA Washzone PX Aspiration
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
    eval.moduleserialnumber as modulesn,
    date_format(eval.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval.ratiodisptl,
    eval.numtotaldisp,
    case when ((eval.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>) and 
               (eval.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>))
    then 1
    else 0
    end as flagged
from (
    select
        w.moduleserialnumber,
        max(w.datetimestamplocal) as flag_date,
        count(w.emptycount) as numtotaldisp,
       (cast (sum(case when (w.emptycount - w.emptytolerance) <= <I_WZASP_THRESHOLD_NUMDISPTCTT>
                 then 1 
                 else 0 
                 end) as double)) / (cast (count(w.emptycount) as double))
            as ratiodisptl
    from
        dx.dx_205_alinity_i_wamdata w
    where
        '<START_DATE>' <= w.transaction_date
    and 
        w.transaction_date < '<END_DATE>'
    and 
        w.washzone = <I_WZASP_THRESHOLD_WZ>
    and 
        w.position = <I_WZASP_THRESHOLD_POS>
    group by
        w.moduleserialnumber
    ) eval"
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
