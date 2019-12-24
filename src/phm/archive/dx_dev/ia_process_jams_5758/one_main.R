#
# Alinity IA Process Path Jams 5758
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
    eval1.moduleserialnumber as modulesn,
    date_format(eval2.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval1.num_retries,
    eval2.num_results,
    case when ((eval2.num_results >= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>) and 
               (eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES>))
    then 1
    else 0
    end as flagged
from (
    select
        m.moduleserialnumber,
        count(m.moduleserialnumber) as num_retries
    from
        dx.dx_205_alinity_i_messagehistory m
    where
        '<START_DATE>' <= m.transaction_date
    and 
        m.transaction_date < '<END_DATE>'
    and 
        m.aimcode = <PROCPATHJAMS_THRESHOLD_AIMCODE>
    and 
        m.aimsubcode = '<PROCPATHJAMS_THRESHOLD_AIMSUBCODE>'
    group by
        m.moduleserialnumber
    ) eval1
inner join (
    select
        r.moduleserialnumber,
        max(r.datetimestamplocal) as flag_date,
        count(r.correctedcount) as num_results
    from
        dx.dx_205_alinity_i_result r
    where
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    and 
        r.correctedcount is not null
    group by
        r.moduleserialnumber
    ) eval2
on 
    eval1.moduleserialnumber = eval2.moduleserialnumber
order by
    eval1.moduleserialnumber"
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
