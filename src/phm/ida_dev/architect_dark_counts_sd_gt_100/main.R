#
# Architect Dark Count Standard Deviation Exceeded
#
#####################################################################
#
# required libraries
#
library(getopt)
library(DBI)
library(RJDBC)
library(dplyr)
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
flagged_query_template <- "
select 
    rawdata.modulesn,
    max(rawdata.max_completion_date) as flag_date,
    1 as flagged
from (
    select
        r.modulesndrm as modulesn,
        trunc (r.completiondate) as test_completion_date, 
        max(r.completiondate) as max_completion_date,
        stddev (r.darkcount) as std_dev_dark_count, 
        avg (to_number (r.darkcount)) as average_dark_count
    from
        idaowner.results_ia r
    where
        r.darkcount is not null 
    and
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= r.completiondate
    and 
        r.completiondate < to_timestamp('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
    group by 
        r.modulesndrm,
        trunc(r.completiondate) 
    ) rawdata
where
    rawdata.std_dev_dark_count >= <THRESHOLD_NUMBER>
group by
    rawdata.modulesn
having
    count(rawdata.modulesn) >= <THRESHOLD_NUMBER_UNIT>"
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
                                   ifelse(results$FLAGGED == 1, 
                                          TRUE, 
                                          FALSE)))
}
#
#####################################################################
#
# start algorithm
#
main(2, flagged_query_template, TRUE, "TBD", "ida")
#
q(status=0)

