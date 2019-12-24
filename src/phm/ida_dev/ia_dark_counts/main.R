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
    evals.modulesn,
    evals.num_testid,
    to_char(evals.flag_date, 'YYYYMMDDHH24MISS') as flag_date,
    evals.max_idc,
    evals.sd_idc
from (
    select
        r.modulesn as modulesn,
        count(r.testid) as num_testid,
        max(r.logdate_local) as flag_date,
        max(r.integrateddarkcount) as max_idc,
        stddev(r.integrateddarkcount) as sd_idc
    from
        idaqowner.icq_results r
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
    and 
        r.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and
        r.integrateddarkcount is not null
    and
        upper(r.modulesn) like 'AI%'
    group by
        r.modulesn
    ) evals
where
    evals.num_testid >= <TESTID>
and
    evals.max_idc >= <INTEGRATEDDARKCOUNT_MAX>
and
    evals.sd_idc >= <INTEGRATEDDARKCOUNT_SD>
order by
    evals.modulesn"
#
not_flagged_query_template <- "
select
    evals.modulesn,
    evals.num_testid,
    to_char(evals.flag_date, 'YYYYMMDD') as flag_date,
    evals.max_idc,
    evals.sd_idc
from (
    select
        r.modulesn as modulesn,
        count(r.testid) as num_testid,
        max(r.logdate_local) as flag_date,
        max(r.integrateddarkcount) as max_idc,
        stddev(r.integrateddarkcount) as sd_idc
    from
        idaqowner.icq_results r
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
    and 
        r.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and
        r.integrateddarkcount is not null
    and
        upper(r.modulesn) like 'AI%'
    group by
        r.modulesn
    ) evals
where not (
    evals.num_testid >= <TESTID>
and
    evals.max_idc >= <INTEGRATEDDARKCOUNT_MAX>
and
    evals.sd_idc >= <INTEGRATEDDARKCOUNT_SD>
)
order by
    evals.modulesn"
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205", "ida")
# main(1, not_flagged_query_template, FALSE, "205", "ida")
#
q(status=0)

