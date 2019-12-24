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
    inner.modulesn,
    to_char(inner.flag_date, 'YYYYMMDDHH24MISS') as flag_date,
    inner.ratiodisptl,
    inner.numtotaldisp
from (
    select
        w.modulesn,
        max(w.logdate_local) as flag_date,
        count(w.emptycount) as numtotaldisp,
       (sum(case when (w.emptycount - w.emptytolerance) <= <I_WZASP_THRESHOLD_NUMDISPTCTT>
                 then 1 
                 else 0 
                 end)) / (count(w.emptycount)) 
            as ratiodisptl
    from
        idaqowner.icq_wam w
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= w.logdate_local
    and 
        w.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        w.washzone = <I_WZASP_THRESHOLD_WZ>
    and 
        w.position = <I_WZASP_THRESHOLD_POS>
    group by
        w.modulesn
    ) inner
where
    inner.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>
and 
    inner.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>"
#
not_flagged_query_template <- "
select
    inner.modulesn,
    to_char(inner.flag_date, 'YYYYMMDD') as flag_date,
    inner.ratiodisptl,
    inner.numtotaldisp
from (
    select
        w.modulesn,
        max(w.logdate_local) as flag_date,
        count(w.emptycount) as numtotaldisp,
       (sum(case when (w.emptycount - w.emptytolerance) <= <I_WZASP_THRESHOLD_NUMDISPTCTT>
                 then 1 
                 else 0 
                 end)) / (count(w.emptycount)) 
            as ratiodisptl
    from
        idaqowner.icq_wam w
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= w.logdate_local
    and 
        w.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        w.washzone = <I_WZASP_THRESHOLD_WZ>
    and 
        w.position = <I_WZASP_THRESHOLD_POS>
    group by
        w.modulesn
    ) inner
where not (
    inner.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>
and 
    inner.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>
)"
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205", "ida")
# main(1, not_flagged_query_template, FALSE, "205", "ida")
#
q(status=0)

