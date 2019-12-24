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
rlibpath <- Sys.getenv("PHM_ROOT")
if (nchar(rlibpath) == 0) {
    stop("PHM_ROOT not defined")
}
source(file.path(rlibpath,"rlib","common_utils.R"))
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
main("ida", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)

