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
    eval.moduleserialnumber as modulesn,
    date_format(eval.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval.ratiodisptl,
    eval.numtotaldisp
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
        dx.dx_205_wamdata w
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
    ) eval
where
    eval.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>
and 
    eval.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>"
#
not_flagged_query_template <- "
select
    eval.moduleserialnumber as modulesn,
    date_format(eval.flag_date,'%Y%m%d') as flag_date,
    eval.ratiodisptl,
    eval.numtotaldisp
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
        dx.dx_205_wamdata w
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
    ) eval
where not (
    eval.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>
and 
    eval.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>
)"
#
#####################################################################
#
# start algorithm
#
main("dx", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)
