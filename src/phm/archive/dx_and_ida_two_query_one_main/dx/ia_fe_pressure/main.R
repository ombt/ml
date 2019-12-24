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
    evals.modulesn,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date,
    evals.mechname,
    evals.aspirations,
    evals.numflags
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
        dx.dx_205_pmevent pm
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
where (
    evals.aspirations >= <ASPS>
and
    (cast (evals.numflags as double) / 
     cast (evals.aspirations as double) ) >= <PCTASPS>
)
order by
    evals.modulesn"
#
not_flagged_query_template <- "
select
    evals.modulesn,
    date_format(evals.flag_date,'%Y%m%d%') as flag_date,
    evals.mechname,
    evals.aspirations,
    evals.numflags
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
        dx.dx_205_pmevent pm
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
where not (
    evals.aspirations >= <ASPS>
and
    (cast (evals.numflags as double) / 
     cast (evals.aspirations as double) ) >= <PCTASPS>
)
order by
    evals.modulesn"
#
#####################################################################
#
# start algorithm
#
main("dx", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)
