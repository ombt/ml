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
    evals.mechname,
    to_char(evals.flag_date, 'YYYYMMDDHH24MISS') as flag_date,
    evals.aspirations,
    evals.numflags
from ( 
    select
        pm.modulesn,
        pm.pipettormechanismname as mechname,
        max(pm.logdate_local) as flag_date,
        count(pm.pipettormechanismname) as aspirations,
        sum(case when pm.frontendpressure > <MAX_VALUE> or 
                      pm.frontendpressure < <MIN_VALUE>
                 then 1
                 else 0
                 end) as numflags
    from
        idaqowner.icq_pmevents pm
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= pm.logdate_local
    and 
        pm.logdate_local < to_timestamp('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
    and 
        pm.frontendpressure is not null
    and 
        pm.pipettingprotocolname != 'NonPipettingProtocol'
    and 
        pm.pipettormechanismname = '<PIPMECHNAME>'
    group by
        pm.modulesn,
        pm.pipettormechanismname
    ) evals
where (
    evals.aspirations >= <ASPS>
and
    (evals.numflags / evals.aspirations) >= <PCTASPS>
)
order by
    evals.modulesn"
#
not_flagged_query_template <- "
select
    evals.modulesn,
    evals.mechname,
    to_char(evals.flag_date, 'YYYYMMDD') as flag_date,
    evals.aspirations,
    evals.numflags
from ( 
    select
        pm.modulesn,
        pm.pipettormechanismname as mechname,
        max(pm.logdate_local) as flag_date,
        count(pm.pipettormechanismname) as aspirations,
        sum(case when pm.frontendpressure > <MAX_VALUE> or 
                      pm.frontendpressure < <MIN_VALUE>
                 then 1
                 else 0
                 end) as numflags
    from
        idaqowner.icq_pmevents pm
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= pm.logdate_local
    and 
        pm.logdate_local < to_timestamp('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
    and 
        pm.frontendpressure is not null
    and 
        pm.pipettingprotocolname != 'NonPipettingProtocol'
    and 
        pm.pipettormechanismname = '<PIPMECHNAME>'
    group by
        pm.modulesn,
        pm.pipettormechanismname
    ) evals
where not (
    evals.aspirations >= <ASPS>
and
    (evals.numflags / evals.aspirations) >= <PCTASPS>
)
order by
    evals.modulesn"
#
#####################################################################
#
# start algorithm
#
main("ida", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)

