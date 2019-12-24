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
main(1, flagged_query_template, TRUE, "205", "ida")
# main(1, not_flagged_query_template, FALSE, "205", "ida")
#
q(status=0)

