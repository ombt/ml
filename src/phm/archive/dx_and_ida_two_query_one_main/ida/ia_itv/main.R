#
# Alinity IA ITV Generic
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
    to_char(evals.flag_date, 'YYYYMMDDHH24MISS') as flag_date,
    evals.mean_pwmvalue
from (
    select
        i.modulesn,
        i.itvmechanismname,
        max(i.logdate_local) as flag_date,
        avg(i.pwmvalue) as mean_pwmvalue
    from 
        idaqowner.icq_itvdata i
    where
        to_timestamp('<START_DATE>',
                     'MM/DD/YYYY HH24:MI:SS') <= i.logdate_local
    and 
        i.logdate_local < to_timestamp('<END_DATE>',
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        i.actualspeed != <I_ITV_THRESHOLD_ACTSPD>
    and 
        i.requestedspeed = <I_ITV_THRESHOLD_REQSPD>
    and 
        i.itvmechanismname = '<I_ITV_THRESHOLD_ITVMECHNAME>'
    group by
        i.modulesn,
        i.itvmechanismname
    ) evals
where
    evals.mean_pwmvalue >= <I_ITV_THRESHOLD_MEANPWM>
order by
    evals.modulesn"
#
not_flagged_query_template <- "
select
    evals.modulesn,
    to_char(evals.flag_date, 'YYYYMMDD') as flag_date,
    evals.mean_pwmvalue
from (
    select
        i.modulesn,
        i.itvmechanismname,
        max(i.logdate_local) as flag_date,
        avg(i.pwmvalue) as mean_pwmvalue
    from 
        idaqowner.icq_itvdata i
    where
        to_timestamp('<START_DATE>',
                     'MM/DD/YYYY HH24:MI:SS') <= i.logdate_local
    and 
        i.logdate_local < to_timestamp('<END_DATE>',
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        i.actualspeed != <I_ITV_THRESHOLD_ACTSPD>
    and 
        i.requestedspeed = <I_ITV_THRESHOLD_REQSPD>
    and 
        i.itvmechanismname = '<I_ITV_THRESHOLD_ITVMECHNAME>'
    group by
        i.modulesn,
        i.itvmechanismname
    ) evals
where
    not ( evals.mean_pwmvalue >= <I_ITV_THRESHOLD_MEANPWM> )
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

