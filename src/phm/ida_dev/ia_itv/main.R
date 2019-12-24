#
# Alinity IA ITV
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
        to_timestamp('<START_DATE>', 'MM/DD/YYYY HH24:MI:SS') <= i.logdate_local
    and 
        i.logdate_local < to_timestamp('<END_DATE>', 'MM/DD/YYYY HH24:MI:SS')
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
main(1, flagged_query_template, TRUE, "205", "ida")
# main(1, not_flagged_query_template, FALSE, "205", "ida")
#
q(status=0)

