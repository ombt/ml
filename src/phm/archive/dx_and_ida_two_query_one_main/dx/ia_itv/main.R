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
    evals.moduleserialnumber as modulesn,
    evals.mean_pwmvalue,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date
from (
    select
        i.moduleserialnumber,
        i.itvmechanismname,
        max(i.datetimestamplocal) as flag_date,
        avg(cast (i.pwmvalue as double)) as mean_pwmvalue
    from 
        dx.dx_205_itvdata i
    where
        '<START_DATE>' <= i.transaction_date
    and 
        i.transaction_date < '<END_DATE>'
    and 
        i.actualspeed != <I_ITV_THRESHOLD_ACTSPD>
    and 
        i.requestedspeed = <I_ITV_THRESHOLD_REQSPD>
    and 
        i.itvmechanismname = '<I_ITV_THRESHOLD_ITVMECHNAME>'
    group by
        i.moduleserialnumber,
        i.itvmechanismname
    ) evals
where
    evals.mean_pwmvalue >= <I_ITV_THRESHOLD_MEANPWM>
order by
    upper(evals.moduleserialnumber)"
#
not_flagged_query_template <- "
select
    evals.moduleserialnumber as modulesn,
    evals.mean_pwmvalue,
    date_format(evals.flag_date,'%Y%m%d') as flag_date
from (
    select
        i.moduleserialnumber,
        i.itvmechanismname,
        max(i.datetimestamplocal) as flag_date,
        avg(cast (i.pwmvalue as double)) as mean_pwmvalue
    from 
        dx.dx_205_itvdata i
    where
        '<START_DATE>' <= i.transaction_date
    and 
        i.transaction_date < '<END_DATE>'
    and 
        i.actualspeed != <I_ITV_THRESHOLD_ACTSPD>
    and 
        i.requestedspeed = <I_ITV_THRESHOLD_REQSPD>
    and 
        i.itvmechanismname = '<I_ITV_THRESHOLD_ITVMECHNAME>'
    group by
        i.moduleserialnumber,
        i.itvmechanismname
    ) evals
where
    not ( evals.mean_pwmvalue >= <I_ITV_THRESHOLD_MEANPWM> )
order by
    upper(evals.moduleserialnumber)"
#
#####################################################################
#
# start algorithm
#
main("dx", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)
