#
# Alinity IA ITV Generic
#
#####################################################################
#
# set working directory
#
args <- commandArgs()
scripts <- args[grepl("--file=", args)]
script_paths <- sub("^.*--file=(.*)$", "\\1", scripts)
work_dir <- dirname(script_paths[1])
#
print(sprintf("Working directory: %s", work_dir))
setwd(work_dir)
#
#####################################################################
#
# required libraries
#
library(checkpoint)
#
CHECKPOINT_LOCATION <- Sys.getenv("CHECKPOINT_LOCATION")
if (nchar(CHECKPOINT_LOCATION) > 0) {
    checkpoint("2019-07-01", 
               checkpointLocation=CHECKPOINT_LOCATION)
} else {
    print("CHECKPOINT_LOCATION is not defined. Skipping.")
}
#
library(getopt)
library(DBI)
library(RJDBC)
library(odbc)
library(dplyr)
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source libs
#
common_utils_path <- file.path(".", "adhoc_common_utils.R")
if ( ! file.exists(common_utils_path)) {
    stop("No 'adhoc_common_utils.R' found")
}
source(common_utils_path)
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
        upper(trim(i.moduleserialnumber)) as moduleserialnumber,
        i.itvmechanismname,
        max(i.datetimestamplocal) as flag_date,
        avg(cast (i.pwmvalue as double)) as mean_pwmvalue
    from 
        dx.dx_205_alinity_i_itvdata i
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
    and (
        i.timetoengage is null 
    or
        i.timetodisengage is null
    )
    group by
        upper(trim(i.moduleserialnumber)),
        i.itvmechanismname
    ) evals
where
    evals.mean_pwmvalue >= <I_ITV_THRESHOLD_MEANPWM>
order by
    upper(evals.moduleserialnumber)"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
from
    dx.dx_205_alinity_i_result dxr
where
    dxr.moduleserialnumber is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
chart_data_query_template <- NA
#
#####################################################################
#
# start algorithm
#
main(1, 
     flagged_query_template, 
     modulesn_query_template, 
     chart_data_query_template,
     "205")
#
q(status=0)
