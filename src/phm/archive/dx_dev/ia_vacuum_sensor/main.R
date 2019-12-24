# Alinity IA Vacuum Sensor
#
#####################################################################
#
# required libraries
#
library(getopt)
library(DBI)
library(RJDBC)
library(dplyr)
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
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
    v.moduleserialnumber as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_alinity_i_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    v.moduleserialnumber
having (
    count(v.adcvalue) >= <I_VACUUM_NUMREADINGS_MIN>
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)
order by
    v.moduleserialnumber"
#
not_flagged_query_template <- "
select
    v.moduleserialnumber as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_alinity_i_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    v.moduleserialnumber
having not (
    count(v.adcvalue) >= <I_VACUUM_NUMREADINGS_MIN>
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)
order by
    v.moduleserialnumber"

#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205")
# main(1, not_flagged_query_template, FALSE, "205")
#
q(status=0)
