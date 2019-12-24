#
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
    v.modulesn,
    to_char(max(v.logdate_local), 'YYYYMMDDHH24MISS') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    idaqowner.icq_vacuumpressuredata v
where
    to_timestamp('<START_DATE>', 
                 'MM/DD/YYYY HH24:MI:SS') <= v.logdate_local
and 
    v.logdate_local < to_timestamp('<END_DATE>', 
                                   'MM/DD/YYYY HH24:MI:SS')
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    v.modulesn
having (
    count(v.adcvalue) >= <I_VACUUM_NUMREADINGS_MIN>
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)
order by
    v.modulesn"
#
not_flagged_query_template <- "
select
    v.modulesn,
    to_char(max(v.logdate_local), 'YYYYMMDD') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    idaqowner.icq_vacuumpressuredata v
where
    to_timestamp('<START_DATE>', 
                 'MM/DD/YYYY HH24:MI:SS') <= v.logdate_local
and 
    v.logdate_local < to_timestamp('<END_DATE>', 
                                   'MM/DD/YYYY HH24:MI:SS')
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    v.modulesn
having not (
    count(v.adcvalue) >= <I_VACUUM_NUMREADINGS_MIN>
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)
order by
    v.modulesn"
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205", "ida")
# main(1, not_flagged_query_template, FALSE, "205", "ida")
#
q(status=0)

