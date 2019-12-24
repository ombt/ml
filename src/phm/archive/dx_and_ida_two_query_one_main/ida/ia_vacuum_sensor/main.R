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
main("ida", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)

