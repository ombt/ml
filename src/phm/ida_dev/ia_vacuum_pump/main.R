#
# Alinity IA Vacuum Pump
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
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.modulesn,
        max(rawdata.logdate_local) as flag_date,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.modulesn,
            vpd.logdate_local,
            vpd.adcvalue
        from
            idaqowner.icq_vacuumpressuredata vpd
        where
            to_timestamp('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
        and 
            vpd.logdate_local < to_timestamp('<END_DATE>', 
                                             'MM/DD/YYYY HH24:MI:SS')
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.modulesn
    ) evals
where
    evals.min_adcvalue > <VACUUMPUMP_MINADC>
and 
    evals.num_evals >= <VACUUMPUMP_NUMEVALS>
order by
    evals.modulesn"
#
not_flagged_query_template <- "
select
    evals.modulesn,
    to_char(evals.flag_date, 'YYYYMMDD') as flag_date,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.modulesn,
        max(rawdata.logdate_local) as flag_date,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.modulesn,
            vpd.logdate_local,
            vpd.adcvalue
        from
            idaqowner.icq_vacuumpressuredata vpd
        where
            to_timestamp('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
        and 
            vpd.logdate_local < to_timestamp('<END_DATE>', 
                                             'MM/DD/YYYY HH24:MI:SS')
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.modulesn
    ) evals
where not (
    evals.min_adcvalue > <VACUUMPUMP_MINADC>
and 
    evals.num_evals >= <VACUUMPUMP_NUMEVALS>
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

