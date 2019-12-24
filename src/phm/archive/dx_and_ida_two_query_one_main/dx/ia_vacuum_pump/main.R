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
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.moduleserialnumber,
        max(rawdata.datetimestamplocal) as flag_date,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.moduleserialnumber,
            vpd.datetimestamplocal,
            vpd.adcvalue
        from
            dx.dx_205_vacuumpressuredata vpd 
        where  
            '<START_DATE>' <= vpd.transaction_date
        and 
            vpd.transaction_date < '<END_DATE>'
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.moduleserialnumber
    ) evals
where
    evals.min_adcvalue > <VACUUMPUMP_MINADC>
and 
    evals.num_evals >= <VACUUMPUMP_NUMEVALS>
order by
    evals.moduleserialnumber"
#
not_flagged_query_template <- "
select
    evals.moduleserialnumber as modulesn,
    date_format(evals.flag_date,'%Y%m%d') as flag_date,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.moduleserialnumber,
        max(rawdata.datetimestamplocal) as flag_date,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.moduleserialnumber,
            vpd.datetimestamplocal,
            vpd.adcvalue
        from
            dx.dx_205_vacuumpressuredata vpd 
        where  
            '<START_DATE>' <= vpd.transaction_date
        and 
            vpd.transaction_date < '<END_DATE>'
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.moduleserialnumber
    ) evals
where not (
    evals.min_adcvalue > <VACUUMPUMP_MINADC>
and 
    evals.num_evals >= <VACUUMPUMP_NUMEVALS>
)
order by
    evals.moduleserialnumber"
#
#####################################################################
#
# start algorithm
#
main("dx", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)
