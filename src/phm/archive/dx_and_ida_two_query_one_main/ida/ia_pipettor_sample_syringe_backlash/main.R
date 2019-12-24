#
# Alinity IA Optics Dark Count
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
    final.modulesn,
    to_char(final.flag_date, 'YYYYMMDDHH24MISS') as flag_date,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner.modulesn,
        max(inner.logdate_local) as flag_date,
        count(inner.modulesn) as num_tests,
        avg(inner.backlash) as avg_backlash
    from (
        select
            ia.modulesn,
            ia.logdate_local,
            regexp_substr(ia.activity,'\\PosDiff:\\s(.*?)\\Z',1,1,null,1) as backlash
            from 
                idaqowner.icq_instrumentactivity ia
            where
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= ia.logdate_local
            and 
                ia.logdate_local < to_timestamp('<END_DATE>', 
                                                'MM/DD/YYYY HH24:MI:SS')
            and 
                ia.activity like '<I_POSDIFF_THRESHOLD_ACTIVITY>'
        ) inner
    group by
        inner.modulesn
    ) final
where
    final.avg_backlash > <I_POSDIFF_THRESHOLD_MAX>
and 
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>
order by
    final.modulesn"
#
not_flagged_query_template <- "
select 
    final.modulesn,
    to_char(final.flag_date, 'YYYYMMDD') as flag_date,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner.modulesn,
        max(inner.logdate_local) as flag_date,
        count(inner.modulesn) as num_tests,
        avg(inner.backlash) as avg_backlash
    from (
        select
            ia.modulesn,
            ia.logdate_local,
            regexp_substr(ia.activity,'\\PosDiff:\\s(.*?)\\Z',1,1,null,1) as backlash
            from 
                idaqowner.icq_instrumentactivity ia
            where
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= ia.logdate_local
            and 
                ia.logdate_local < to_timestamp('<END_DATE>', 
                                                'MM/DD/YYYY HH24:MI:SS')
            and 
                ia.activity like '<I_POSDIFF_THRESHOLD_ACTIVITY>'
        ) inner
    group by
        inner.modulesn
    ) final
where not (
    final.avg_backlash > <I_POSDIFF_THRESHOLD_MAX>
and 
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>
)
order by
    final.modulesn"
#
#####################################################################
#
# start algorithm
#
main("ida", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)

