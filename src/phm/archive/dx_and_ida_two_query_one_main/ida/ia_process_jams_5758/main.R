#
# Alinity IA Process Path Jams 5758
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
    eval1.modulesn,
    to_char(eval2.flag_date, 'YYYYMMDDHH24MISS') as flag_date,
    eval1.num_retries,
    eval2.num_results
from (
    select
        m.modulesn,
        count(m.modulesn) as num_retries
    from
        idaqowner.icq_messagehistory m
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= m.logdate_local
    and 
        m.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        m.aimcode = '<PROCPATHJAMS_THRESHOLD_AIMCODE>'
    and 
        m.aimsubcode = '<PROCPATHJAMS_THRESHOLD_AIMSUBCODE>'
    group by
        m.modulesn
    ) eval1
inner join (
    select
        r.modulesn,
        max(r.logdate_local) as flag_date,
        count(r.correctedcount) as num_results
    from
        idaqowner.icq_results r
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
    and 
        r.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        r.correctedcount is not null
    group by
        r.modulesn
    ) eval2
on 
    eval1.modulesn = eval2.modulesn
where
    eval2.num_results >= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES>
order by
    eval1.modulesn"
#
not_flagged_query_template <- "
select
    eval1.modulesn,
    to_char(eval2.flag_date, 'YYYYMMDD') as flag_date,
    eval1.num_retries,
    eval2.num_results
from (
    select
        m.modulesn,
        count(m.modulesn) as num_retries
    from
        idaqowner.icq_messagehistory m
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= m.logdate_local
    and 
        m.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        m.aimcode = '<PROCPATHJAMS_THRESHOLD_AIMCODE>'
    and 
        m.aimsubcode = '<PROCPATHJAMS_THRESHOLD_AIMSUBCODE>'
    group by
        m.modulesn
    ) eval1
inner join (
    select
        r.modulesn,
        max(r.logdate_local) as flag_date,
        count(r.correctedcount) as num_results
    from
        idaqowner.icq_results r
    where
        to_timestamp('<START_DATE>', 
                     'MM/DD/YYYY HH24:MI:SS') <= r.logdate_local
    and 
        r.logdate_local < to_timestamp('<END_DATE>', 
                                       'MM/DD/YYYY HH24:MI:SS')
    and 
        r.correctedcount is not null
    group by
        r.modulesn
    ) eval2
on 
    eval1.modulesn = eval2.modulesn
where not (
    eval2.num_results >= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES>
)
order by
    eval1.modulesn"
#
#####################################################################
#
# start algorithm
#
main("ida", 1, flagged_query_template, not_flagged_query_template)
#
q(status=0)

