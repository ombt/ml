#
# Alinity IA Process Path Jams 5756
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
where (
    eval2.num_results <= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_LE_NR_NUMRETRIES>
) or (
    eval2.num_results > <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_GT_NR_NUMRETRIES>
)
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
main(1, flagged_query_template, TRUE, "205", "ida")
# main(1, not_flagged_query_template, FALSE, "205", "ida")
#
q(status=0)

