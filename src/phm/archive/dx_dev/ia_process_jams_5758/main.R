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
    eval1.moduleserialnumber as modulesn,
    date_format(eval2.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval1.num_retries,
    eval2.num_results
from (
    select
        m.moduleserialnumber,
        count(m.moduleserialnumber) as num_retries
    from
        dx.dx_205_alinity_i_messagehistory m
    where
        '<START_DATE>' <= m.transaction_date
    and 
        m.transaction_date < '<END_DATE>'
    and 
        m.aimcode = <PROCPATHJAMS_THRESHOLD_AIMCODE>
    and 
        m.aimsubcode = '<PROCPATHJAMS_THRESHOLD_AIMSUBCODE>'
    group by
        m.moduleserialnumber
    ) eval1
inner join (
    select
        r.moduleserialnumber,
        max(r.datetimestamplocal) as flag_date,
        count(r.correctedcount) as num_results
    from
        dx.dx_205_alinity_i_result r
    where
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    and 
        r.correctedcount is not null
    group by
        r.moduleserialnumber
    ) eval2
on 
    eval1.moduleserialnumber = eval2.moduleserialnumber
where
    eval2.num_results >= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES>
order by
    eval1.moduleserialnumber"
#
not_flagged_query_template <- "
select
    eval1.moduleserialnumber as modulesn,
    date_format(eval2.flag_date,'%Y%m%d') as flag_date,
    eval1.num_retries,
    eval2.num_results
from (
    select
        m.moduleserialnumber,
        count(m.moduleserialnumber) as num_retries
    from
        dx.dx_205_alinity_i_messagehistory m
    where
        '<START_DATE>' <= m.transaction_date
    and 
        m.transaction_date < '<END_DATE>'
    and 
        m.aimcode = <PROCPATHJAMS_THRESHOLD_AIMCODE>
    and 
        m.aimsubcode = '<PROCPATHJAMS_THRESHOLD_AIMSUBCODE>'
    group by
        m.moduleserialnumber
    ) eval1
inner join (
    select
        r.moduleserialnumber,
        max(r.datetimestamplocal) as flag_date,
        count(r.correctedcount) as num_results
    from
        dx.dx_205_alinity_i_result r
    where
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    and 
        r.correctedcount is not null
    group by
        r.moduleserialnumber
    ) eval2
on 
    eval1.moduleserialnumber = eval2.moduleserialnumber
where not (
    eval2.num_results >= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and 
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES>
)
order by
    eval1.moduleserialnumber"
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "205")
# main(1, not_flagged_query_template, FALSE, "205")
#
q(status=0)
