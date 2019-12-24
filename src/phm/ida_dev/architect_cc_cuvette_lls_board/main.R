#
# Architect CC Cuvette LLS Board 
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
one_query_template <- "
with rawdata as (
    select
        r.modulesndrm,
        r.cuvettenumber,
        r.completiondate,
        p.logfield24 as disreadyave,
        p.logfield25 as disbeginave
    from
        idaowner.results_cc r
    inner join
        idaowner.pressures_dis p
    on
        p.resultcode = '30'
    and
        to_timestamp('<START_DATE>', 'MM/DD/YYYY HH24:MI:SS') <= p.completiondate
    and 
        p.completiondate < to_timestamp('<END_DATE>', 'MM/DD/YYYY HH24:MI:SS')
    and 
        ((p.modulesndrm like 'C4%') or
         (p.modulesndrm like 'C16%'))
    and
        p.replicateid is not null
    and
        r.modulesndrm = p.modulesndrm
    and
        r.replicateid = p.replicateid
    where
        to_timestamp('<START_DATE>', 'MM/DD/YYYY HH24:MI:SS') <= r.completiondate
    and 
        r.completiondate < to_timestamp('<END_DATE>', 'MM/DD/YYYY HH24:MI:SS')
    and 
        ((r.modulesndrm like 'C4%') or
         (r.modulesndrm like 'C16%'))
    and
        r.replicateid is not null
)
select
    middle.modulesndrm as modulesn,
    middle.cuvettetype,
    to_char(max(middle.flag_date), 'YYYYMMDDHH24MISS') as flag_date,
    case when ((100*sum(middle.exceed_percuv_pct_thld)/count(middle.cuvettenumber)) > 10)
         then 1
         else 0
         end as flagged
from (
    select
        inner.modulesndrm,
        inner.cuvettenumber,
        inner.cuvettetype,
        inner.flag_date,
        inner.sample_count,
        inner.exceed_threshold_count,
        case when ((inner.sample_count > 20) and
                   (100*(inner.exceed_threshold_count/inner.sample_count) > <LOGFIELD24_PCT>))
             then
                 1
             else
                 0
             end as exceed_percuv_pct_thld
    from (
        select
            rawdata.modulesndrm,
            rawdata.cuvettenumber,
            'C4' as cuvettetype,
            max(rawdata.completiondate) as flag_date,
            count(rawdata.disreadyave) as sample_count,
            sum(case when (cast (rawdata.disreadyave as integer) > <LOGFIELD24_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesndrm like 'C4%'
        group by
            rawdata.modulesndrm,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesndrm,
            rawdata.cuvettenumber,
            'C16-ALINE' as cuvettetype,
            max(rawdata.completiondate) as flag_date,
            count(rawdata.disreadyave) as sample_count,
            sum(case when (cast (rawdata.disreadyave as integer) > <LOGFIELD24_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesndrm like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 0
        group by
            rawdata.modulesndrm,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesndrm,
            rawdata.cuvettenumber,
            'C16-BLINE' as cuvettetype,
            max(rawdata.completiondate) as flag_date,
            count(rawdata.disreadyave) as sample_count,
            sum(case when (cast (rawdata.disreadyave as integer) > <LOGFIELD24_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesndrm like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 1
        group by
            rawdata.modulesndrm,
            rawdata.cuvettenumber
        ) inner
    ) middle
group by
    middle.modulesndrm,
    middle.cuvettetype
order by
    middle.modulesndrm,
    middle.cuvettetype
"
#
post_processing <- function(results,
                            params, 
                            db_conn, 
                            query, 
                            options, 
                            test_period, 
                            flagged)
{
    results <- within(results,
    {
        PL[CUVETTETYPE == "C4"] <- "128"
        PL[CUVETTETYPE == "C16-ALINE"] <- "127"
        PL[CUVETTETYPE == "C16-BLINE"] <- "127"
    })
    #
    return(flagged_post_processing(results, 
                                   ifelse(results$FLAGGED, 
                                          TRUE, 
                                          FALSE)))
}
#
#####################################################################
#
# start algorithm
#
main(7, one_query_template, FALSE, "TBD", "ida")
#
q(status=0)

