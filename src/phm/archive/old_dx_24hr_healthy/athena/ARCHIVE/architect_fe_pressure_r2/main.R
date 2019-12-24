#
# Architect Dark Count R2 Average Exceeded
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
# required libraries
#
library(checkpoint)
#
CHECKPOINT_LOCATION <- Sys.getenv("CHECKPOINT_LOCATION")
if (nchar(CHECKPOINT_LOCATION) > 0) {
    checkpoint("2019-07-01", 
               checkpointLocation=CHECKPOINT_LOCATION)
} else {
    print("CHECKPOINT_LOCATION is not defined. Skipping.")
}
#
library(getopt)
library(DBI)
library(RJDBC)
library(odbc)
library(dplyr)
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source libs
#
common_utils_path <- file.path(".", "adhoc_common_utils.R")
if ( ! file.exists(common_utils_path)) {
    stop("No 'adhoc_common_utils.R' found")
}
source(common_utils_path)
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
with rawdata as (
    select
        tbl2.deviceid as deviceid,
        tbl2.modulesndrm as modulesn,
        tbl2.productline as pl,
        tbl2.pipetter as pipetter,
        date_trunc('day', tbl2.completiondate_iso) as trunc_comp_date,
        avg(tbl2.frontendpressure) as avg_fep,
        max(tbl2.completiondate_iso) as max_comp_date
    from ( 
        select 
            tbl1.deviceid,
            tbl1.modulesndrm,
            tbl1.completiondate_iso,
            tbl1.productline,
            coalesce (
                case when substr(tbl1.modulesndrm,1,2)='I1' 
                     then 
                         'I1' 
                     end,
                case when tbl1.location in ('INNER_REAGENT', 
                                             'MEDIAL_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_INNER_REAGENT', 
                                             'R1_MEDIAL_REAGENT',
                                             'R1_OUTER_REAGENT') and
                          tbl1.pipetter = ('PTRGNT1') 
                     then 
                          'R1' 
                     end,
                case when tbl1.location in ('INNER_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_MEDIAL_REAGENT', 
                                             'R1_OUTER_REAGENT') and
                          tbl1.pipetter = ('PTRGNT2')
                     then 
                         'R1' 
                     end,
                case when tbl1.location in ('INNER_REAGENT', 
                                             'MEDIAL_REAGENT', 
                                             'OUTER_REAGENT', 
                                             'R1_INNER_REAGENT', 
                                             'R1_OUTER_REAGENT', 
                                             'R1_MEDIAL_REAGENT') and
                          tbl1.pipetter = ('RGNT1') 
                     then 
                         'R1' 
                     end,
                case when tbl1.location in ( 'RV2') and
                          tbl1.pipetter = ('RGNT1')
                     then 
                         'R1' 
                     end,
                case when tbl1.location in ('RV48') and
                          tbl1.pipetter = ('RGNT1')
                     then 
                         'R2' 
                     end,
                case when tbl1.location in ('R2_INNER_REAGENT', 
                                             'R2_OUTER_REAGENT', 
                                             'R2_MEDIAL_REAGENT') and
                          tbl1.pipetter = ('RGNT1') 
                     then 
                         'R2' 
                     end,
                case when tbl1.location in ('MEDIAL_REAGENT', 
                                             'R2_INNER_REAGENT') and
                          tbl1.pipetter = ('RGNT2')
                     then 
                         'R2'
                     end,
                case when tbl1.location in ('MEDIAL_REAGENT', 
                                             'R2_INNER_REAGENT') and
                          tbl1.pipetter = ('RGNT3')
                     then 
                         'R2'
                     end,
                case when tbl1.location in ('RV24') and
                          tbl1.pipetter in ('PTSAMP1', 
                                             'PTSAMP2')
                     then 
                         'SAMP'
                     end,
                case when tbl1.location in ('RV2') and
                          tbl1.pipetter in ('PTSAMP1', 
                                             'PTSAMP2')
                     then 
                         'R1'
                     end,
                case when tbl1.location in ('ISH_SAMPLE', 
                                             'LAS_SAMPLE') and
                          tbl1.pipetter in ('SAMP')
                     then 
                         'SAMP'
                     end,
                case when tbl1.location in ('STAT_SAMPLE') and
                          tbl1.pipetter in ('SAMP')
                     then 
                         'STATSAMP'
                     else 
                         tbl1.pipetter 
                     end
                ) pipetter,
            tbl1.frontendpressure
        from ( 
            select 
                ip.architect_deviceid as deviceid,
                upper(trim(ip.architect_moduleserial)) as modulesndrm,
                ip.architect_productline as productline,
                ip.frontendpressure,
                ip.pipetter,
                ip.completiondate_iso,
                ip.location
            from 
                dx.dx_architect_pm ip
            where
                '<START_DATE>' <= ip.transaction_date
            and 
                ip.transaction_date < '<END_DATE>'
        ) tbl1
    ) tbl2
    group by
        tbl2.deviceid,
        tbl2.modulesndrm,
        tbl2.productline,
        tbl2.pipetter,
        date_trunc('day', tbl2.completiondate_iso)
)
select
    final.pl,
    final.modulesn,
    final.pipetter,
    final.flag_date
from (
    select
        derived.pl,
        derived.modulesn,
        derived.pipetter,
        sum(derived.fep_gt27000_cnt) as fep_gt27000_cnt,
        sum(derived.fep_cnt) as fep_cnt,
        max(derived.flag_date) as flag_date
    from (
        select 
            rawdata.modulesn,
            rawdata.pl,
            rawdata.pipetter,
            sum(case when rawdata.avg_fep >  <THRESHOLD_NUMBER>
                     then 1
                     else 0
                     end) as fep_gt27000_cnt,
            count(rawdata.avg_fep) as fep_cnt,
            max(rawdata.max_comp_date) as flag_date
        from 
            rawdata
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.pipetter,
            rawdata.trunc_comp_date
        ) derived
    group by
        derived.pl,
        derived.modulesn,
        derived.pipetter
    ) final
where
    final.fep_gt27000_cnt >= <THRESHOLD_NUMBER_UNIT>
and
    final.pipetter = 'R2'
order by
    final.pl,
    final.modulesn,
    final.pipetter"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.architect_moduleserial))) as modulesn
from
    dx.dx_architect_results dxr
where
    dxr.architect_moduleserial is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
chart_data_query_template <- NA
#
#####################################################################
#
# start algorithm
#
main(7, 
     flagged_query_template, 
     modulesn_query_template, 
     chart_data_query_template,
     NA)
#
q(status=0)
