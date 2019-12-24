#
# Alinity CC Cuvette Integrity
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    final2.moduleserialnumber as modulesn,
    date_format(max(final2.flag_date),'yyyyMMddHHmmss') as flag_date,
    final2.gt20000_gt20perc_sampevents,
    count(final2.moduleserialnumber) as count_moduleserialnumber
from (
    select
        middle2.*,
        case when (cast (middle2.num_sampevents_gt20000_percuv as double) / 
                   cast (middle2.num_sampevents_percuv as double)) > <CUVETTEINTEGRITY_PERCSAMPEVENTS_MIN>
             then 1
             else 0
             end as gt20000_gt20perc_sampevents
    from (
        select
            inner2.moduleserialnumber,
            inner2.cuvettenumber,
            max(inner2.datetimestamplocal) as flag_date,
            count(inner2.cuvettenumber) as num_sampevents_percuv,
            sum(inner2.check_gt20000) as num_sampevents_gt20000_percuv
        from (
            select
                sdp.scmserialnumber,
                sdp.datetimestamplocal,
                sdp.dispensebeginaverage,
                sdp.samplekey,
                sdp.testnumber,
                sdp.replicatestart,
                sdp.replicatenumber,
                dpm.moduleserialnumber,
                dpm.scmserialnumber,
                dpm.samplekey,
                dpm.toshibatestnumber,
                dpm.startingreplicatenumber,
                dpm.replicatenumber,
                r.scmserialnumber,
                r.testid as results_testid,
                r.cuvettenumber,
                case when sdp.dispensebeginaverage > <CUVETTEINTEGRITY_DISBEGAVG_MIN>
                     then 1
                     else 0
                     end as check_gt20000
            from
                dx_214_alinity_ci_ccsampledispensepcidata sdp
            left join 
                dx_210_alinity_c_ccdispensepm dpm
            on 
                to_date('<START_DATE>', 'yyyy-MM-dd') <= dpm.datetimestamplocal
            and 
                dpm.datetimestamplocal < to_date('<END_DATE>', 'yyyy-MM-dd')
            and
                sdp.scmserialnumber = dpm.scmserialnumber
            and 
                dpm.datetimestamplocal
                between 
                    sdp.datetimestamplocal - interval '0.1' second 
                and 
                    sdp.datetimestamplocal + interval '0.1' second
            and 
                sdp.samplekey = dpm.samplekey
            and 
                sdp.testnumber = dpm.toshibatestnumber
            and 
                sdp.replicatestart = dpm.startingreplicatenumber
            and 
                sdp.replicatenumber = dpm.replicatenumber
            left join 
                dx_210_alinity_c_result r
            on 
                to_date('<START_DATE>', 'yyyy-MM-dd') <= r.datetimestamplocal
            and 
                r.datetimestamplocal < to_date('<END_DATE>', 'yyyy-MM-dd')
            and
                dpm.scmserialnumber = r.scmserialnumber
            and 
                dpm.testid = r.testid
            and 
                r.cuvettenumber is not null
            where
                to_date('<START_DATE>', 'yyyy-MM-dd') <= sdp.datetimestamplocal
            and 
                sdp.datetimestamplocal < to_date('<END_DATE>', 'yyyy-MM-dd')
        ) inner2        
        group by
            inner2.moduleserialnumber,
            inner2.cuvettenumber
        order by
            inner2.moduleserialnumber,
            inner2.cuvettenumber
        ) middle2
    where
        middle2.num_sampevents_percuv > <CUVETTEINTEGRITY_SAMPEVENTS_MIN>
    and 
        middle2.cuvettenumber 
        between 
            <CUVETTEINTEGRITY_SEGMENT1>
        and 
            <CUVETTEINTEGRITY_SEGMENT2>
    ) final2
where
    final2.gt20000_gt20perc_sampevents = <THRESHOLDS_COUNT>
group by
    final2.moduleserialnumber,
    final2.gt20000_gt20perc_sampevents
having
    count(final2.moduleserialnumber) <= <CUVETTEINTEGRITY_NUMCUVETTES_MAX>
order by
    final2.moduleserialnumber,
    final2.gt20000_gt20perc_sampevents"
#
modulesn_query_template <- "
select
    distinct(dxr.moduleserialnumber) as modulesn
from
    dx_210_alinity_c_result dxr
where
    to_date('<MODULESN_START_DATE>', 'yyyy-MM-dd') <= dxr.datetimestamplocal
and 
    dxr.datetimestamplocal < to_date('<MODULESN_END_DATE>', 'yyyy-MM-dd')"
#
reliability_query_template <- NA
#
# number of days to check
#
number_of_days <- 7
#
# product line code for output file
#
product_line_code <- "210"
#
# configuration type, athena or spark
#
config_type <- "spark"
#
# load parquet files into spark 
#
spark_load_data <- function(db_conn,
                            param_sets, 
                            test_period)
{
    library(DBI)
    #
    overwrite <- TRUE
    #
    for (ts in seq(as.Date(test_period["START_DATE","VALUE"]),
                   as.Date(test_period["END_DATE","VALUE"]),
                   by="day")) {
        #
        # date to load
        #
        td <- format(as.Date(ts,origin="1970-01-01"), "%Y-%m-%d")
        #
        # load parquet files
        #
        tbl <- "dx_214_alinity_ci_ccsampledispensepcidata"
        pq_path <- paste0("s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/214-alinity-ci/CcSampleDispensePciData/transaction_date=", td)
print(paste("pq path", pq_path))
        read_in <- spark_read_parquet(db_conn, 
                                      name=tbl,
                                      path=pq_path,
                                      overwrite=overwrite)
        #
        tbl <- "dx_210_alinity_c_ccdispensepm"
        pq_path <- paste0("s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/210-alinity-c/CcDispensePM/transaction_date=", td)
print(paste("pq path", pq_path))
        read_in <- spark_read_parquet(db_conn, 
                                      name=tbl,
                                      path=pq_path,
                                      overwrite=overwrite)
        #
        tbl <- "dx_210_alinity_c_result"
        pq_path <- paste0("s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/210-alinity-c/Result/transaction_date=", td)
print(paste("pq path", pq_path))
        read_in <- spark_read_parquet(db_conn, 
                                      name=tbl,
                                      path=pq_path,
                                      overwrite=overwrite)
        overwrite <- FALSE
    }
}
#

