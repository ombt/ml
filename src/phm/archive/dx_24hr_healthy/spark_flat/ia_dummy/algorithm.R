#
# Alinity CC Cuvette Integrity
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
with rawdata as (
select 
    d3.msn as modulesn,
    max(d3.flag_date) as flag_date,
    sum(d3.sum_exceed_range_threshold) as total_range_cnt,
    sum(d3.sum_exceed_stddev_threshold) as total_stddev_cnt
from (
    select 
        d2.msn,
        d2.flag_date,
        sum(d2.range_flag) over (
            order by 
                d2.msn,
                d2.flag_date
            asc rows 3 preceding
        ) as sum_exceed_range_threshold,
        sum(d2.stddev_flag) over (
            order by 
                d2.msn,
                d2.flag_date
            asc rows 3 preceding
        ) as sum_exceed_stddev_threshold
    from (
        select 
            d1.msn,
            d1.flag_date,
            d1.p1,
            case when ((d1.rnum > 9) and ( d1.p1 > 0) and
                       ((d1.p1 > (d1.avg_p1 + 3.0*(d1.max_p1 - d1.min_p1)/4.0)) or
                        (d1.p1 < (d1.avg_p1 - 3.0*(d1.max_p1 - d1.min_p1)/4.0))))
                 then 1
                 else 0 end as range_flag,
            case when ((d1.rnum > 9) and (d1.p1 > 0) and
                       ((d1.p1 > (d1.avg_p1 + 3.0*d1.stddev_p1)) or
                        (d1.p1 < (d1.avg_p1 - 3.0*d1.stddev_p1))))
                 then 1
                 else 0 end as stddev_flag
        from ( 
            select
                dpm.moduleserialnumber as msn,
                dpm.pressured1 as p1,
                row_number() over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal) asc
                ) as rnum,
                max(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as max_p1,
                min(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as min_p1,
                avg(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as avg_p1,
                stddev(dpm.pressured1) over (
                    order by 
                        dpm.moduleserialnumber,
                        date_trunc('day', dpm.datetimestamplocal)
                    asc rows 9 preceding
                ) as stddev_p1,
                date_trunc('day', dpm.datetimestamplocal) as flag_date
            from
                dx_210_alinity_c_ccdispensepm dpm
            where
                to_date('<START_DATE>', 'yyyy-MM-dd') <= dpm.datetimestamplocal
            and 
                dpm.datetimestamplocal < to_date('<END_DATE>', 'yyyy-MM-dd')
            and 
                dpm.pressured1 is not null
            order by
                dpm.moduleserialnumber,
                date_trunc('day', dpm.datetimestamplocal)
        ) d1
    ) d2
) d3
group by
    d3.msn
)
select 
    rawdata.modulesn,
    date_format(rawdata.flag_date,'yyyyMMddHHmmss') as flag_date,
    rawdata.total_range_cnt,
    rawdata.total_stddev_cnt
from 
    rawdata
order by
    rawdata.modulesn"
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
number_of_days <- 15
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
    # table names and paths
    #
    tbl1_name    <- "dx_210_alinity_c_ccdispensepm"
    tbl1_pq_path <- "s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/210-alinity-c/CcDispensePM/transaction_date="
    tbl1_pq_paths <- c()
    #
    tbl2_name    <- "dx_210_alinity_c_result"
    tbl2_pq_path <- "s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/210-alinity-c/Result/transaction_date="
    tbl2_pq_paths <- c()
    #
    # create list of files to load.
    #
    for (ts in seq(as.Date(test_period["START_DATE","VALUE"]),
                   as.Date(test_period["END_DATE","VALUE"]),
                   by="day")) {
        #
        # date to load
        #
        td <- format(as.Date(ts,origin="1970-01-01"), "%Y-%m-%d")
        #
        # parquet file paths
        #
        tbl1_pq_paths <- c(tbl1_pq_paths, paste0(tbl1_pq_path,td))
        tbl2_pq_paths <- c(tbl2_pq_paths, paste0(tbl2_pq_path,td))
    }
    #
    # now load data
    #
    tbl1_tbl <- spark_read_parquet(db_conn, 
                                   name=tbl1_name,
                                   path=tbl1_pq_paths,
                                   overwrite=TRUE)
    tbl2_tbl <- spark_read_parquet(db_conn, 
                                   name=tbl2_name,
                                   path=tbl2_pq_paths,
                                   overwrite=TRUE)
}

