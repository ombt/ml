#
# Architect Dark Count Average and Standard Deviation Exceeded
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select 
    rawdata.modulesn,
    rawdata.pl,
    max(rawdata.max_completion_date) as flag_date
from (
    select
        r.architect_moduleserial as modulesn,
        r.architect_productline as pl,
        date_trunc('day', r.completiondatetime_iso) as test_completion_date, 
        max(r.completiondatetime_iso) as max_completion_date,
        stddev(r.darkcount) as std_dev_dark_count, 
        avg (r.darkcount) as average_dark_count
    from
        dx.dx_architect_results r
    where
        r.darkcount is not null 
    and
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    group by 
        r.architect_moduleserial,
        r.architect_productline,
        date_trunc('day', r.completiondatetime_iso)
    ) rawdata
where
    rawdata.std_dev_dark_count >= <MAX_SD>
and
    rawdata.average_dark_count >= <MAX_AVG>
group by
    rawdata.modulesn,
    rawdata.pl
having
    count(rawdata.modulesn) >= <MAX_DAYS>"
#
modulesn_query_template <- "
select
    distinct(upper(dxr.architect_moduleserial)) as modulesn
from
    dx.dx_architect_results dxr
where
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
reliability_query_template <- NA
#
# number of days to check
#
number_of_days <- 7
#
# product line code for output file
#
product_line_code <- NA
#
# configuration type, athena or spark
#
config_type <- "spark"
