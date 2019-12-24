#
# Architect Dark Count Average Exceeded
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select 
    rawdata.modulesn,
    rawdata.pl,
    date_format(max(rawdata.max_completion_date),'%Y%m%d%H%i%s') as flag_date,
    max(rawdata.average_dark_count) as average_dark_count
from (
    select
        upper(trim(r.architect_moduleserial)) as modulesn,
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
        r.architect_productline is not null
    and
        r.architect_productline in ( '115', '116', '117' )
    and
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    group by 
        upper(trim(r.architect_moduleserial)),
        r.architect_productline,
        date_trunc('day', r.completiondatetime_iso)
    ) rawdata
where
    rawdata.average_dark_count >= <DARKCOUNT_MAX_AVG>
group by
    rawdata.modulesn,
    rawdata.pl
having
    count(rawdata.modulesn) >= <DARKCOUNT_MAX_DAYS>"
#
use_suppression <- FALSE
#
chart_data_query_template <- "
select 
    rawdata.modulesn,
    rawdata.pl,
    date_format(max(rawdata.max_completion_date),'%Y%m%d%H%i%s') as flag_date,
    max(rawdata.average_dark_count) as chart_data_value
from (
    select
        upper(trim(r.architect_moduleserial)) as modulesn,
        r.architect_productline as pl,
        date_trunc('day', r.completiondatetime_iso) as test_completion_date, 
        max(r.completiondatetime_iso) as max_completion_date,
        stddev(r.darkcount) as std_dev_dark_count, 
        avg(r.darkcount) as average_dark_count
    from
        dx.dx_architect_results r
    where
        r.darkcount is not null 
    and
        r.architect_productline is not null
    and
        r.architect_productline in ( '115', '116', '117' )
    and
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    group by 
        upper(trim(r.architect_moduleserial)),
        r.architect_productline,
        date_trunc('day', r.completiondatetime_iso)
    ) rawdata
group by
    rawdata.modulesn,
    rawdata.pl
having
    max(rawdata.average_dark_count) is not null
and
    count(rawdata.modulesn) >= 1"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.architect_moduleserial))) as modulesn,
    dxr.architect_productline as pl
from
    dx.dx_architect_results dxr
where
    dxr.architect_moduleserial is not null
and
    dxr.architect_productline is not null
and
    dxr.architect_productline in ( '115', '116', '117' )
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
# number of days to check
#
number_of_days <- 2
#
# product line code for output file
#
product_line_code <- NA
#
# configuration type, athena or spark
#
config_type <- "dx"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$AVERAGE_DARK_COUNT
    #
    for (irec in 1:nrow(flagged_results)) {
        modulesn <- flagged_results[irec, "MODULESN"]
        #
        if (grepl("^I1SR", modulesn)) {
             flagged_results[irec, "IHN_LEVEL3_DESC"] <- "i1SR Dark Count >250"
        } else if (grepl("^ISR", modulesn)) {
             flagged_results[irec, "IHN_LEVEL3_DESC"] <- "i2SR Dark Count >250"
        } else if (grepl("^I20", modulesn)) {
             flagged_results[irec, "IHN_LEVEL3_DESC"] <- "i2SR Dark Count >250"
        }
    }
    #
    return(flagged_results)
}
#
