#
# Alinity IA Process Path Jams 5758
#
#####################################################################
#
# query templates
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
modulesn_query_template <- "
select
    distinct(dxr.moduleserialnumber) as modulesn
from
    dx.dx_205_alinity_i_result dxr
where
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
reliability_query_template <- NA
#
# number of days to check
#
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- "205"
#
# configuration type, athena or spark
#
config_type <- "spark"

