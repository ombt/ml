#
# Alinity IA Pipettor Sample Syringe Backlash
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select 
    final.moduleserialnumber as modulesn,
    date_format(final.flag_date,'%Y%m%d%H%i%s') as flag_date,
    final.num_tests,
    final.avg_backlash
from (
    select
        inner1.moduleserialnumber,
        max(inner1.datetimestamplocal) as flag_date,
        count(inner1.moduleserialnumber) as num_tests,
        avg(cast (inner1.backlash as double)) as avg_backlash
    from (
        select
            ia.moduleserialnumber,
            ia.datetimestamplocal,
            regexp_extract(ia.activity,'^.*PosDiff: *(\\d+).*$',1) as backlash
        from 
            dx.dx_205_alinity_i_instrumentactivity ia
        where
            '<START_DATE>' <= ia.transaction_date
        and 
            ia.transaction_date < '<END_DATE>'
        and 
            ia.activity like '<I_POSDIFF_THRESHOLD_ACTIVITY>'
        ) inner1
    group by
        inner1.moduleserialnumber
    ) final
where
    final.avg_backlash > <I_POSDIFF_THRESHOLD_MAX>
and 
    final.num_tests >= <I_POSDIFF_THRESHOLD_NUMREPS>
order by
    final.moduleserialnumber"
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

