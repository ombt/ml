#
# Alinity CC Cuvette Integrity
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    evals.moduleserialnumber as modulesn,
    evals.mean_pwmvalue,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date
from (
    select
        i.moduleserialnumber,
        i.itvmechanismname,
        max(i.datetimestamplocal) as flag_date,
        avg(cast (i.pwmvalue as double)) as mean_pwmvalue
    from 
        dx.dx_205_alinity_i_itvdata i
    where
        '<START_DATE>' <= i.transaction_date
    and 
        i.transaction_date < '<END_DATE>'
    and 
        i.actualspeed != <I_ITV_THRESHOLD_ACTSPD>
    and 
        i.requestedspeed = <I_ITV_THRESHOLD_REQSPD>
    and 
        i.itvmechanismname = '<I_ITV_THRESHOLD_ITVMECHNAME>'
    group by
        i.moduleserialnumber,
        i.itvmechanismname
    ) evals
where
    evals.mean_pwmvalue >= <I_ITV_THRESHOLD_MEANPWM>
order by
    upper(evals.moduleserialnumber)"
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
