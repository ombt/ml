# Alinity IA Vacuum Sensor
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    v.moduleserialnumber as modulesn,
    date_format(max(v.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_alinity_i_vacuumpressuredata v
where
    '<START_DATE>' <= v.transaction_date
and 
    v.transaction_date < '<END_DATE>'
and
    v.vacuumstatename = '<I_VACUUM_VACSTNAME>'
group by
    v.moduleserialnumber
having (
    count(v.adcvalue) >= 3
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)
order by
    v.moduleserialnumber"
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

