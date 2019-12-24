# Alinity IA Vacuum Sensor
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
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
    count(v.adcvalue) >= <I_VACUUM_NUMREADINGS_MIN>
and 
    avg(v.adcvalue) <= <I_VACUUM_MEANADC_MIN>
)
order by
    v.moduleserialnumber"
#
# number of days to check
#
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- "205"

