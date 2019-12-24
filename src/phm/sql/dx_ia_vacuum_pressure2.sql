select
    v.deviceid,
    v.moduleserialnumber as modulesn,
    avg(v.adcvalue) as mean_adc,
    count(v.adcvalue) as num_readings
from 
    dx.dx_205_vacuumpressuredata v
where
    date_parse('01/01/2019 00:00:00', '%m/%d/%Y %T') <= v.datetimestamplocal
and 
    v.datetimestamplocal < date_parse('02/01/2019 00:00:00', '%m/%d/%Y %T') 
and
    v.vacuumstatename = 'VacuumBledOff'
group by
    v.deviceid,
    v.moduleserialnumber
having (
    avg(v.adcvalue) <= 3549
and 
    count(v.adcvalue) >= 3
)
order by
    v.deviceid,
    v.moduleserialnumber

