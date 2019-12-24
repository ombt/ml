select
    pm.deviceid as deviceid,
    pm.moduleserialnumber as modulesn,
    pm.pipettormechanismname as mechname,
    count(pm.pipettormechanismname) as aspirations,
    sum(case when pm.frontendpressure > 27000 or 
                  pm.frontendpressure < 21000
             then 1
             else 0
        end) as numflags
from 
    dx.dx_205_pmevent pm
where
    date_parse('02/01/2019 00:00:00', '%m/%d/%Y %T') <= pm.datetimestamplocal
and 
    pm.datetimestamplocal < date_parse('03/01/2019 00:00:00', '%m/%d/%Y %T') 
and 
    pm.frontendpressure is not null
and 
    pm.pipettingprotocolname != 'NonPipettingProtocol'
and 
    pm.pipettormechanismname in 
    (
        'SamplePipettorMechanism',
        'Reagent1PipettorMechanism',
        'Reagent2PipettorMechanism'
    )
group by
    pm.deviceid,
    pm.moduleserialnumber,
    pm.pipettormechanismname
having (
    count(pm.pipettormechanismname) >= 10
and
    ((sum(case when pm.frontendpressure > 27000 or 
                    pm.frontendpressure < 21000
             then 1
             else 0
        end)) / count(pm.pipettormechanismname)) >= 0.02
)
order by
    pm.pipettormechanismname,
    pm.deviceid,
    pm.moduleserialnumber

