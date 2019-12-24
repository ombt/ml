select
    pm.deviceid as deviceid,
    pm.modulesn as modulesn,
    pm.pipettormechanismname as mechname,
    count(pm.pipettormechanismname) as aspirations,
    sum(case when pm.frontendpressure > 27000 or 
                  pm.frontendpressure < 21000
             then 1
             else 0
        end) as numflags
from 
    idaqowner.icq_pmevents pm
where
    to_timestamp('02/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS') <= pm.logdate_local
and 
    pm.logdate_local < to_timestamp('03/01/2019 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
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
    pm.modulesn,
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
    pm.modulesn

