select
    count(*)
from
    idaowner.pressures_dis
where
    completiondate > sysdate - 7
and (
    modulesndrm like 'C4%' 
or 
    modulesndrm like 'C16%' )


