select
    count(*)
from
    idaowner.results_cc
where
    completiondate > sysdate - 7
and (
    modulesndrm like 'C4%' 
or 
    modulesndrm like 'C16%' )


