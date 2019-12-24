select 
    *
from 
    svc_phm_owner.phm_alg_output
where 
    to_timestamp('01/01/2018 00:00:00', 
                 'MM/DD/YYYY HH24:MI:SS') <= flag_date
and 
    flag_date <= to_timestamp('06/01/2019 00:00:00', 
                              'MM/DD/YYYY HH24:MI:SS')
and
    flag_yn = 'YES'
