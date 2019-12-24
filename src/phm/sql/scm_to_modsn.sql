select
    scmserialnumber,
    count(distinct moduleserialnumber) as cnt_modsn
from 
    dx.dx_214_ccsampledispensepcidata
where
    '2018-11-20' <= transaction_date
and
    transaction_date < '2018-11-28'
group by
    scmserialnumber
order by
    scmserialnumber;

select
    scmserialnumber,
    count(distinct moduleserialnumber) as cnt_modsn
from 
    dx.dx_210_ccdispensepm
where
    '2018-11-20' <= transaction_date
and
    transaction_date < '2018-11-28'
group by
    scmserialnumber
order by
    scmserialnumber;

select
    scmserialnumber,
    count(distinct moduleserialnumber) as cnt_modsn
from 
    dx.dx_210_result
where
    '2018-11-20' <= transaction_date
and
    transaction_date < '2018-11-28'
group by
    scmserialnumber
order by
    scmserialnumber;

select
    distinct moduleserialnumber
from 
    dx.dx_214_ccsampledispensepcidata
where
    '2018-11-20' <= transaction_date
and
    transaction_date < '2018-11-28';

select
    count(distinct testid) as cnt_testid,
    count(distinct scmserialnumber) as cnt_modscm,
    count(distinct moduleserialnumber) as cnt_modsn
from 
    dx.dx_214_ccsampledispensepcidata
where
    '2018-11-20' <= transaction_date
and
    transaction_date < '2018-11-28';

select
    count(distinct testid) as cnt_testid,
    count(distinct scmserialnumber) as cnt_modscm,
    count(distinct moduleserialnumber) as cnt_modsn
from 
    dx.dx_210_ccdispensepm
where
    '2018-11-20' <= transaction_date
and
    transaction_date < '2018-11-28';

select
    count(distinct testid) as cnt_testid,
    count(distinct scmserialnumber) as cnt_modscm,
    count(distinct moduleserialnumber) as cnt_modsn
from 
    dx.dx_210_result
where
    '2018-11-20' <= transaction_date
and
    transaction_date < '2018-11-28';

