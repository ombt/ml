-- select 
--     rawdata.modulesn,
--     idam.productline as pl2,
--     max(rawdata.max_completion_date) as flag_date,
--     1 as flagged
-- from (
--     select
--         r.modulesndrm as modulesn,
--         trunc (r.completiondate) as test_completion_date, 
--         max(r.completiondate) as max_completion_date,
--         stddev (r.darkcount) as std_dev_dark_count, 
--         avg (to_number (r.darkcount)) as average_dark_count
--     from
--         idaowner.results_ia r
--     where
--         r.darkcount is not null 
--     and
--         to_timestamp('07/01/2019 00:00:00', 
--                      'MM/DD/YYYY HH24:MI:SS') <= r.completiondate
--     and 
--         r.completiondate < to_timestamp('07/04/2019 00:00:00', 
--                                         'MM/DD/YYYY HH24:MI:SS')
--     group by 
--         r.modulesndrm,
--         trunc(r.completiondate) 
--     ) rawdata
-- inner join
--     idaowner.idamodules idam
-- on
--     rawdata.modulesn = idam.modulesn
-- where
--     rawdata.std_dev_dark_count >= 100
--     -- rawdata.average_dark_count >= 250
-- group by
--     rawdata.modulesn,
--     idam.productline
-- having
--     count(rawdata.modulesn) >= 2
    
select 
    rawdata.modulesn,
    rawdata.pl,
    max(rawdata.max_completion_date) as flag_date
from (
    select
        r.architect_moduleserial as modulesn,
        r.architect_productline as pl,
        date_trunc('day', r.completiondatetime_iso) as test_completion_date, 
        max(r.completiondatetime_iso) as max_completion_date,
        stddev(r.darkcount) as std_dev_dark_count, 
        avg (r.darkcount) as average_dark_count
    from
        dx.dx_architect_results r
    where
        r.darkcount is not null 
    and
        '2019-06-01' <= r.transaction_date
    and
        r.transaction_date < '2019-06-04'
    group by 
        r.architect_moduleserial,
        r.architect_productline,
        date_trunc('day', r.completiondatetime_iso)
    ) rawdata
where
    rawdata.std_dev_dark_count >= 100
and
    rawdata.average_dark_count >= 250
group by
    rawdata.modulesn,
    rawdata.pl
having
    count(rawdata.modulesn) >= 2
    
    
