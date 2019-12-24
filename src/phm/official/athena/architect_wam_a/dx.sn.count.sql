-- select 
--     upper(trim(wa.architect_moduleserial)) as sn,
--     count(trim(wa.architect_moduleserial)) as cnt_sn
-- from 
--     dx.dx_architect_wam wa
-- where 
--     wa.architect_productline is not null
-- and
--     wa.architect_productline in ( '115', '116', '117' )
-- and
--     '2019-10-24' <= wa.transaction_date
-- and 
--     wa.transaction_date < '2019-10-25'
-- and 
--     upper(trim(wa.architect_moduleserial)) in
--     (
--         'I1SR03166', 'I1SR03267', 'I1SR03442', 'I1SR50784', 'I1SR60147',
--         'I201838', 'I201981', 'I202245', 'ISR01402', 'ISR01730',
--         'ISR01831', 'ISR02112', 'ISR02137', 'ISR02287', 'ISR02314',
--         'ISR02803', 'ISR02854', 'ISR03500', 'ISR03507', 'ISR04173',
--         'ISR04441', 'ISR04703', 'ISR05216', 'ISR05250', 'ISR05294',
--         'ISR05590', 'ISR05679', 'ISR05868', 'ISR05869', 'ISR06051',
--         'ISR06065', 'ISR06615', 'ISR06834', 'ISR07052', 'ISR07085',
--         'ISR07425', 'ISR07604', 'ISR07791', 'ISR50321', 'ISR50339',
--         'ISR50362', 'ISR50779', 'ISR50934', 'ISR51038', 'ISR51425',
--         'ISR51769', 'ISR51792', 'ISR52133', 'ISR52233', 'ISR52606',
--         'ISR52904', 'ISR54788', 'ISR54793', 'ISR55012', 'ISR55193',
--         'ISR55199', 'ISR55323', 'ISR55366', 'ISR55585', 'ISR55928',
--         'ISR55952', 'ISR56080', 'ISR60172', 'ISR60283', 'ISR60330',
--         'ISR60758', 'ISR61047', 'ISR61913', 'ISR62542', 'ISR63597'
--     )
-- group by
--     upper(trim(wa.architect_moduleserial))
-- order by
--     upper(trim(wa.architect_moduleserial)) 


-- select 
--     upper(trim(wa.architect_moduleserial)) as sn,
--     count(trim(wa2.architect_moduleserial)) as cnt_sn
-- from 
--     dx.dx_architect_wam wa
-- left join
--     dx.dx_architect_wam wa2
-- on
--     upper(trim(wa.architect_moduleserial)) = upper(trim(wa2.architect_moduleserial))
-- and
--     wa2.architect_productline is not null
-- and
--     wa2.architect_productline in ( '115', '116', '117' )
-- and
--     '2019-10-24' <= wa2.transaction_date
-- and 
--     wa2.transaction_date < '2019-10-25'
-- where 
--     wa.architect_productline is not null
-- and
--     wa.architect_productline in ( '115', '116', '117' )
-- and
--     '2019-10-24' <= wa.transaction_date
-- and 
--     wa.transaction_date < '2019-10-25'
-- and 
--     upper(trim(wa.architect_moduleserial)) in
--     (
--         'I1SR03166', 'I1SR03267', 'I1SR03442', 'I1SR50784', 'I1SR60147',
--         'I201838', 'I201981', 'I202245', 'ISR01402', 'ISR01730',
--         'ISR01831', 'ISR02112', 'ISR02137', 'ISR02287', 'ISR02314',
--         'ISR02803', 'ISR02854', 'ISR03500', 'ISR03507', 'ISR04173',
--         'ISR04441', 'ISR04703', 'ISR05216', 'ISR05250', 'ISR05294',
--         'ISR05590', 'ISR05679', 'ISR05868', 'ISR05869', 'ISR06051',
--         'ISR06065', 'ISR06615', 'ISR06834', 'ISR07052', 'ISR07085',
--         'ISR07425', 'ISR07604', 'ISR07791', 'ISR50321', 'ISR50339',
--         'ISR50362', 'ISR50779', 'ISR50934', 'ISR51038', 'ISR51425',
--         'ISR51769', 'ISR51792', 'ISR52133', 'ISR52233', 'ISR52606',
--         'ISR52904', 'ISR54788', 'ISR54793', 'ISR55012', 'ISR55193',
--         'ISR55199', 'ISR55323', 'ISR55366', 'ISR55585', 'ISR55928',
--         'ISR55952', 'ISR56080', 'ISR60172', 'ISR60283', 'ISR60330',
--         'ISR60758', 'ISR61047', 'ISR61913', 'ISR62542', 'ISR63597'
--     )
-- group by
--     upper(trim(wa.architect_moduleserial))
-- order by
--     upper(trim(wa.architect_moduleserial)) 


-- select 
    -- sn
-- from (
-- left join
--     dx.dx_architect_wam wa2
-- on
--     upper(trim(wa.architect_moduleserial)) = upper(trim(wa2.architect_moduleserial))
-- and
--     wa2.architect_productline is not null
-- and
--     wa2.architect_productline in ( '115', '116', '117' )
-- and
--     '2019-10-24' <= wa2.transaction_date
-- and 
--     wa2.transaction_date < '2019-10-25'
-- where 
--     wa.architect_productline is not null
-- and
--     wa.architect_productline in ( '115', '116', '117' )
-- and
--     '2019-10-24' <= wa.transaction_date
-- and 
--     wa.transaction_date < '2019-10-25'
-- and 
--     upper(trim(wa.architect_moduleserial)) in
--     (
--         'I1SR03166', 'I1SR03267', 'I1SR03442', 'I1SR50784', 'I1SR60147',
--         'I201838', 'I201981', 'I202245', 'ISR01402', 'ISR01730',
--         'ISR01831', 'ISR02112', 'ISR02137', 'ISR02287', 'ISR02314',
--         'ISR02803', 'ISR02854', 'ISR03500', 'ISR03507', 'ISR04173',
--         'ISR04441', 'ISR04703', 'ISR05216', 'ISR05250', 'ISR05294',
--         'ISR05590', 'ISR05679', 'ISR05868', 'ISR05869', 'ISR06051',
--         'ISR06065', 'ISR06615', 'ISR06834', 'ISR07052', 'ISR07085',
--         'ISR07425', 'ISR07604', 'ISR07791', 'ISR50321', 'ISR50339',
--         'ISR50362', 'ISR50779', 'ISR50934', 'ISR51038', 'ISR51425',
--         'ISR51769', 'ISR51792', 'ISR52133', 'ISR52233', 'ISR52606',
--         'ISR52904', 'ISR54788', 'ISR54793', 'ISR55012', 'ISR55193',
--         'ISR55199', 'ISR55323', 'ISR55366', 'ISR55585', 'ISR55928',
--         'ISR55952', 'ISR56080', 'ISR60172', 'ISR60283', 'ISR60330',
--         'ISR60758', 'ISR61047', 'ISR61913', 'ISR62542', 'ISR63597'
--     )
-- group by
--     upper(trim(wa.architect_moduleserial))
-- order by
--     upper(trim(wa.architect_moduleserial)) 

select
    sns.sn,
    count(upper(trim(wa.architect_moduleserial))) as sn_count
from (
    select 'I1SR02908' as sn
    union select 'I1SR02936' as sn
    union select 'I1SR03166' as sn
    union select 'I1SR03197' as sn
    union select 'I1SR03267' as sn
    union select 'I1SR03378' as sn
    union select 'I1SR03442' as sn
    union select 'I1SR03746' as sn
    union select 'I1SR03846' as sn
    union select 'I1SR50033' as sn
    union select 'I1SR50810' as sn
    union select 'I1SR50957' as sn
    union select 'I1SR51102' as sn
    union select 'I1SR51467' as sn
    union select 'I1SR52016' as sn
    union select 'I1SR52068' as sn
    union select 'I1SR52398' as sn
    union select 'I1SR52469' as sn
    union select 'I1SR53026' as sn
    union select 'I1SR53085' as sn
    union select 'I1SR53306' as sn
    union select 'I1SR53920' as sn
    union select 'I1SR54263' as sn
    union select 'I1SR55027' as sn
    union select 'I1SR55311' as sn
    union select 'I1SR56303' as sn
    union select 'I1SR56469' as sn
    union select 'I1SR56543' as sn
    union select 'I1SR56610' as sn
    union select 'I1SR60147' as sn
    union select 'I1SR60619' as sn
    union select 'I1SR60749' as sn
    union select 'ISR01017' as sn
    union select 'ISR01628' as sn
    union select 'ISR02803' as sn
    union select 'ISR02863' as sn
    union select 'ISR02871' as sn
    union select 'ISR03183' as sn
    union select 'ISR03283' as sn
    union select 'ISR03599' as sn
    union select 'ISR03608' as sn
    union select 'ISR03776' as sn
    union select 'ISR03985' as sn
    union select 'ISR04177' as sn
    union select 'ISR05836' as sn
    union select 'ISR06051' as sn
    union select 'ISR07225' as sn
    union select 'ISR07359' as sn
    union select 'ISR07547' as sn
    union select 'ISR07604' as sn
    union select 'ISR08177' as sn
    union select 'ISR08382' as sn
    union select 'ISR50114' as sn
    union select 'ISR50325' as sn
    union select 'ISR50353' as sn
    union select 'ISR50516' as sn
    union select 'ISR50600' as sn
    union select 'ISR50633' as sn
    union select 'ISR50779' as sn
    union select 'ISR50883' as sn
    union select 'ISR51463' as sn
    union select 'ISR51534' as sn
    union select 'ISR51593' as sn
    union select 'ISR51652' as sn
    union select 'ISR51769' as sn
    union select 'ISR51922' as sn
    union select 'ISR52133' as sn
    union select 'ISR52288' as sn
    union select 'ISR52869' as sn
    union select 'ISR52890' as sn
    union select 'ISR53166' as sn
    union select 'ISR53323' as sn
    union select 'ISR53580' as sn
    union select 'ISR53992' as sn
    union select 'ISR54100' as sn
    union select 'ISR54245' as sn
    union select 'ISR54258' as sn
    union select 'ISR54380' as sn
    union select 'ISR54498' as sn
    union select 'ISR54572' as sn
    union select 'ISR54607' as sn
    union select 'ISR54639' as sn
    union select 'ISR54669' as sn
    union select 'ISR54752' as sn
    union select 'ISR55147' as sn
    union select 'ISR55193' as sn
    union select 'ISR55309' as sn
    union select 'ISR55573' as sn
    union select 'ISR56080' as sn
    union select 'ISR60012' as sn
    union select 'ISR60041' as sn
    union select 'ISR60343' as sn
    union select 'ISR60522' as sn
    union select 'ISR60855' as sn
    union select 'ISR60993' as sn
    union select 'ISR61043' as sn
    union select 'ISR61161' as sn
    union select 'ISR61258' as sn
    union select 'ISR61466' as sn
    union select 'ISR61542' as sn
    union select 'ISR61598' as sn
    union select 'ISR61741' as sn
    union select 'ISR61774' as sn
    union select 'ISR61913' as sn
    union select 'ISR62034' as sn
    union select 'ISR62058' as sn
    union select 'ISR62065' as sn
    union select 'ISR62069' as sn
    union select 'ISR62442' as sn
    union select 'ISR62668' as sn
    union select 'ISR62740' as sn
    union select 'ISR63106' as sn
    union select 'ISR63597' as sn
    ) sns
left join
    dx.dx_architect_wam wa
on
    upper(trim(wa.architect_moduleserial)) = sns.sn
and
    wa.architect_productline is not null
and
    wa.architect_productline in ( '115', '116', '117' )
and
    '2019-10-24' <= wa.transaction_date
and 
    wa.transaction_date < '2019-10-25'
group by
    sns.sn
order by
    count(upper(trim(wa.architect_moduleserial))),
    sns.sn
