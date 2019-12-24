#
# Architect WAM Pattern C
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select 
    rawc.sn as modulesn,
    rawc.wzprobe as wzprobec,
    rawc.pl,
    max(rawc.flagc) as flagc,
    min(rawc.flagdatec) as flag_date
from (
    select 
        wzc.architect_moduleserial as sn,
        wzc.architect_productline as pl,
        cast(wzc.washzoneid as varchar) || '.' || wzc.position as wzprobe,
        wzc.eventdate_iso as flagdatec, 
        wzc.tempdelta, 
        case when 
                 (wzc.eventdate_iso - interval '3600' second) < lag (wzc.eventdate_iso,19) over 
                 (
                     partition by 
                         wzc.architect_moduleserial, 
                         wzc.washzoneid, 
                         wzc.position
                     order by 
                         wzc.eventdate_iso
                 ) 
             then 
                 'yes' 
             else 
                 'no' 
             end as flagc 
    from ( 
        select 
            wc.architect_moduleserial, 
            wc.architect_productline, 
            wc.eventdate_iso,
            wc.washzoneid -1 as washzoneid,
            '1' as position,
            wc.position1 as replicateid,
            case when 
                     wc.position1 = lag (wc.position1) over 
                     (
                         order by 
                             wc.architect_moduleserial, 
                             wc.position1, 
                             wc.washzoneid,
                             wc.eventdate_iso
                     ) 
                 and 
                     wc.washzoneid = lag (wc.washzoneid) over 
                     (
                         order by 
                             wc.architect_moduleserial, 
                             wc.position1, 
                             wc.washzoneid,
                             wc.eventdate_iso
                     )
                 and 
                     (wc.eventdate_iso - interval '10' second) < lag (wc.eventdate_iso) over 
                     (
                         order by 
                             wc.architect_moduleserial, 
                             wc.position1, 
                             wc.washzoneid,
                             wc.eventdate_iso
                     )
                then 
                    'probe 1 second temp'
                else 
                    'probe 1 first temp' 
                end as pip_order,
            wc.tempdeltaposition1/1000 tempdelta
        from 
            dx.dx_architect_wam wc
        where 
            wc.position1 > 0 
        and 
            '<START_DATE>' <= wc.transaction_date
        and 
            wc.transaction_date < '<END_DATE>'
        union all 
        select 
            wc.architect_moduleserial,
            wc.architect_productline, 
            wc.eventdate_iso, 
            wc.washzoneid -1 as washzoneid,
            '2' as position,
            wc.position2 as replicateid,
            'probe 2' as pip_order, 
            wc.tempdeltaposition2/1000 tempdelta
        from 
            dx.dx_architect_wam wc
        where 
            wc.position2 > 0 
        and 
            '<START_DATE>' <= wc.transaction_date
        and 
            wc.transaction_date < '<END_DATE>'
        union all 
        select 
            wc.architect_moduleserial,
            wc.architect_productline, 
            wc.eventdate_iso,
            wc.washzoneid -1 as washzoneid,
            '3' as position, 
            wc.position3 as replicateid,
            case when 
                     wc.position3 = lag (wc.position3) over 
                     (
                         order by 
                             wc.architect_moduleserial, 
                             wc.position3, 
                             wc.washzoneid, 
                             wc.eventdate_iso
                     ) 
                 and 
                     wc.washzoneid = lag (wc.washzoneid) over 
                     (
                         order by 
                             wc.architect_moduleserial, 
                             wc.position3, 
                             wc.washzoneid,
                             wc.eventdate_iso
                     ) 
                 and 
                     (wc.eventdate_iso - interval '10' second) < lag (wc.eventdate_iso) over 
                     (
                         order by 
                             wc.architect_moduleserial, 
                             wc.position3, 
                             wc.washzoneid, 
                             wc.eventdate_iso
                     )
                then 
                    'probe 3 second temp'
                else 
                    'probe 3 first temp' 
                end as pip_order,
                wc.tempdeltaposition3/1000 tempdelta 
        from 
            dx.dx_architect_wam wc
        where 
            wc.position3 > 0 
        and 
            '<START_DATE>' <= wc.transaction_date
        and 
            wc.transaction_date < '<END_DATE>'
    ) wzc 
    where 
        not wzc.pip_order = 'probe 3 second temp' 
    and 
        not wzc.pip_order = 'probe 1 first temp' 
    and 
        wzc.tempdelta < <MIN_VALUE> 
) rawc
where 
    rawc.flagc = 'yes' 
group by 
    rawc.sn, 
    rawc.pl, 
    rawc.wzprobe"
#
modulesn_query_template <- "
select
    distinct(dxr.architect_moduleserial) as modulesn
from
    dx.dx_architect_results dxr
where
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
reliability_query_template <- NA
#
# number of days to check
#
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- NA
#
# configuration type, athena or spark
#
config_type <- "spark"
