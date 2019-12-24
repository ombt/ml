#
# Architect WAM Pattern A
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select 
    rawa.sn as modulesn,
    rawa.wzprobe as wzprobea,
    rawa.pl,
    max(rawa.flaga) as flaga,
    min(rawa.flagdatea) as flag_date
from (
    select 
        wza.architect_moduleserial as sn,
        wza.architect_productline as pl,
        cast(wza.washzoneid as varchar) || '.' || wza.position as wzprobe,
        wza.eventdate_iso as flagdatea,
        wza.maxtemp,
        case when 
                 wza.maxtemp > <ERROR_CODE_VALUE> 
             and 
                 lag (wza.maxtemp) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE> 
             and
                 lag (wza.maxtemp, 2) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE> 
             and
                 lag (wza.maxtemp, 3) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE> 
             and
                 lag (wza.maxtemp, 4) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > <ERROR_CODE_VALUE>
             then 
                 'yes' 
             else 
                 'no' 
             end as flaga 
    from (
        select 
            wa.architect_moduleserial,
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid - 1 as washzoneid,
            '1' as position,
            wa.position1 as replicateid,
            case when 
                     wa.position1 = lag (wa.position1) over 
                     (
                         order by 
                             wa.architect_moduleserial, 
                             wa.position1, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     wa.washzoneid = lag (wa.washzoneid) over 
                     (
                         order by 
                             wa.architect_moduleserial, 
                             wa.position1, 
                             wa.washzoneid, 
                             wa.eventdate_iso
                     ) 
                 and 
                     (wa.eventdate_iso - interval '10' second) < lag (wa.eventdate_iso) over 
                     (
                         order by 
                             wa.architect_moduleserial, 
                             wa.position1, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     )
                 then 
                     'probe 1 second temp'
                 else 
                     'probe 1 first temp' 
                 end as pip_order, 
            wa.maxtempposition1/1000 maxtemp
        from 
            dx.dx_architect_wam wa
        where 
            wa.position1 > 0 
        and 
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
        union all 
        select 
            wa.architect_moduleserial,
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid -1 as washzoneid,
            '2' as position,
            wa.position2 as replicateid,
            'probe 2' as pip_order,
            wa.maxtempposition2/1000 maxtemp
        from 
            dx.dx_architect_wam wa
        where 
            wa.position2 > 0 
        and 
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
        union all 
        select 
            wa.architect_moduleserial,
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid -1 as washzoneid,
            '3' as position,
            wa.position3 as replicateid,
            case when 
                     wa.position3 = lag (wa.position3) over 
                     (
                         order by 
                             wa.architect_moduleserial, 
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     wa.washzoneid = lag (wa.washzoneid) over 
                     (
                         order by 
                             wa.architect_moduleserial, 
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     (wa.eventdate_iso - interval '10' second) < lag (wa.eventdate_iso) over 
                     (
                         order by 
                             wa.architect_moduleserial, 
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     )
                 then 
                     'probe 3 second temp'
                 else 
                     'probe 3 first temp' end 
                 as pip_order,
            wa.maxtempposition3/1000 maxtemp
        from 
            dx.dx_architect_wam wa
        where 
            wa.position3 > 0 
        and 
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
    ) wza 
    where 
        not wza.pip_order = 'probe 3 second temp' 
    and
        not wza.pip_order = 'probe 1 first temp' 
) rawa
where 
    rawa.flaga = 'yes' 
group by 
    rawa.sn, 
    rawa.pl,
    rawa.wzprobe"
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
