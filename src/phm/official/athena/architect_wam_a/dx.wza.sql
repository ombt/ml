    select 
        wza.architect_moduleserial as sn,
        wza.architect_productline as pl,
        cast(wza.washzoneid as varchar) || '.' || wza.position as wzprobe,
        wza.eventdate_iso as flagdatea,
        wza.maxtemp,
        case when 
                 wza.maxtemp > 35 
             and 
                 lag (wza.maxtemp) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > 35 
             and
                 lag (wza.maxtemp, 2) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > 35 
             and
                 lag (wza.maxtemp, 3) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > 35 
             and
                 lag (wza.maxtemp, 4) over 
                 (
                     partition by
                         wza.architect_moduleserial, 
                         wza.washzoneid, 
                         wza.position 
                     order by 
                         wza.eventdate_iso
                 ) > 35
             then 
                 'yes' 
             else 
                 'no' 
             end as flaga 
    from (
        select 
            upper(trim(wa.architect_moduleserial)) as architect_moduleserial,
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid - 1 as washzoneid,
            '1' as position,
            wa.position1 as replicateid,
            case when 
                     wa.position1 = lag (wa.position1) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position1, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     wa.washzoneid = lag (wa.washzoneid) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position1, 
                             wa.washzoneid, 
                             wa.eventdate_iso
                     ) 
                 and 
                     (wa.eventdate_iso - interval '10' second) < lag (wa.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
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
            wa.architect_productline is not null
        and
            wa.architect_productline in ( '115', '116', '117' )
        and
            wa.position1 > 0 
        and 
            '2019-10-24' <= wa.transaction_date
        and 
            wa.transaction_date < '2019-10-25'
        union all 
        select 
            upper(trim(wa.architect_moduleserial)),
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
            wa.architect_productline is not null
        and
            wa.architect_productline in ( '115', '116', '117' )
        and
            wa.position2 > 0 
        and 
            '2019-10-24' <= wa.transaction_date
        and 
            wa.transaction_date < '2019-10-25'
        union all 
        select 
            upper(trim(wa.architect_moduleserial)),
            wa.architect_productline,
            wa.eventdate_iso,
            wa.washzoneid -1 as washzoneid,
            '3' as position,
            wa.position3 as replicateid,
            case when 
                     wa.position3 = lag (wa.position3) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     wa.washzoneid = lag (wa.washzoneid) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
                             wa.position3, 
                             wa.washzoneid,
                             wa.eventdate_iso
                     ) 
                 and 
                     (wa.eventdate_iso - interval '10' second) < lag (wa.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wa.architect_moduleserial)),
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
            wa.architect_productline is not null
        and
            wa.architect_productline in ( '115', '116', '117' )
        and
            wa.position3 > 0 
        and 
            '2019-10-24' <= wa.transaction_date
        and 
            wa.transaction_date < '2019-10-25'
    ) wza 
    where 
        not wza.pip_order = 'probe 3 second temp' 
    and
        not wza.pip_order = 'probe 1 first temp' 
