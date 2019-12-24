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
    rawa.wzprobe as data_series,
    rawa.pl,
    max(rawa.flaga) as flaga,
    max(rawa.maxtemp) as maxtemp,
    date_format(min(rawa.flagdatea), '%Y%m%d%H%i%s') as flag_date
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
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
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
            '<START_DATE>' <= wa.transaction_date
        and 
            wa.transaction_date < '<END_DATE>'
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
    distinct(upper(trim(dxr.architect_moduleserial))) as modulesn,
    dxr.architect_productline as pl
from
    dx.dx_architect_results dxr
where
    dxr.architect_moduleserial is not null
and
    dxr.architect_productline is not null
and
    dxr.architect_productline in ( '115', '116', '117' )
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
#
chart_data_query_template <- "
select
    chart_data.modulesn,
    chart_data.pl,
    date_format(chart_data.flag_date, '%Y%m%d%H%i%s') as flag_date,
    chart_data.wzprobea,
    max(chart_data.maxtemp) as chart_data_value
from (
    select 
        rawa.sn as modulesn,
        rawa.wzprobe as wzprobea,
        rawa.pl,
        rawa.maxtemp,
        date_trunc('hour', rawa.flagdatea) as flag_date
    from (
        select 
            wza.architect_moduleserial as sn,
            wza.architect_productline as pl,
            cast(wza.washzoneid as varchar) || '.' || wza.position as wzprobe,
            wza.eventdate_iso as flagdatea,
            wza.maxtemp
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
                '<START_DATE>' <= wa.transaction_date
            and 
                wa.transaction_date < '<END_DATE>'
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
                '<START_DATE>' <= wa.transaction_date
            and 
                wa.transaction_date < '<END_DATE>'
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
                '<START_DATE>' <= wa.transaction_date
            and 
                wa.transaction_date < '<END_DATE>'
        ) wza 
        where 
            not wza.pip_order = 'probe 3 second temp' 
        and
            not wza.pip_order = 'probe 1 first temp' 
    ) rawa
) chart_data
group by 
    chart_data.modulesn,
    chart_data.pl,
    chart_data.wzprobea,
    chart_data.flag_date
"
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
config_type <- "dx"
#
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period)
{
    if (nrow(flagged_results) > 0) {
        sn_count <- table(flagged_results[,"MODULESN"])
        #
        for (irec in 1:nrow(flagged_results)) {
            if (flagged_results[irec, "FLAG_YN"] == 1) {
                modulesn <- flagged_results[irec, "MODULESN"]
                print(sprintf("Count(%s) = %d", modulesn, sn_count[modulesn]))
                #
                if (sn_count[modulesn] >= 2) {
                    mtype <- ""
                    pl <- flagged_results[irec, "PL"]
                    if (pl == "115") {
                        mtype <- "i2"
                    } else if (pl == "116") {
                        mtype <- "i2SR"
                    } else if (pl == "117") {
                        mtype <- "i1SR"
                    }
                    flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM1 %s WZ Multi Probe", mtype)
                } else {
                    wz  <- substr(flagged_results[irec, "WZPROBEA"], 1, 1) 
                    prb <- substr(flagged_results[irec, "WZPROBEA"], 3, 3)
                    pl <- flagged_results[irec, "PL"]
                    #
                    if (pl == "115") {
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM1 WZ%s P%s", wz, prb)
                    } else if (pl == "116") {
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM1 WZ%s P%s", wz, prb)
                    } else if (pl == "117") {
                        wz <- ''
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM1 i1SR WZ%s P%s", wz, prb)
                    }
                }
            }
        }
    }
    #
    return(flagged_results)
}
