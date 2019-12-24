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
    rawc.wzprobe as data_series,
    rawc.pl,
    max(rawc.flagc) as flagc,
    max(rawc.tempdelta) as tempdelta,
    date_format(min(rawc.flagdatec), '%Y%m%d%H%i%s') as flag_date
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
            upper(trim(wc.architect_moduleserial)) as architect_moduleserial, 
            wc.architect_productline, 
            wc.eventdate_iso,
            wc.washzoneid -1 as washzoneid,
            '1' as position,
            wc.position1 as replicateid,
            case when 
                     wc.position1 = lag (wc.position1) over 
                     (
                         order by 
                             upper(trim(wc.architect_moduleserial)), 
                             wc.position1, 
                             wc.washzoneid,
                             wc.eventdate_iso
                     ) 
                 and 
                     wc.washzoneid = lag (wc.washzoneid) over 
                     (
                         order by 
                             upper(trim(wc.architect_moduleserial)), 
                             wc.position1, 
                             wc.washzoneid,
                             wc.eventdate_iso
                     )
                 and 
                     (wc.eventdate_iso - interval '10' second) < lag (wc.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wc.architect_moduleserial)), 
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
            wc.architect_productline is not null
        and
            wc.architect_productline in ( '115', '116', '117' )
        and
            wc.position1 > 0 
        and 
            '<START_DATE>' <= wc.transaction_date
        and 
            wc.transaction_date < '<END_DATE>'
        union all 
        select 
            upper(trim(wc.architect_moduleserial)) as architect_moduleserial, 
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
            wc.architect_productline is not null
        and
            wc.architect_productline in ( '115', '116', '117' )
        and
            wc.position2 > 0 
        and 
            '<START_DATE>' <= wc.transaction_date
        and 
            wc.transaction_date < '<END_DATE>'
        union all 
        select 
            upper(trim(wc.architect_moduleserial)) as architect_moduleserial, 
            wc.architect_productline, 
            wc.eventdate_iso,
            wc.washzoneid -1 as washzoneid,
            '3' as position, 
            wc.position3 as replicateid,
            case when 
                     wc.position3 = lag (wc.position3) over 
                     (
                         order by 
                             upper(trim(wc.architect_moduleserial)), 
                             wc.position3, 
                             wc.washzoneid, 
                             wc.eventdate_iso
                     ) 
                 and 
                     wc.washzoneid = lag (wc.washzoneid) over 
                     (
                         order by 
                             upper(trim(wc.architect_moduleserial)), 
                             wc.position3, 
                             wc.washzoneid,
                             wc.eventdate_iso
                     ) 
                 and 
                     (wc.eventdate_iso - interval '10' second) < lag (wc.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wc.architect_moduleserial)), 
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
            wc.architect_productline is not null
        and
            wc.architect_productline in ( '115', '116', '117' )
        and
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
    chart_data.wzprobec,
    max(chart_data.tempdelta) as chart_data_value
from (
    select 
        rawc.sn as modulesn,
        rawc.wzprobe as wzprobec,
        rawc.pl,
        rawc.tempdelta,
        date_trunc('hour', rawc.flagdatec) as flag_date
    from (
        select 
            wzc.architect_moduleserial as sn,
            wzc.architect_productline as pl,
            cast(wzc.washzoneid as varchar) || '.' || wzc.position as wzprobe,
            wzc.eventdate_iso as flagdatec, 
            wzc.tempdelta
        from ( 
            select 
                upper(trim(wc.architect_moduleserial)) as architect_moduleserial, 
                wc.architect_productline, 
                wc.eventdate_iso,
                wc.washzoneid -1 as washzoneid,
                '1' as position,
                wc.position1 as replicateid,
                case when 
                         wc.position1 = lag (wc.position1) over 
                         (
                             order by 
                                 upper(trim(wc.architect_moduleserial)), 
                                 wc.position1, 
                                 wc.washzoneid,
                                 wc.eventdate_iso
                         ) 
                     and 
                         wc.washzoneid = lag (wc.washzoneid) over 
                         (
                             order by 
                                 upper(trim(wc.architect_moduleserial)), 
                                 wc.position1, 
                                 wc.washzoneid,
                                 wc.eventdate_iso
                         )
                     and 
                         (wc.eventdate_iso - interval '10' second) < lag (wc.eventdate_iso) over 
                         (
                             order by 
                                 upper(trim(wc.architect_moduleserial)), 
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
                wc.architect_productline is not null
            and
                wc.architect_productline in ( '115', '116', '117' )
            and
                wc.position1 > 0 
            and 
                '<START_DATE>' <= wc.transaction_date
            and 
                wc.transaction_date < '<END_DATE>'
            union all 
            select 
                upper(trim(wc.architect_moduleserial)) as architect_moduleserial, 
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
                wc.architect_productline is not null
            and
                wc.architect_productline in ( '115', '116', '117' )
            and
                wc.position2 > 0 
            and 
                '<START_DATE>' <= wc.transaction_date
            and 
                wc.transaction_date < '<END_DATE>'
            union all 
            select 
                upper(trim(wc.architect_moduleserial)) as architect_moduleserial, 
                wc.architect_productline, 
                wc.eventdate_iso,
                wc.washzoneid -1 as washzoneid,
                '3' as position, 
                wc.position3 as replicateid,
                case when 
                         wc.position3 = lag (wc.position3) over 
                         (
                             order by 
                                 upper(trim(wc.architect_moduleserial)), 
                                 wc.position3, 
                                 wc.washzoneid, 
                                 wc.eventdate_iso
                         ) 
                     and 
                         wc.washzoneid = lag (wc.washzoneid) over 
                         (
                             order by 
                                 upper(trim(wc.architect_moduleserial)), 
                                 wc.position3, 
                                 wc.washzoneid,
                                 wc.eventdate_iso
                         ) 
                     and 
                         (wc.eventdate_iso - interval '10' second) < lag (wc.eventdate_iso) over 
                         (
                             order by 
                                 upper(trim(wc.architect_moduleserial)), 
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
                wc.architect_productline is not null
            and
                wc.architect_productline in ( '115', '116', '117' )
            and
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
    ) rawc
) chart_data
group by 
    chart_data.modulesn,
    chart_data.pl,
    chart_data.wzprobec,
    chart_data.flag_date"
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
                            sprintf("WAM2 %s WZ Multi Probe", mtype)
                } else {
                    wz  <- substr(flagged_results[irec, "WZPROBEC"], 1, 1) 
                    prb <- substr(flagged_results[irec, "WZPROBEC"], 3, 3)
                    pl <- flagged_results[irec, "PL"]
                    #
                    if (pl == "115") {
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM2 WZ%s P%s", wz, prb)
                    } else if (pl == "116") {
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM2 WZ%s P%s", wz, prb)
                    } else if (pl == "117") {
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM2 i1SR WZ%s P%s", wz, prb)
                    }
                }
            }
        }
    }
    #
    return(flagged_results)
}
