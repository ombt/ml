#
# Architect WAM Pattern B
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
select 
    rawb.sn as modulesn,
    rawb.wzprobe as wzprobeb,
    rawb.wzprobe as data_series,
    rawb.pl,
    max(rawb.flagb) as flagb,
    max(rawb.ambienttemp) as ambienttemp,
    date_format(min(rawb.flagdateb), '%Y%m%d%H%i%s') as flag_date
from (
    select 
        wzb.architect_moduleserial as sn,
        wzb.architect_productline as pl,
        cast(wzb.washzoneid as varchar) || '.' || wzb.position as wzprobe,
        wzb.eventdate_iso as flagdateb, 
        wzb.ambienttemp,
        case when 
                 wzb.ambienttemp < <ERROR_CODE_VALUE> 
             and 
                 lag (wzb.ambienttemp) over 
                 (
                     partition by
                         wzb.architect_moduleserial, 
                         wzb.washzoneid, 
                         wzb.position 
                     order by 
                         wzb.eventdate_iso
                 ) < <ERROR_CODE_VALUE>
             then 
                 'yes' 
             else 
                 'no' 
             end as flagb 
    from (
        select 
            upper(trim(wb.architect_moduleserial)) as architect_moduleserial,
            wb.architect_productline,
            wb.eventdate_iso, 
            wb.washzoneid -1 as washzoneid,
            '1' as position, 
            wb.position1 as replicateid,
            case when 
                     wb.position1 = lag (wb.position1) over 
                     (
                         order by 
                             upper(trim(wb.architect_moduleserial)),
                             wb.position1, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     ) 
                 and 
                     wb.washzoneid = lag (wb.washzoneid) over 
                     (
                         order by 
                             upper(trim(wb.architect_moduleserial)),
                             wb.position1, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     )
                 and 
                     (wb.eventdate_iso - interval '10' second) < lag (wb.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wb.architect_moduleserial)),
                             wb.position1, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     )
                 then 
                     'probe 1 second temp'
                 else 
                     'probe 1 first temp' 
                 end as pip_order,
            ( wb.maxtempposition1 - wb.tempdeltaposition1)/1000 as ambienttemp
        from 
            dx.dx_architect_wam wb
        where 
            wb.architect_productline is not null
        and
            wb.architect_productline in ( '115', '116', '117' )
        and
            wb.position1 > 0 
        and 
            '<START_DATE>' <= wb.transaction_date
        and 
            wb.transaction_date < '<END_DATE>'
        union all 
        select 
            upper(trim(wb.architect_moduleserial)) as architect_moduleserial,
            wb.architect_productline,
            wb.eventdate_iso,
            wb.washzoneid -1 as washzoneid,
            '2' as position,
            wb.position2 as replicateid,
            'probe 2' as pip_order,
            (wb.maxtempposition2 - wb.tempdeltaposition2)/1000 as ambienttemp
        from 
            dx.dx_architect_wam wb
        where 
            wb.architect_productline is not null
        and
            wb.architect_productline in ( '115', '116', '117' )
        and
            wb.position2 > 0 
        and 
            '<START_DATE>' <= wb.transaction_date
        and 
            wb.transaction_date < '<END_DATE>'
        union all 
        select 
            upper(trim(wb.architect_moduleserial)) as architect_moduleserial,
            wb.architect_productline,
            wb.eventdate_iso,
            wb.washzoneid -1 as washzoneid,
            '3' as position,
            wb.position3 as replicateid,
            case when 
                     wb.position3 = lag (wb.position3) over 
                     (
                         order by 
                             upper(trim(wb.architect_moduleserial)),
                             wb.position3, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     ) 
                 and 
                     wb.washzoneid = lag (wb.washzoneid) over 
                     (
                         order by 
                             upper(trim(wb.architect_moduleserial)),
                             wb.position3, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     ) 
                 and 
                     (wb.eventdate_iso - interval '10' second) < lag (wb.eventdate_iso) over 
                     (
                         order by 
                             upper(trim(wb.architect_moduleserial)),
                             wb.position3, 
                             wb.washzoneid,
                             wb.eventdate_iso
                     )
                then 
                    'probe 3 second temp'
                else 
                    'probe 3 first temp' 
                end as pip_order,
                (wb.maxtempposition3 - wb.tempdeltaposition3)/1000 as ambienttemp
        from 
            dx.dx_architect_wam wb
        where 
            wb.architect_productline is not null
        and
            wb.architect_productline in ( '115', '116', '117' )
        and
            wb.position3 > 0 
        and 
            '<START_DATE>' <= wb.transaction_date
        and 
            wb.transaction_date < '<END_DATE>'
    ) wzb 
    where 
        not wzb.pip_order = 'probe 3 second temp' 
    and
        not wzb.pip_order = 'probe 1 first temp' 
) rawb
where 
    rawb.flagb = 'yes' 
group by 
    rawb.sn, 
    rawb.pl,
    rawb.wzprobe"
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
    chart_data.wzprobeb,
    max(chart_data.ambienttemp) as chart_data_value
from (
    select 
        rawb.sn as modulesn,
        rawb.wzprobe as wzprobeb,
        rawb.pl,
        rawb.ambienttemp,
        date_trunc('hour', rawb.flagdateb) as flag_date
    from (
        select 
            wzb.architect_moduleserial as sn,
            wzb.architect_productline as pl,
            cast(wzb.washzoneid as varchar) || '.' || wzb.position as wzprobe,
            wzb.eventdate_iso as flagdateb, 
            wzb.ambienttemp
        from (
            select 
                upper(trim(wb.architect_moduleserial)) as architect_moduleserial,
                wb.architect_productline,
                wb.eventdate_iso, 
                wb.washzoneid -1 as washzoneid,
                '1' as position, 
                wb.position1 as replicateid,
                case when 
                         wb.position1 = lag (wb.position1) over 
                         (
                             order by 
                                 upper(trim(wb.architect_moduleserial)),
                                 wb.position1, 
                                 wb.washzoneid,
                                 wb.eventdate_iso
                         ) 
                     and 
                         wb.washzoneid = lag (wb.washzoneid) over 
                         (
                             order by 
                                 upper(trim(wb.architect_moduleserial)),
                                 wb.position1, 
                                 wb.washzoneid,
                                 wb.eventdate_iso
                         )
                     and 
                         (wb.eventdate_iso - interval '10' second) < lag (wb.eventdate_iso) over 
                         (
                             order by 
                                 upper(trim(wb.architect_moduleserial)),
                                 wb.position1, 
                                 wb.washzoneid,
                                 wb.eventdate_iso
                         )
                     then 
                         'probe 1 second temp'
                     else 
                         'probe 1 first temp' 
                     end as pip_order,
                ( wb.maxtempposition1 - wb.tempdeltaposition1)/1000 as ambienttemp
            from 
                dx.dx_architect_wam wb
            where 
                wb.architect_productline is not null
            and
                wb.architect_productline in ( '115', '116', '117' )
            and
                wb.position1 > 0 
            and 
                '<START_DATE>' <= wb.transaction_date
            and 
                wb.transaction_date < '<END_DATE>'
            union all 
            select 
                upper(trim(wb.architect_moduleserial)) as architect_moduleserial,
                wb.architect_productline,
                wb.eventdate_iso,
                wb.washzoneid -1 as washzoneid,
                '2' as position,
                wb.position2 as replicateid,
                'probe 2' as pip_order,
                (wb.maxtempposition2 - wb.tempdeltaposition2)/1000 as ambienttemp
            from 
                dx.dx_architect_wam wb
            where 
                wb.architect_productline is not null
            and
                wb.architect_productline in ( '115', '116', '117' )
            and
                wb.position2 > 0 
            and 
                '<START_DATE>' <= wb.transaction_date
            and 
                wb.transaction_date < '<END_DATE>'
            union all 
            select 
                upper(trim(wb.architect_moduleserial)) as architect_moduleserial,
                wb.architect_productline,
                wb.eventdate_iso,
                wb.washzoneid -1 as washzoneid,
                '3' as position,
                wb.position3 as replicateid,
                case when 
                         wb.position3 = lag (wb.position3) over 
                         (
                             order by 
                                 upper(trim(wb.architect_moduleserial)),
                                 wb.position3, 
                                 wb.washzoneid,
                                 wb.eventdate_iso
                         ) 
                     and 
                         wb.washzoneid = lag (wb.washzoneid) over 
                         (
                             order by 
                                 upper(trim(wb.architect_moduleserial)),
                                 wb.position3, 
                                 wb.washzoneid,
                                 wb.eventdate_iso
                         ) 
                     and 
                         (wb.eventdate_iso - interval '10' second) < lag (wb.eventdate_iso) over 
                         (
                             order by 
                                 upper(trim(wb.architect_moduleserial)),
                                 wb.position3, 
                                 wb.washzoneid,
                                 wb.eventdate_iso
                         )
                    then 
                        'probe 3 second temp'
                    else 
                        'probe 3 first temp' 
                    end as pip_order,
                    (wb.maxtempposition3 - wb.tempdeltaposition3)/1000 as ambienttemp
            from 
                dx.dx_architect_wam wb
            where 
                wb.architect_productline is not null
            and
                wb.architect_productline in ( '115', '116', '117' )
            and
                wb.position3 > 0 
            and 
                '<START_DATE>' <= wb.transaction_date
            and 
                wb.transaction_date < '<END_DATE>'
        ) wzb 
        where 
            not wzb.pip_order = 'probe 3 second temp' 
        and
            not wzb.pip_order = 'probe 1 first temp' 
    ) rawb
) chart_data
group by
    chart_data.modulesn,
    chart_data.pl,
    chart_data.wzprobeb,
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
                            sprintf("WAM1 %s WZ Multi Probe", mtype)
                } else {
                    wz  <- substr(flagged_results[irec, "WZPROBEB"], 1, 1) 
                    prb <- substr(flagged_results[irec, "WZPROBEB"], 3, 3)
                    pl <- flagged_results[irec, "PL"]
                    #
                    if (pl == "115") {
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM1 WZ%s P%s", wz, prb)
                    } else if (pl == "116") {
                        flagged_results[irec, "IHN_LEVEL3_DESC"] <- 
                            sprintf("WAM1 WZ%s P%s", wz, prb)
                    } else if (pl == "117") {
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
