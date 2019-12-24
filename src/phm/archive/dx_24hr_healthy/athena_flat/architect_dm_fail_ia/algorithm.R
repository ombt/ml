#
# Architect DM Fail IA
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
with maintenance_cte as (
select 
    ma.architect_deviceid,
    ma.architect_moduleid,
    upper(trim(ma.architect_moduleserial)) as architect_moduleserial,
    ma.architect_productline,
    ma.completiondate_iso,
    ma.result,
    case when (ma.result like 'Completed' or 
               ma.result like 'Completata' or
               ma.result like 'Finalizado' or
               ma.result like 'Terminé' or
               ma.result like 'Abgeschlossen' or
               ma.result like 'Concluído' or
               ma.result like 'Dokonceno' or
               ma.result like '??????' or
               ma.result like '??' or 
               ma.result like '???') 
         then 'complete' 
         when (ma.result like 'Failed' or
               ma.result like '??' or
               ma.result like '???' or
               ma.result like 'Fallita' or
               ma.result like 'Fallido' or
               ma.result like 'Echoué' or
               ma.result like 'Fehlgeschlagen' or
               ma.result like 'Falhado' or
               ma.result like 'Chyba' or
               ma.result like '??????')
         then 'failed'
         when (ma.result like 'User canceled' or
               ma.result like '????' or
               ma.result like '???????????' or
               ma.result like 'Annullata' or
               ma.result like 'Cancel usuario' or
               ma.result like 'Cancelado utili.' or
               ma.result like 'Annulé par utilis.' or
               ma.result like 'Benutzerabbruch' or
               ma.result like 'Zru' or
               ma.result like 'Zrueno uivatelem' or
               ma.result like '??????')
         then 'failed'
         else null 
         end as result_tr
from
    dx.dx_architect_maint ma
where
    ma.\"PROCEDURE\" like '%6041%' 
and
    '<START_DATE>' <= ma.transaction_date
and 
    ma.transaction_date < '<END_DATE>'
and
    ma.architect_productline in ( '115', '116', '117' )
order by 
    ma.architect_moduleserial,
    ma.completiondate_iso desc
),
flag_table_cte as (
select
    finaldat.architect_deviceid,
    finaldat.architect_moduleserial,
    finaldat.result_tr,
    finaldat.mindate as first_fail,
    finaldat.maxdate as last_fail,
    finaldat.cnt as n_fail,
    finaldat.n_days_with_fail,
    date_diff('day', 
              date_trunc('day', finaldat.mindate),
              date_trunc('day', date_parse('<END_DATE>', '%Y-%m-%d'))) as n_days_since_first_fail
from (
    select
        dat.architect_deviceid,
        dat.architect_moduleserial,
        dat.result_tr,
        dat.mindate,
        dat.maxdate,
        dat.cnt,
        dat.n_days_with_fail,
        row_number() over (
            partition by 
                dat.architect_moduleserial 
            order by 
                dat.maxdate desc) as rn
    from (
        select
            results.architect_deviceid,
            results.architect_moduleserial,
            results.result_tr,
            min(results.completiondate_iso) as mindate,
            max(results.completiondate_iso) as maxdate,
            count(distinct(date_trunc('day', results.completiondate_iso))) as n_days_with_fail,
            count(*) as cnt
        from (
            select
                m.*,
                row_number() over (
                    order by 
                        m.architect_moduleserial,
                        m.completiondate_iso desc
                ) as a,
                row_number() over (
                    partition by 
                        m.result_tr 
                    order by 
                        m.architect_moduleserial,
                        m.completiondate_iso desc
                ) as b,
                ( row_number() over (
                      order by 
                          m.architect_moduleserial,
                          m.completiondate_iso desc) - 
                  row_number() over (
                      partition by 
                          m.result_tr 
                      order by 
                          m.architect_moduleserial,
                          m.completiondate_iso desc) ) as di
            from (
                select 
                    *
                from
                    maintenance_cte ma
                order by 
                    ma.architect_moduleserial,
                    ma.completiondate_iso desc
            ) m
            order by 
                m.architect_moduleserial,
                m.completiondate_iso desc
        ) results
        group by
            results.architect_deviceid,
            results.architect_moduleserial,
            results.result_tr,
            results.di
        order by 
            results.architect_moduleserial
    ) dat
) finaldat
where 
    finaldat.rn = 1 
and 
    finaldat.result_tr = 'failed' 
and 
    finaldat.n_days_with_fail >= <DM_FAIL_DAYS_WITH_FAIL>
),
flagged_cte as (
select
    data.architect_moduleserial,
    data.architect_productline,
    data.flag_date,
    data.v_n_patient_since_fail,
    data.v_n_days_with_patient,
    data.v_n_days_since_last_pat,
    case when (data.v_n_patient_since_fail >= <DM_FAIL_N_PATIENT_SINCE_FAIL>) and
              (data.v_n_days_with_patient >= <DM_FAIL_DAYS_WITH_PATIENT>) and
              (data.v_n_days_since_last_pat <= <DM_FAIL_DAYS_SINCE_LAST_PAT>)
         then 'YES'
         else 'NO' end as flagged
from (
    select
        dar.architect_moduleserial,
        dar.architect_productline,
        max(dar.completiondatetime_iso) as flag_date,
        count(*) as v_n_patient_since_fail,
        count(distinct(date_trunc('day', dar.completiondatetime_iso))) as v_n_days_with_patient,
        date_diff('day', 
                  date_trunc('day', max(dar.completiondatetime_iso)),
                  date_trunc('day', date_parse('<END_DATE>', '%Y-%m-%d'))) as v_n_days_since_last_pat
    from
        dx.dx_architect_results dar
    inner join
        flag_table_cte ft
    on
        ft.architect_moduleserial = upper(trim(dar.architect_moduleserial))
    where
        dar.sampletype = 'PATIENT' 
    and
        dar.completiondatetime_iso >= ft.first_fail
    and
        dar.transaction_date >= '<START_DATE>'
    and
        dar.transaction_date < '<END_DATE>'
    and
        dar.architect_productline in ( '115', '116', '117' )
    group by
        dar.architect_moduleserial,
        dar.architect_productline
    ) data
)
select
    f.architect_moduleserial as modulesn,
    f.architect_productline as pl,
    date_format(f.flag_date,'%Y%m%d%H%i%s') as flag_date,
    f.v_n_patient_since_fail,
    f.v_n_days_with_patient,
    f.v_n_days_since_last_pat,
    f.flagged
from 
    flagged_cte f
where
    flagged = 'YES'
"
#
modulesn_query_template <- "
select distinct
    upper(trim(dxr.architect_moduleserial)) as modulesn,
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
chart_data_query_template <- NA
#
# number of days to check
#
number_of_days <- 60
#
# product line code for output file
#
product_line_code <- NA
#
# configuration type, athena or spark
#
config_type <- "dx"
#
