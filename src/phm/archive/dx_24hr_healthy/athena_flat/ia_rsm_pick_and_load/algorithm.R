# Alinity IA RSM Pick and Load
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    deviceid, 
    upper(trim(instrument)) as modulesn,
    date_format(flag_date,'%Y%m%d%H%i%s') as flag_date,
    frac_recover,
    frac_engage,
    perday_recover,
    perday_engage
from (
    select 
        deviceid, 
        instrument,
        flag_date,
        case when num_recover > 0 
             then num_recover / num_rsm_move
             else 0 
             end as frac_recover,
        case when num_engage > 0 
             then num_engage / num_rsm_move
             else 0 
             end as frac_engage,
        case when num_recover > 0 
             then num_recover / num_days
             else 0 
             end as perday_recover,
        case when num_engage > 0 
             then num_engage / num_days
             else 0 
             end as perday_engage
    from (
        select 
            deviceid, 
            instrument, 
            min(flag_date) as flag_date,
            count(day) as num_days,
            sum(num_retry - 2*num_exceed) as num_recover,
            sum(num_engage) as num_engage,
            sum(num_scans + num_retry - num_exceed) as num_rsm_move
        from (
            select 
                date_trunc('day', ia.datetimestamplocal) as day,
                ia.deviceid,
                ia.scmserialnumber as instrument,
                min(ia.datetimestamplocal) as flag_date,
                sum(case when component = 'CarrierScheduler: CarrierScanned'
                         then 1 
                         else 0 
                         end) as num_scans,
                sum(case when component like '%Load%Pick%' and activity like 'Retry%'
                         then 1 
                         else 0 
                         end) as num_retry,
                sum(case when component like '%Load%Pick%' and activity like 'Exceed%'
                         then 1 
                         else 0 
                         end) as num_exceed,
                sum(case when component like '%Load%Pick%' and activity like '%engagement%'
                         then 1 
                         else 0 
                         end) as num_engage
            from 
                dx.dx_214_alinity_ci_instrumentactivity ia
            where 
                ia.scmserialnumber like 'SCM%'
            and
                '<START_DATE>' <= ia.transaction_date
            and 
                ia.transaction_date < '<END_DATE>'
            group by
                date_trunc('day', ia.datetimestamplocal),
                ia.deviceid,
                ia.scmserialnumber
        )
        group by
            deviceid, 
            instrument
    )
)
where 
    (2.3 * frac_recover + 
     2.6 * frac_engage + 
     0.68 * perday_recover + 
     0.85 * perday_engage) >= 3.97
order by 
    deviceid, 
    instrument
"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
from
    dx.dx_214_alinity_ci_result dxr
where
    dxr.moduleserialnumber is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
chart_data_query_template <- NA
#
# number of days to check
#
number_of_days <- 7
#
# product line code for output file
#
product_line_code <- "214"
#
# configuration type, athena or spark
#
config_type <- "dx"
#
use_suppression <- TRUE
#
generate_suppression <- function(params, 
                                 rel_db_con, 
                                 test_period)
{
query <- "
select
    distinct(work.sn) as modulesn
from (
SELECT
  DISTINCT(UPPER(CALCULATEDSN)) AS SN
FROM
  TICKETHEADER TH
INNER JOIN TICKETWORKDONE TWD
  ON TH.TICKET_SQ = TWD.TICKET_SQ
WHERE 
  TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '214'
        AND TWD.WORKDONE_CODE LIKE 'DA%' 
) work
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}
