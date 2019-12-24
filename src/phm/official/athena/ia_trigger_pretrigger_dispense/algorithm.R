#
# Alinity Trigger Pretrigger Dispense
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select 
    proc3.deviceid, 
    proc3.modulesn, 
    proc3.pl,
    proc3.date_only as flag_date,
    count(*) as rec_count,
    avg(proc3.shapeflag) as avg_shapeflag
from (
    select
        proc2.deviceid, 
        proc2.modulesn, 
        proc2.pl,
        date_format(date_trunc('day', proc2.datetimestamp),
                    '%Y%m%d%H%i%s') as date_only,
        case when ((proc2.correctedcount <= 30) and 
                   ((proc2.peak_adj_signal / proc2.dark_adj_signal) <= 0.44)) or
                  ((proc2.correctedcount <= 50) and 
                   ((proc2.peak_adj_signal / proc2.dark_adj_signal) <= 0.40)) or
                  ((proc2.correctedcount <= 70) and 
                   ((proc2.peak_adj_signal / proc2.dark_adj_signal) <= 0.35))
             then 1
             else 0
             end as shapeflag
    from (
        select
            proc.deviceid,
            proc.modulesn,
            proc.pl,
            proc.datetimestamp,
            proc.correctedcount,
            sum(proc.dsr_4 +
                proc.dsr_5 +
                proc.dsr_6 +
                proc.dsr_7) as peak_adj_signal,
            sum(proc.dsr_1 +
                proc.dsr_2 +
                proc.dsr_3 +
                proc.dsr_4 +
                proc.dsr_5 +
                proc.dsr_6 +
                proc.dsr_7 +
                proc.dsr_8 +
                proc.dsr_9 +
                proc.dsr_10 +
                proc.dsr_11 +
                proc.dsr_12 +
                proc.dsr_13 +
                proc.dsr_14 +
                proc.dsr_15 +
                proc.dsr_16 +
                proc.dsr_17 +
                proc.dsr_18 +
                proc.dsr_19 +
                proc.dsr_20 +
                proc.dsr_21 +
                proc.dsr_22 +
                proc.dsr_23 +
                proc.dsr_24 +
                proc.dsr_25 +
                proc.dsr_26 +
                proc.dsr_27 +
                proc.dsr_28 +
                proc.dsr_29 +
                proc.dsr_30) as dark_adj_signal
        from (
            select
                raw.deviceid,
                raw.modulesn,
                raw.pl,
                raw.datetimestamp,
                raw.correctedcount,
                case when cast (raw.dsr_array[1] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[1] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_1,
                case when cast (raw.dsr_array[2] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[2] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_2,
                case when cast (raw.dsr_array[3] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[3] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_3,
                case when cast (raw.dsr_array[4] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[4] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_4,
                case when cast (raw.dsr_array[5] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[5] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_5,
                case when cast (raw.dsr_array[6] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[6] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_6,
                case when cast (raw.dsr_array[7] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[7] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_7,
                case when cast (raw.dsr_array[8] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[8] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_8,
                case when cast (raw.dsr_array[9] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[9] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_9,
                case when cast (raw.dsr_array[10] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[10] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_10,
                case when cast (raw.dsr_array[11] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[11] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_11,
                case when cast (raw.dsr_array[12] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[12] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_12,
                case when cast (raw.dsr_array[13] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[13] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_13,
                case when cast (raw.dsr_array[14] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[14] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_14,
                case when cast (raw.dsr_array[15] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[15] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_15,
                case when cast (raw.dsr_array[16] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[16] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_16,
                case when cast (raw.dsr_array[17] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[17] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_17,
                case when cast (raw.dsr_array[18] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[18] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_18,
                case when cast (raw.dsr_array[19] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[19] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_19,
                case when cast (raw.dsr_array[20] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[20] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_20,
                case when cast (raw.dsr_array[21] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[21] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_21,
                case when cast (raw.dsr_array[22] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[22] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_22,
                case when cast (raw.dsr_array[23] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[23] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_23,
                case when cast (raw.dsr_array[24] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[24] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_24,
                case when cast (raw.dsr_array[25] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[25] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_25,
                case when cast (raw.dsr_array[26] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[26] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_26,
                case when cast (raw.dsr_array[27] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[27] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_27,
                case when cast (raw.dsr_array[28] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[28] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_28,
                case when cast (raw.dsr_array[29] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[29] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_29,
                case when cast (raw.dsr_array[30] as double) > raw.darkaverage
                     then
                         cast (raw.dsr_array[30] as double) - raw.darkaverage
                     else
                         0
                     end as dsr_30
            from (
                select 
                    ir.deviceid,
                    upper(trim(ir.moduleserialnumber)) as modulesn,
                    ir.productline as pl,
                    ir.datetimestamp,
                    ir.correctedcount,
                    ir.darkaverage,
                    ir.darksignalreads,
                    split(ir.darksignalreads, ',', 30) as dsr_array
                from 
                    dx.dx_205_alinity_i_result ir
                where 
                    '<START_DATE>' <= ir.transaction_date
                and 
                    ir.transaction_date < '<END_DATE>'
                and
                    lower(ir.sampleid) not like '%saline%'
                and
                    lower(ir.sampleid) not like '%buf%'
                and
                    lower(ir.operatorid) not like 'fse'
                and
                    (cast (ir.assaynumber as varchar)) not like '%213%'
                and
                    (cast (ir.assaynumber as varchar)) not like '%216%'            
            ) raw
        ) proc
        group by
            proc.deviceid,
            proc.modulesn,
            proc.pl,
            proc.datetimestamp,
            proc.correctedcount
    ) proc2 
) proc3
group by
    proc3.deviceid, 
    proc3.modulesn, 
    proc3.pl,
    proc3.date_only
having
    count(*) >= 50 
and
    avg(proc3.shapeflag) > 0.01
order by
    proc3.deviceid, 
    proc3.modulesn, 
    proc3.pl,
    proc3.date_only
"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
from
    dx.dx_205_alinity_i_result dxr
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
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- "205"
#
# configuration type, athena or spark
#
config_type <- "dx"
#
use_suppression <- FALSE
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
  TH.SERVICE_CLOSED_DT >= DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '205'
        AND ( TWD.WORKDONE_CODE LIKE 'CE%' OR
              TWD.WORKDONE_CODE LIKE 'CD%' OR
              TWD.WORKDONE_CODE LIKE 'CS%' )
) work
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}
