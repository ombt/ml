#
# Alinity IA Process Path Jams 5756
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    eval1.moduleserialnumber as modulesn,
    date_format(eval2.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval1.num_retries,
    eval2.num_results
from (
    select
        upper(trim(m.moduleserialnumber)) as  moduleserialnumber,
        count(m.moduleserialnumber) as num_retries
    from
        dx.dx_205_alinity_i_messagehistory m
    where
        '<START_DATE>' <= m.transaction_date
    and 
        m.transaction_date < '<END_DATE>'
    and 
        m.aimcode = <PROCPATHJAMS_THRESHOLD_AIMCODE>
    and 
        m.aimsubcode = '<PROCPATHJAMS_THRESHOLD_AIMSUBCODE>'
    group by
        upper(trim(m.moduleserialnumber))
    ) eval1
inner join (
    select
        upper(trim(r.moduleserialnumber)) as moduleserialnumber,
        max(r.datetimestamplocal) as flag_date,
        count(r.correctedcount) as num_results
    from
        dx.dx_205_alinity_i_result r
    where
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    and 
        r.correctedcount is not null
    group by
        upper(trim(r.moduleserialnumber))
    ) eval2
on 
    eval1.moduleserialnumber = eval2.moduleserialnumber
where (
    eval2.num_results <= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES_LOW>
) or (
    eval2.num_results > <PROCPATHJAMS_THRESHOLD_NUMRESULTS>
and
    eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES_HIGH>
) "
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
use_suppression <- TRUE
#
chart_data_query_template <- "
select
    eval1.moduleserialnumber as modulesn,
    date_format(eval2.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval1.num_retries as chart_data_value,
    eval2.num_results
from (
    select
        upper(trim(m.moduleserialnumber)) as  moduleserialnumber,
        count(m.moduleserialnumber) as num_retries
    from
        dx.dx_205_alinity_i_messagehistory m
    where
        '<START_DATE>' <= m.transaction_date
    and 
        m.transaction_date < '<END_DATE>'
    and 
        m.aimcode = <PROCPATHJAMS_THRESHOLD_AIMCODE>
    and 
        m.aimsubcode = '<PROCPATHJAMS_THRESHOLD_AIMSUBCODE>'
    group by
        upper(trim(m.moduleserialnumber))
    ) eval1
inner join (
    select
        upper(trim(r.moduleserialnumber)) as moduleserialnumber,
        max(r.datetimestamplocal) as flag_date,
        count(r.correctedcount) as num_results
    from
        dx.dx_205_alinity_i_result r
    where
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    and 
        r.correctedcount is not null
    group by
        upper(trim(r.moduleserialnumber))
    ) eval2
on 
    eval1.moduleserialnumber = eval2.moduleserialnumber "
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
post_flagged_processing <- function(flagged_results, 
                                    db_conn, 
                                    params, 
                                    test_period)
{
    flagged_results$CHART_DATA_VALUE <- flagged_results$NUM_RETRIES
    #
    return(flagged_results)
}
#
generate_suppression <- function(params, 
                                 rel_db_con, 
                                 test_period)
{
    query <- "
SELECT
  DISTINCT(UPPER(CALCULATEDSN)) AS MODULESN
FROM
  TICKETHEADER TH
INNER JOIN TICKETWORKDONE TWD
  ON TH.TICKET_SQ = TWD.TICKET_SQ
WHERE 
  TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
    AND TH.BESTPL = '205'
        AND TWD.WORKDONE_CODE LIKE 'DF%' 
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}


