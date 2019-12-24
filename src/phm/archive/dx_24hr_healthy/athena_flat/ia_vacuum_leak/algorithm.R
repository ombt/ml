#
# Alinity IA Vacuum Leak
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    flagged.moduleserialnumber as modulesn,
    date_format(flagged.flag_date,'%Y%m%d%H%i%s') as flag_date,
    flagged.meanpercentdiff as meanpercentdiff,
    flagged.days_flagged
from (
    select
        evals.moduleserialnumber,
        max(evals.flag_date) as flag_date,
        max(evals.meanpercentdiff) as meanpercentdiff,
        count(*) as days_flagged
    from (
        select
            raws.moduleserialnumber,
            raws.logdate,
            max(raws.datetimestamplocal) as flag_date,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                upper(trim(vpd.moduleserialnumber)) as moduleserialnumber,
                vpd.datetimestamplocal, 
                date_trunc('day', vpd.datetimestamplocal) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                dx.dx_205_alinity_i_vacuumpressuredata vpd 
            where  
                '<START_DATE>' <= vpd.transaction_date
            and 
                vpd.transaction_date < '<END_DATE>'
            and  
                vpd.vacuumstatename = 'ConcludeLeakTest'
            and
                vpd.adcvalueleaktest is not null
            and
                vpd.adcvalue is not null
            and
                vpd.adcvalue <> 0
            ) raws
        group by
            raws.moduleserialnumber,
            raws.logdate
        ) evals
    where
        evals.meanpercentdiff >= <VACUUMLEAK_MEANDIFF>
    group by
        evals.moduleserialnumber
    ) flagged
where
    flagged.days_flagged >= <VACUUMLEAK_FLAGDAYS>"
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
    flagged.moduleserialnumber as modulesn,
    date_format(flagged.flag_date,'%Y%m%d%H%i%s') as flag_date,
    flagged.meanpercentdiff as chart_data_value,
    flagged.days_flagged
from (
    select
        evals.moduleserialnumber,
        max(evals.flag_date) as flag_date,
        max(evals.meanpercentdiff) as meanpercentdiff,
        count(*) as days_flagged
    from (
        select
            raws.moduleserialnumber,
            raws.logdate,
            max(raws.datetimestamplocal) as flag_date,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                upper(trim(vpd.moduleserialnumber)) as moduleserialnumber,
                vpd.datetimestamplocal, 
                date_trunc('day', vpd.datetimestamplocal) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                dx.dx_205_alinity_i_vacuumpressuredata vpd 
            where  
                '<MODULESN_START_DATE>' <= vpd.transaction_date
            and 
                vpd.transaction_date < '<MODULESN_END_DATE>'
            and  
                vpd.vacuumstatename = 'ConcludeLeakTest'
            and
                vpd.adcvalueleaktest is not null
            and
                vpd.adcvalue is not null
            and
                vpd.adcvalue <> 0
            ) raws
        group by
            raws.moduleserialnumber,
            raws.logdate
        ) evals
    group by
        evals.moduleserialnumber
    ) flagged"
#
# number of days to check
#
number_of_days <- 7
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
    flagged_results$CHART_DATA_VALUE <- flagged_results$MEANPERCENTDIFF
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
        AND (TWD.WORKDONE_CODE LIKE 'D3V%' 
             OR TWD.WORKDONE_CODE LIKE 'D3J%'
             OR TWD.WORKDONE_CODE LIKE 'A1V%')
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}

