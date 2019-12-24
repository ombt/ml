#
# Alinity IA ITV Generic
#
#####################################################################
#
# query templates
#
flagged_query_template <- "
select
    evals.moduleserialnumber as modulesn,
    evals.mean_pwmvalue,
    date_format(evals.flag_date,'%Y%m%d%H%i%s') as flag_date
from (
    select
        upper(trim(i.moduleserialnumber)) as moduleserialnumber,
        i.itvmechanismname,
        max(i.datetimestamplocal) as flag_date,
        avg(cast (i.pwmvalue as double)) as mean_pwmvalue
    from 
        dx.dx_205_alinity_i_itvdata i
    where
        '<START_DATE>' <= i.transaction_date
    and 
        i.transaction_date < '<END_DATE>'
    and 
        i.actualspeed != <I_ITV_THRESHOLD_ACTSPD>
    and 
        i.requestedspeed = <I_ITV_THRESHOLD_REQSPD>
    and 
        i.itvmechanismname = '<I_ITV_THRESHOLD_ITVMECHNAME>'
    and (
        i.timetoengage is null 
    or
        i.timetodisengage is null
    )
    group by
        upper(trim(i.moduleserialnumber)),
        i.itvmechanismname
    ) evals
where
    evals.mean_pwmvalue >= <I_ITV_THRESHOLD_MEANPWM>"
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

use_suppression <- TRUE
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
INNER JOIN 
    TICKETWORKDONE TWD
ON 
    TH.TICKET_SQ = TWD.TICKET_SQ
WHERE 
    TH.SERVICE_CLOSED_DT >=  DATEADD(DAY, -45, CAST(GETDATE() AS DATE))
AND 
    TH.BESTPL = '205'
AND (
    TWD.WORKDONE_CODE LIKE 'C8%' OR
    TWD.WORKDONE_CODE LIKE 'C9%' OR
    TWD.WORKDONE_CODE LIKE 'CJ%' OR
    TWD.WORKDONE_CODE LIKE 'CI%' 
)
"
    #
    return(exec_query(params, rel_db_con, query, test_period))
}
