#
# Alinity IA Optics Dark Count
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
select
    dxr.moduleserialnumber as modulesn,
    date_format(max(dxr.datetimestamplocal),'%Y%m%d%H%i%s') as flag_date,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc,
    case when ((count(dxr.testid) >= <TESTID>) and
               (max(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_MAX>) and
               (stddev(dxr.integrateddarkcount) >= <INTEGRATEDDARKCOUNT_SD>))
    then 1
    else 0
    end as flagged
from
    dx.dx_205_alinity_i_result dxr
where
    '<START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<END_DATE>'
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= <THRESHOLDS_COUNT>
and
    upper(dxr.moduleserialnumber) like 'AI%'
group by
    dxr.moduleserialnumber
order by
    dxr.moduleserialnumber"
#
# number of days to check
#
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- "205"
#
post_processing <- function(results,
                            params, 
                            db_conn, 
                            query)
{
    return(flagged_post_processing(results, 
                                   ifelse(results$FLAGGED, 
                                          TRUE, 
                                          FALSE)))
}
#
