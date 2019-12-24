#
# Alinity IA Washzone PX Aspiration
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
select
    eval.moduleserialnumber as modulesn,
    date_format(eval.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval.ratiodisptl,
    eval.numtotaldisp,
    case when ((eval.ratiodisptl >= <I_WZASP_THRESHOLD_RATIODISPTL>) and 
               (eval.numtotaldisp >= <I_WZASP_THRESHOLD_NUMTOTALDISP>))
    then 1
    else 0
    end as flagged
from (
    select
        w.moduleserialnumber,
        max(w.datetimestamplocal) as flag_date,
        count(w.emptycount) as numtotaldisp,
       (cast (sum(case when (w.emptycount - w.emptytolerance) <= <I_WZASP_THRESHOLD_NUMDISPTCTT>
                 then 1 
                 else 0 
                 end) as double)) / (cast (count(w.emptycount) as double))
            as ratiodisptl
    from
        dx.dx_205_alinity_i_wamdata w
    where
        '<START_DATE>' <= w.transaction_date
    and 
        w.transaction_date < '<END_DATE>'
    and 
        w.washzone = <I_WZASP_THRESHOLD_WZ>
    and 
        w.position = <I_WZASP_THRESHOLD_POS>
    group by
        w.moduleserialnumber
    ) eval"
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
