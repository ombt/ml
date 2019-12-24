#
# Alinity IA FE Pressure
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
select
    evals.modulesn,
    date_format(evals.flag_date,'%Y%m%d%%H%i%s') as flag_date,
    evals.mechname,
    evals.aspirations,
    evals.numflags,
    case when ((evals.aspirations >= <ASPS>) and
               ((cast (evals.numflags as double) / 
                cast (evals.aspirations as double) ) >= <PCTASPS>))
    then 1
    else 0
    end as flagged
from ( 
    select
        pm.moduleserialnumber as modulesn,
        pm.pipettormechanismname as mechname,
        max(pm.datetimestamplocal) as flag_date,
        count(pm.pipettormechanismname) as aspirations,
        sum(case when pm.frontendpressure > <MAX_VALUE> or 
                      pm.frontendpressure < <MIN_VALUE>
                 then 1
                 else 0
            end) as numflags
    from
        dx.dx_205_alinity_i_pmevent pm
    where
        '<START_DATE>' <= pm.transaction_date
    and 
        pm.transaction_date < '<END_DATE>'
    and 
        pm.frontendpressure is not null
    and 
        pm.pipettingprotocolname != 'NonPipettingProtocol'
    and 
        pm.pipettormechanismname = '<PIPMECHNAME>'
    group by
        pm.moduleserialnumber,
        pm.pipettormechanismname
    ) evals
order by
    evals.modulesn"
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
