#
# Alinity IA Process Path Jams 5756
#
#####################################################################
#
# query for generating flagged data
#
sql_query <- "
select
    eval1.moduleserialnumber as modulesn,
    date_format(eval2.flag_date,'%Y%m%d%H%i%s') as flag_date,
    eval1.num_retries,
    eval2.num_results,
    case when ((eval2.num_results >= <PROCPATHJAMS_THRESHOLD_NUMRESULTS>) and 
               (eval1.num_retries >= <PROCPATHJAMS_THRESHOLD_NUMRETRIES>))
    then 1
    else 0
    end as flagged
from (
    select
        m.moduleserialnumber,
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
        m.moduleserialnumber
    ) eval1
inner join (
    select
        r.moduleserialnumber,
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
        r.moduleserialnumber
    ) eval2
on 
    eval1.moduleserialnumber = eval2.moduleserialnumber
order by
    eval1.moduleserialnumber"
#
# number of days to check
#
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- "205"
#
# algorithm specific
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
