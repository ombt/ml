#
# Alinity IA Optics Dark Count
#
#####################################################################
#
# required libraries
#
library(getopt)
library(DBI)
library(RJDBC)
library(dplyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source libs
#
rlibpath <- Sys.getenv("PHM_ROOT")
if (nchar(rlibpath) == 0) {
    stop("PHM_ROOT not defined")
}
source(file.path(rlibpath,"rlib","common_utils.R"))
#
#####################################################################
#
# algorithm specific functions
#
chart_fields <- c("DAYS_FLAGGED")
#
flagged_query_template <- "
SELECT
    evalflags.MODULESN,
    evalflags.SevenDAYGROUP
FROM (
    SELECT
        flags.MODULESN,
        SUM(CASE WHEN trunc(flags.LogDate) >= TRUNC(SYSDATE) - 7 AND 
                      TRUNC(flags.LogDate) < TRUNC(SYSDATE)
                 THEN 1
                 ELSE 0
                 END) AS SevenDAYGROUP
    FROM (
        SELECT
            evals.MODULESN,
            evals.LogDate,
            evals.meanPercentDiff
        FROM (
            SELECT
                raws.MODULESN,
                (trunc(raws.LOGDATE_LOCAL)) AS LogDate,
                AVG(raws.PercentDiff) AS meanPercentDiff
            FROM (
                SELECT
                    t1.MODULESN, 
                    t1.LOGDATE_LOCAL, 
                    t1.VACUUMSTATENAME, 
                    t1.VERIFYVACUUMSUBSTATENAME, 
                    100*(t1.ADCVALUELEAKTEST-t1.ADCVALUE)/t1.ADCVALUE AS PercentDiff, 
                    t1.ADCVALUE, 
                    t1.ADCVALUELEAKTEST
                FROM 
                    IDAQOWNER.ICQ_VACUUMPRESSUREDATA  t1 
                WHERE  
                    t1.LOGDATE_LOCAL >= SYSDATE-7  
                AND  
                    t1.VACUUMSTATENAME =  'ConcludeLeakTest'
                ) raws
            GROUP BY
                raws.MODULESN,
                trunc(raws.LOGDATE_LOCAL) 
            ) )evals
        WHERE
            meanPercentDiff>=<VACUUMLEAK_MEANDIFF>
        ) flags 
    GROUP BY
        flags.MODULESN
    ) evalFlags
WHERE 
    evalFlags.SevenDAYGROUP>=<VACUUMLEAK_FLAGDAYS

flagged_query_template <- "
select
    flagged.modulesn,
    flagged.days_flagged
from (
    select
        evals.modulesn,
        count(*) as days_flagged
    from (
        select
            raws.modulesn,
            raws.logdate,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                vpd.modulesn, 
                trunc(vpd.logdate_local) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                idaqowner.icq_vacuumpressuredata vpd 
            where  
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
            and 
                vpd.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
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
            raws.modulesn,
            raws.logdate
        ) evals
    where
        evals.meanpercentdiff >= <VACUUMLEAK_MEANDIFF>
    group by
        evals.modulesn
    ) flagged
where
    flagged.days_flagged >= <VACUUMLEAK_FLAGDAYS>
order by
    flagged.modulesn"
#
not_flagged_query_template <- "
select
    flagged.modulesn,
    flagged.days_flagged
from (
    select
        evals.modulesn,
        count(*) as days_flagged
    from (
        select
            raws.modulesn,
            raws.logdate,
            avg(raws.percentdiff) as meanpercentdiff
        from (
            select
                vpd.modulesn, 
                trunc(vpd.logdate_local) as logdate,
                100*(vpd.adcvalueleaktest-vpd.adcvalue)/vpd.adcvalue as percentdiff
            from 
                idaqowner.icq_vacuumpressuredata vpd 
            where  
                to_timestamp('<START_DATE>', 
                             'MM/DD/YYYY HH24:MI:SS') <= vpd.logdate_local
            and 
                vpd.logdate_local < to_timestamp('<END_DATE>', 
                                                 'MM/DD/YYYY HH24:MI:SS')
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
            raws.modulesn,
            raws.logdate
        ) evals
    where
        not ( evals.meanpercentdiff >= <VACUUMLEAK_MEANDIFF> )
    group by
        evals.modulesn
    ) flagged
order by
    flagged.modulesn"
#
#####################################################################
#
# start algorithm
#
main("ida", default_last_week, flagged_query_template, not_flagged_query_template, chart_fields)
#
q(status=0)

