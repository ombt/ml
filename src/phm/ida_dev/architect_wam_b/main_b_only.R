#
# Architect WAM Pattern B
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
# set working directory
#
args <- commandArgs()
scripts <- args[grepl("--file=", args)]
script_paths <- sub("^.*--file=(.*)$", "\\1", scripts)
work_dir <- dirname(script_paths[1])
#
print(sprintf("Working directory: %s", work_dir))
setwd(work_dir)
#
#####################################################################
#
# source libs
#
common_utils_path <- file.path(".", "common_utils.R")
if ( ! file.exists(common_utils_path)) {
    if (nchar(Sys.getenv("DEV_ROOT")) == 0) {
        stop("No 'common_utils.R' found")
    }
    common_utils_path <- file.path(Sys.getenv("DEV_ROOT"),
                                   "rlib",
                                   "common_utils.R")
    if ( ! file.exists(common_utils_path)) {
        stop("No DEV_ROOT 'common_utils.R' found")
    }
}
source(common_utils_path)
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
SELECT 
    RAWB.SN AS MODULESN,
    RAWB.WZPROBE AS WZPROBEB,
    IDAM.PRODUCTLINE AS PL,
    MAX(RAWB.FLAGB) AS FLAGB,
    TO_CHAR(MIN(RAWB.FLAGDATEB), 'YYYYMMDDHH24MISS') AS FLAG_DATE
FROM (
    SELECT 
        WZB.MODULESNDRM AS SN,
        WZB.WASHZONEID || '.' || WZB.POSITION AS WZPROBE,
        WZB.EVENTDATE AS FLAGDATEB, 
        WZB.AMBIENTTEMP,
        CASE WHEN 
                 WZB.AMBIENTTEMP < <MIN_VALUE> 
             AND 
                 LAG (WZB.AMBIENTTEMP) OVER 
                 (
                     PARTITION BY
                         WZB.MODULESNDRM, 
                         WZB.WASHZONEID, 
                         WZB.POSITION 
                     ORDER BY 
                         WZB.EVENTDATE
                 ) < <MIN_VALUE>
             THEN 
                 'YES' 
             ELSE 
                 'NO' 
             END AS FLAGB 
    FROM (
        SELECT 
            WB.MODULESNDRM,
            WB.EVENTDATE, 
            WB.WASHZONEID -1 AS WASHZONEID,
            '1' AS POSITION, 
            WB.POSITION1 AS REPLICATEID,
            CASE WHEN 
                     WB.POSITION1 = LAG (WB.POSITION1) OVER 
                     (
                         ORDER BY 
                             WB.MODULESNDRM, 
                             WB.POSITION1, 
                             WB.WASHZONEID,
                             WB.EVENTDATE
                     ) 
                 AND 
                     WB.WASHZONEID = LAG (WB.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WB.MODULESNDRM, 
                             WB.POSITION1, 
                             WB.WASHZONEID,
                             WB.EVENTDATE
                     )
                 AND 
                     WB.EVENTDATE -10 /(24*60*60) < LAG (WB.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WB.MODULESNDRM, 
                             WB.POSITION1, 
                             WB.WASHZONEID,
                             WB.EVENTDATE
                     )
                 THEN 
                     'Probe 1 Second Temp'
                 ELSE 
                     'Probe 1 First Temp' 
                 END AS PIP_ORDER,
            ( WB.MAXTEMPPOSITION1 - WB.TEMPDELTAPOSITION1)/1000 AS AMBIENTTEMP
        FROM 
            IDAOWNER.WASHASPIRATIONS WB 
        WHERE 
            WB.POSITION1 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WB.EVENTDATE
        AND 
            WB.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
        UNION ALL 
        SELECT 
            WB.MODULESNDRM,
            WB.EVENTDATE,
            WB.WASHZONEID -1 AS WASHZONEID,
            '2' AS POSITION,
            WB.POSITION2 AS REPLICATEID,
            'Probe 2' AS PIP_ORDER,
            (WB.MAXTEMPPOSITION2 - WB.TEMPDELTAPOSITION2)/1000 AS AMBIENTTEMP
        FROM 
            IDAOWNER.WASHASPIRATIONS WB 
        WHERE 
            WB.POSITION2 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WB.EVENTDATE
        AND 
            WB.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
        UNION ALL 
        SELECT 
            WB.MODULESNDRM, 
            WB.EVENTDATE,
            WB.WASHZONEID -1 AS WASHZONEID,
            '3' AS POSITION,
            WB.POSITION3 AS REPLICATEID,
            CASE WHEN 
                     WB.POSITION3 = LAG (WB.POSITION3) OVER 
                     (
                         ORDER BY 
                             WB.MODULESNDRM, 
                             WB.POSITION3, 
                             WB.WASHZONEID,
                             WB.EVENTDATE
                     ) 
                 AND 
                     WB.WASHZONEID = LAG (WB.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WB.MODULESNDRM, 
                             WB.POSITION3, 
                             WB.WASHZONEID,
                             WB.EVENTDATE
                     ) 
                 AND 
                     WB.EVENTDATE -10 /(24*60*60) < LAG (WB.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WB.MODULESNDRM, 
                             WB.POSITION3, 
                             WB.WASHZONEID,
                             WB.EVENTDATE
                     )
                THEN 
                    'Probe 3 Second Temp'
                ELSE 
                    'Probe 3 First Temp' 
                END AS PIP_ORDER,
                (WB.MAXTEMPPOSITION3 - WB.TEMPDELTAPOSITION3)/1000 AS AMBIENTTEMP
        FROM 
            IDAOWNER.WASHASPIRATIONS WB 
        WHERE 
            WB.POSITION3 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WB.EVENTDATE
        AND 
            WB.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
    ) WZB 
    WHERE 
        NOT WZB.PIP_ORDER = 'Probe 3 Second Temp' 
    AND
        NOT WZB.PIP_ORDER = 'Probe 1 First Temp' 
) RAWB
INNER JOIN
    IDAOWNER.IDAMODULES IDAM
ON
    RAWB.SN = IDAM.MODULESN
WHERE 
    RAWB.FLAGB = 'YES' 
GROUP BY 
    RAWB.SN, 
    RAWB.WZPROBE,
    IDAM.PRODUCTLINE"
#
post_processing <- function(results,
                            params, 
                            db_conn, 
                            query, 
                            options, 
                            test_period, 
                            flagged)
{
    #
    # assigned extra fields needed for output.
    #
    results <- results[results$FLAGB == "YES", ]
    results$PL <- results$PL2
    #
    return(flagged_post_processing(results, 
                                   ifelse(results$FLAGB == "YES", 
                                          TRUE, 
                                          FALSE)))
}
#
#####################################################################
#
# start algorithm
#
main(1, flagged_query_template, TRUE, "TBD", "ida")
#
q(status=0)
