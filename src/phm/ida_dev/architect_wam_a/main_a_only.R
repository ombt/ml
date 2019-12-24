#
# Architect WAM Pattern A
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
    RAWA.SN AS MODULESN,
    RAWA.WZPROBE AS WZPROBEA,
    IDAM.PRODUCTLINE AS PL2,
    MAX(RAWA.FLAGA) AS FLAGA,
    TO_CHAR(MIN(RAWA.FLAGDATEA), 'YYYYMMDDHH24MISS') AS FLAG_DATE
FROM (
    SELECT 
        WZA.MODULESNDRM AS SN,
        WZA.WASHZONEID || '.' || WZA.POSITION AS WZPROBE,
        WZA.EVENTDATE AS FLAGDATEA,
        WZA.MAXTEMP,
        CASE WHEN 
                 WZA.MAXTEMP > <ERROR_CODE_VALUE> 
             AND 
                 LAG (WZA.MAXTEMP) OVER 
                 (
                     PARTITION BY
                         WZA.MODULESNDRM, 
                         WZA.WASHZONEID, 
                         WZA.POSITION 
                     ORDER BY 
                         WZA.EVENTDATE
                 ) > <ERROR_CODE_VALUE> 
             AND
                 LAG (WZA.MAXTEMP, 2) OVER 
                 (
                     PARTITION BY
                         WZA.MODULESNDRM, 
                         WZA.WASHZONEID, 
                         WZA.POSITION 
                     ORDER BY 
                         WZA.EVENTDATE
                 ) > <ERROR_CODE_VALUE> 
             AND
                 LAG (WZA.MAXTEMP, 3) OVER 
                 (
                     PARTITION BY
                         WZA.MODULESNDRM, 
                         WZA.WASHZONEID, 
                         WZA.POSITION 
                     ORDER BY 
                         WZA.EVENTDATE
                 ) > <ERROR_CODE_VALUE> 
             AND
                 LAG (WZA.MAXTEMP, 4) OVER 
                 (
                     PARTITION BY
                         WZA.MODULESNDRM, 
                         WZA.WASHZONEID, 
                         WZA.POSITION 
                     ORDER BY 
                         WZA.EVENTDATE
                 ) > <ERROR_CODE_VALUE>
             THEN 
                 'YES' 
             ELSE 
                 'NO' 
             END AS FLAGA 
    FROM ( 
        SELECT 
            WA.MODULESNDRM,
            WA.EVENTDATE,
            WA.WASHZONEID -1 AS WASHZONEID,
            '1' AS POSITION,
            WA.POSITION1 AS REPLICATEID,
            CASE WHEN 
                     WA.POSITION1 = LAG (WA.POSITION1) OVER 
                     (
                         ORDER BY 
                             WA.MODULESNDRM, 
                             WA.POSITION1, 
                             WA.WASHZONEID,
                             WA.EVENTDATE
                     ) 
                 AND 
                     WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WA.MODULESNDRM, 
                             WA.POSITION1, 
                             WA.WASHZONEID, 
                             WA.EVENTDATE
                     ) 
                 AND 
                     WA.EVENTDATE - 10 /(24*60*60) < 
                     LAG (WA.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WA.MODULESNDRM, 
                             WA.POSITION1, 
                             WA.WASHZONEID,
                             WA.EVENTDATE
                     )
                 THEN 
                     'Probe 1 Second Temp'
                 ELSE 
                     'Probe 1 First Temp' 
                 END AS PIP_ORDER, 
            WA.MAXTEMPPOSITION1/1000 MAXTEMP
        FROM 
            IDAOWNER.WASHASPIRATIONS WA 
        WHERE 
            WA.POSITION1 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WA.EVENTDATE
        AND 
            WA.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
        UNION ALL 
        SELECT 
            WA.MODULESNDRM,
            WA.EVENTDATE,
            WA.WASHZONEID -1 AS WASHZONEID,
            '2' AS POSITION,
            WA.POSITION2 AS REPLICATEID,
            'Probe 2' AS PIP_ORDER,
            WA.MAXTEMPPOSITION2/1000 MAXTEMP
        FROM 
            IDAOWNER.WASHASPIRATIONS WA 
        WHERE 
            WA.POSITION2 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WA.EVENTDATE
        AND 
            WA.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
        UNION ALL 
        SELECT 
            WA.MODULESNDRM,
            WA.EVENTDATE,
            WA.WASHZONEID -1 AS WASHZONEID,
            '3' AS POSITION,
            WA.POSITION3 AS REPLICATEID,
            CASE WHEN 
                     WA.POSITION3 = LAG (WA.POSITION3) OVER 
                     (
                         ORDER BY 
                             WA.MODULESNDRM, 
                             WA.POSITION3, 
                             WA.WASHZONEID,
                             WA.EVENTDATE
                     ) 
                 AND 
                     WA.WASHZONEID = LAG (WA.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WA.MODULESNDRM, 
                             WA.POSITION3, 
                             WA.WASHZONEID,
                             WA.EVENTDATE
                     ) 
                 AND 
                     WA.EVENTDATE -10 /(24*60*60) < LAG (WA.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WA.MODULESNDRM, 
                             WA.POSITION3, 
                             WA.WASHZONEID,
                             WA.EVENTDATE
                     )
                 THEN 
                     'Probe 3 Second Temp'
                 ELSE 
                     'Probe 3 First Temp' END 
                 AS PIP_ORDER,
            WA.MAXTEMPPOSITION3/1000 MAXTEMP
        FROM 
            IDAOWNER.WASHASPIRATIONS WA 
        WHERE 
            WA.POSITION3 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WA.EVENTDATE
        AND 
            WA.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
    ) WZA 
    WHERE 
        NOT WZA.PIP_ORDER = 'Probe 3 Second Temp' 
    AND
        NOT WZA.PIP_ORDER = 'Probe 1 First Temp' 
) RAWA
INNER JOIN
    IDAOWNER.IDAMODULES IDAM
ON
    RAWA.SN = IDAM.MODULESN
WHERE 
    RAWA.FLAGA = 'YES' 
GROUP BY 
    RAWA.SN, 
    RAWA.WZPROBE,
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
    results <- results[results$FLAGA == "YES", ]
    results$PL <- results$PL2
    #
    return(flagged_post_processing(results, 
                                   ifelse(results$FLAGA == "YES", 
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
