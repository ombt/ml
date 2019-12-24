#
# Architect WAM Pattern c
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
    RAWC.SN AS MODULESN,
    RAWC.WZPROBE AS WZPROBEC,
    IDAM.PRODUCTLINE AS PL2,
    MAX(RAWC.FLAGC) AS FLAGC,
    TO_CHAR(MIN(RAWC.FLAGDATEC), 'YYYYMMDDHH24MISS') AS FLAG_DATE
FROM (
    SELECT 
        WZC.MODULESNDRM AS SN,
        WZC.WASHZONEID || '.' || WZC.POSITION AS WZPROBE,
        WZC.EVENTDATE AS FLAGDATEC,
        WZC.TEMPDELTA, 
        CASE WHEN 
                 WZC.EVENTDATE -1/24 < LAG (WZC.EVENTDATE,19) OVER 
                 (
                     PARTITION BY 
                         WZC.MODULESNDRM, 
                         WZC.WASHZONEID, 
                         WZC.POSITION
                     ORDER BY 
                         WZC.EVENTDATE
                 ) 
             THEN 
                 'YES' 
             ELSE 
                 'NO' 
             END AS FLAGC 
    FROM ( 
        SELECT 
            WC.MODULESNDRM, 
            WC.EVENTDATE,
            WC.WASHZONEID -1 AS WASHZONEID,
            '1' AS POSITION,
            WC.POSITION1 AS REPLICATEID,
            CASE WHEN 
                     WC.POSITION1 = LAG (WC.POSITION1) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION1, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     ) 
                 AND 
                     WC.WASHZONEID = LAG (WC.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION1, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     )
                 AND 
                     WC.EVENTDATE -10 /(24*60*60) < LAG (WC.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION1, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     )
                THEN 
                    'Probe 1 Second Temp'
                ELSE 
                    'Probe 1 First Temp' 
                END AS PIP_ORDER,
            WC.TEMPDELTAPOSITION1/1000 TEMPDELTA
        FROM 
            IDAOWNER.WASHASPIRATIONS WC 
        WHERE 
            WC.POSITION1 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WC.EVENTDATE
        AND 
            WC.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
        UNION ALL 
        SELECT 
            WC.MODULESNDRM,
            WC.EVENTDATE, 
            WC.WASHZONEID -1 AS WASHZONEID,
            '2' AS POSITION,
            WC.POSITION2 AS REPLICATEID,
            'Probe 2' AS PIP_ORDER, 
            WC.TEMPDELTAPOSITION2/1000 TEMPDELTA
        FROM 
            IDAOWNER.WASHASPIRATIONS WC 
        WHERE 
            WC.POSITION2 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WC.EVENTDATE
        AND 
            WC.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
        UNION ALL 
        SELECT 
            WC.MODULESNDRM,
            WC.EVENTDATE,
            WC.WASHZONEID -1 AS WASHZONEID,
            '3' AS POSITION, 
            WC.POSITION3 AS REPLICATEID,
            CASE WHEN 
                     WC.POSITION3 = LAG (WC.POSITION3) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION3, 
                             WC.WASHZONEID, 
                             WC.EVENTDATE
                     ) 
                 AND 
                     WC.WASHZONEID = LAG (WC.WASHZONEID) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION3, 
                             WC.WASHZONEID,
                             WC.EVENTDATE
                     ) 
                 AND 
                     WC.EVENTDATE -10 /(24*60*60) < LAG (WC.EVENTDATE) OVER 
                     (
                         ORDER BY 
                             WC.MODULESNDRM, 
                             WC.POSITION3, 
                             WC.WASHZONEID, 
                             WC.EVENTDATE
                     )
                THEN 
                    'Probe 3 Second Temp'
                ELSE 
                    'Probe 3 First Temp' 
                END AS PIP_ORDER,
                WC.TEMPDELTAPOSITION3/1000 TEMPDELTA 
        FROM 
            IDAOWNER.WASHASPIRATIONS WC 
        WHERE 
            WC.POSITION3 > 0 
        AND 
            TO_TIMESTAMP('<START_DATE>', 
                         'MM/DD/YYYY HH24:MI:SS') <= WC.EVENTDATE
        AND 
            WC.EVENTDATE < TO_TIMESTAMP('<END_DATE>', 
                                        'MM/DD/YYYY HH24:MI:SS')
    ) WZC 
    WHERE 
        NOT WZC.PIP_ORDER = 'Probe 3 Second Temp' 
    AND 
        NOT WZC.PIP_ORDER = 'Probe 1 First Temp' 
    AND 
        WZC.TEMPDELTA < <MIN_VALUE> 
) RAWC
INNER JOIN
    IDAOWNER.IDAMODULES IDAM
ON
    RAWC.SN = IDAM.MODULESN
WHERE 
    RAWC.FLAGC = 'YES' 
GROUP BY 
    RAWC.SN, 
    RAWC.WZPROBE,
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
    results <- results[results$FLAGC == "YES", ]
    results$PL <- results$PL2
    #
    return(flagged_post_processing(results, 
                                   ifelse(results$FLAGC == "YES", 
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
