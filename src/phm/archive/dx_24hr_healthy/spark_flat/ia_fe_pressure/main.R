#
# generic main routine
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
print(sprintf("INFO: Working directory: %s", work_dir))
setwd(work_dir)
#
#####################################################################
#
# required libraries
#
library(checkpoint)
#
CHECKPOINT_LOCATION <- Sys.getenv("CHECKPOINT_LOCATION")
if (nchar(CHECKPOINT_LOCATION) > 0) {
    checkpoint("2019-07-01", 
               checkpointLocation=CHECKPOINT_LOCATION)
} else {
    print("INFO: CHECKPOINT_LOCATION is not defined. Skipping.")
}
#
library(DBI)
library(RJDBC)
library(odbc)
library(dplyr)
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
#
#####################################################################
#
# source common util and algorithm libs
#
source("common_utils.R")
source("algorithm.R")
#
#####################################################################
#
# start of main try-catch error-handling block
#
errors$phm_patterns_sk("NONE")
errors$occurred(FALSE)
#
tryCatch({
    #
    # read in config and parameters file
    #
    config <- read_csv_file(filename="config.csv",
                            type_of_file="Configuration")
    rownames(config) <- config[,1]
    #
    params <- read_csv_file(filename="input.csv",
                            type_of_file="Parameters")
    params$PHM_PATTERNS_SK_DUP <- params$PHM_PATTERNS_SK
    param_sets <- split(params, list(params$PHM_PATTERNS_SK))
    #
    # get start and end dates
    #
    test_period <- get_test_period(number_of_days) 
    print(sprintf("INFO: START DATE: %s, END DATE: %s",
                  test_period["START_DATE","VALUE"],
                  test_period["END_DATE","VALUE"]))
    #
    # open database connection
    #
    db_conn <- connect_to_db(config)
    #
    # load parquet files for spark, if needed. default version
    # does nothing. is overwritten by algorithm when needed.
    #
    spark_load_data(db_conn, param_sets, test_period)
    #
    # check if we have a reliability query
    #
    rel_db_conn <- NA
    if ( ! is.na(reliability_query_template)) {
        rel_db_conn <- reliability_connect_to_db(config)
    }
    #
    # run group algorithm for all the parameters sets
    #
    # execute query on all the parameter sets
    #
    for (params in param_sets) {
        #
        # set patterns for any errors and set row names
        #
        errors$phm_patterns_sk(unique(params[ , "PHM_PATTERNS_SK_DUP"])[1])
        rownames(params) <- params[,"PARAMETER_NAME"]
        print(params["INFO: IHN_LEVEL3_DESC", "PARAMETER_VALUE"])
        #
        # execute queries
        #
        flagged_results <- exec_query(params, 
                                      db_conn, 
                                      flagged_query_template, 
                                      test_period)
        #
        if (errors$occurred()) {
            next
        }
        #
        modulesn_results <- exec_query(params, 
                                       db_conn, 
                                       modulesn_query_template,
                                       test_period)
        #
        if (errors$occurred()) {
            next
        }
        #
        if ( ! is.na(reliability_query_template)) {
            reliability_results <- exec_query(params, 
                                              rel_db_conn, 
                                              reliability_query_template,
                                              test_period)
            if (errors$occurred()) {
                next
            }
        } else {
            reliability_results <- empty_results()
        }
        #
        if ( ! is.na(chart_data_query_template)) {
            chart_data_results <- exec_query(params, 
                                             db_conn, 
                                             chart_data_query_template,
                                             test_period)
            if (errors$occurred()) {
                return(empty_results())
            }
        } else {
            chart_data_results <- empty_results()
        }
        #
        # process the query results
        #
        if ((nrow(flagged_results) == 0) && (nrow(modulesn_results) == 0)) {
            #
            # nothing found. 
            #
            next
        }
        #
        # were any instruments flagged?
        #
        if (nrow(flagged_results) == 0) {
            #
            # nothing was flagged
            #
            flagged_results <- empty_results()
        } else {
            #
            # all column names are uppercase
            #
            names(flagged_results) <- toupper(names(flagged_results))
            #
            # add extra columns required in the output file
            #
            flagged_results <- within(flagged_results,
            {
                if ( ! is.na(product_line_code)) {
                    PL <- product_line_code
                }
                PHN_PATTERNS_SK <- 
                    unique(params[ , "PHM_PATTERNS_SK_DUP"])[[1]]
                IHN_LEVEL3_DESC <- 
                    params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]
                #
                FLAG_YN <- 1
                CHART_DATA_VALUE <- 1
            })
            #
            flagged_results <- 
                post_flagged_processing(flagged_results, 
                                        db_conn, 
                                        params, 
                                        options, 
                                        test_period)
            #
            # check if we have any reliability results.
            #
            if (nrow(reliability_results) > 0) {
                #
                # remove any common modulesn
                #
                names(reliability_results) <- 
                    toupper(names(reliability_results))
                reliability_modulesn <- 
                    unique(reliability_results[, "MODULESN"])
                suppressed_modulesn <- 
                    flagged_results[(flagged_results$MODULESN %in%
                                     reliability_modulesn), "MODULESN"]
                flagged_results <- 
                    flagged_results[ ! (flagged_results$MODULESN %in%
                                        reliability_modulesn), ]
                print(paste("INFO SUPPRESSED MODULESN", 
                            suppressed_modulesn))
                #
                # standardize empty results 
                #
                if (nrow(flagged_results) == 0) {
                    flagged_results <- empty_results()
                }
            }
        }
        #
        # were any modulesn found?
        #
        if (nrow(modulesn_results) == 0) {
            #
            # nothing was found
            #
            not_flagged_results <- empty_results()
        } else {
            #
            # all column names are uppercase
            #
            names(modulesn_results) <- toupper(names(modulesn_results))
            #
            # generate the list of all modulesn with data in the last day.
            #
            modulesn_list <- unique(modulesn_results[, "MODULESN"])
            #
            not_flagged_results <- data.frame(MODULESN=modulesn_list)
            #
            # remove any flagged modulesn
            #
            if (nrow(flagged_results) > 0) {
                flagged_modulesn_list <- unique(flagged_results[, "MODULESN"])
                not_flagged_results <- 
                    data.frame(MODULESN=not_flagged_results[ ! (not_flagged_results$MODULESN %in% flagged_modulesn_list), "MODULESN" ])
            }
            #
            # add extra columns required in the output file
            #
            not_flagged_results <- within(not_flagged_results,
            {
                if ( ! is.na(product_line_code)) {
                    PL <- product_line_code
                } else {
                    PL <- "TBD"
                }
                PHN_PATTERNS_SK <- 
                    unique(params[ , "PHM_PATTERNS_SK_DUP"])[[1]]
                IHN_LEVEL3_DESC <- 
                    params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]
                FLAG_DATE <- 
                    test_period["HEALTHY_FLAG_DATE", "VALUE"]
                #
                FLAG_YN <- 0
                CHART_DATA_VALUE <- 0
            })
            #
            if (nrow(chart_data_results) > 0) {
                print("INFO: Assigning chart data for healthy instruments")
                #
                names(chart_data_results) <- toupper(names(chart_data_results))
                not_flagged_modulesn_list <- unique(not_flagged_results[, "MODULESN"])
                chart_data_modulesn_list <- unique(chart_data_results[, "MODULESN"])
                #
                intersection_modulesn_list <- intersect(not_flagged_modulesn_list, 
                                                        chart_data_modulesn_list)
                #
                not_flagged_results[not_flagged_results$MODULESN %in% 
                                    intersection_modulesn_list, "CHART_DATA_VALUE"] <-
                    chart_data_results[chart_data_results$MODULESN %in% 
                                       intersection_modulesn_list, "CHART_DATA_VALUE"]
            } else {
                print("INFO: Skip assigning chart data for healthy instruments")
            }
        }
        #
        # save results
        #
        keep <- c("MODULESN",
                  "FLAG_DATE",
                  "PL",
                  "PHN_PATTERNS_SK",
                  "IHN_LEVEL3_DESC",
                  "FLAG_YN",
                  "CHART_DATA_VALUE")
        write_results(rbind(subset(flagged_results, 
                            select=keep),
                            not_flagged_results))
    }
    #
    # reset errors in case something goes wrong
    #
    errors$phm_patterns_sk("NONE")
    errors$occurred(FALSE)
    #
    # close DB connection
    #
    disconnect_from_db(db_conn)
    #
    # end of try-catch error-handling block
    #
}, error=errors$handler)
#
if (errors$total_errors() > 0) {
    q(status=-1)
}
#
q(status=0)
