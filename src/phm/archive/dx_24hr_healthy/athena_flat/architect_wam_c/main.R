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
    #
    # use later version of dplyr since it is much faster
    #
    checkpoint("2019-10-01", checkpointLocation=CHECKPOINT_LOCATION)
    library(dplyr)
    #
    checkpoint("2019-07-01", checkpointLocation=CHECKPOINT_LOCATION)
    library(DBI)
    library(RJDBC)
    library(odbc)
    library(sparklyr)
} else {
    print("INFO: CHECKPOINT_LOCATION is not defined. Skipping.")
    #
    library(DBI)
    library(RJDBC)
    library(odbc)
    library(dplyr)
    library(sparklyr)
}
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
    # check if we have to use suppression
    #
    rel_db_conn <- NA
    if (use_suppression) {
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
        #
        if ( ! ("IHN_LEVEL3_DESC" %in% params[ ,"PARAMETER_NAME"])) {
            params <- rbind(params,
                            list(ALGORITHM_NAME=c(params[1,"ALGORITHM_NAME"]),
                                 PHM_PATTERNS_SK=c(params[1,"PHM_PATTERNS_SK"]),
                                 PHM_PATTERNS_SK_DUP=c(params[1,"PHM_PATTERNS_SK_DUP"]),
                                 PARAMETER_NAME=c("IHN_LEVEL3_DESC"),
                                 PARAMETER_VALUE=c("")),
                            stringsAsFactors=FALSE)
        }
        rownames(params) <- params[,"PARAMETER_NAME"]
        print(sprintf("INFO: >>>> %s <<<<", params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]))
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
        errors$occurred(FALSE)
        #
        flagged_results <- 
            pre_flagged_processing(flagged_results, 
                                   db_conn, 
                                   params, 
                                   test_period)
        if (errors$occurred()) {
            next
        }
        #
        if (file.exists("flag_only")) {
            print("INFO: 'flag_only file exists. Skipping healthy, suppression and charting.")
            modulesn_results <- empty_results()
            suppression_results <- empty_results()
            chart_data_results <- empty_results()
        } else {
            modulesn_results <- exec_query(params, 
                                           db_conn, 
                                           modulesn_query_template,
                                           test_period)
            #
            if (errors$occurred()) {
                next
            }
            #
            if (use_suppression) {
                suppression_results <- generate_suppression(params, 
                                                            rel_db_conn, 
                                                            test_period)
                if (errors$occurred()) {
                    next
                }
            } else {
                suppression_results <- empty_results()
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
                CHART_FLAG_DATE <- FLAG_DATE
                #
                FLAG_YN <- 1
                CHART_DATA_VALUE <- 1
            })
            #
            write_data(flagged_results, "debug_flagged_results.csv")
            flagged_results <- 
                post_flagged_processing(flagged_results, 
                                        db_conn, 
                                        params, 
                                        test_period)
            write_data(flagged_results, "debug_postprocessing_flagged_results.csv")
            #
            # check if we have any suppression results.
            #
            if (nrow(suppression_results) > 0) {
                #
                # remove any common modulesn
                #
                names(suppression_results) <- 
                    toupper(names(suppression_results))
                write_data(suppression_results, "debug_suppression_results.csv")
                suppression_modulesn <- 
                    unique(suppression_results[, "MODULESN"])
                suppressed_modulesn <- 
                    flagged_results[(flagged_results$MODULESN %in%
                                     suppression_modulesn), "MODULESN"]
                flagged_results <- 
                    flagged_results[ ! (flagged_results$MODULESN %in%
                                        suppression_modulesn), ]
                sn_list <- paste(suppressed_modulesn, collapse=',')
                print(paste("INFO: SUPPRESSED MODULESN:", sn_list, collapse=' '))
                save_to_file(sn_list, "suppression.csv")
                #
                # standardize empty results 
                #
                if (nrow(flagged_results) == 0) {
                    flagged_results <- empty_results()
                }
            }
            write_data(flagged_results, "debug_suppression_flagged_results.csv")
            #
            if (nrow(flagged_results) > 0) {
                #
                # product lines codes are already assigned at this point.
                # here we assign chart data values, if we have any to assign.
                # there may be more than one set of values to assign per SN,
                # so assign by groups. 
                #
                if (nrow(chart_data_results) > 0) {
                    print("INFO: Assigning chart data for flagged instruments")
                    tmp_flagged_results <- flagged_results %>%
                                           rename_all(toupper) %>%
                                           select(c(MODULESN,PL,PHN_PATTERNS_SK,IHN_LEVEL3_DESC,FLAG_DATE,FLAG_YN,CHART_DATA_VALUE))
                    #
                    # get list of SNs which have chart data and are both in the 
                    # healthy list and the chart data list.
                    #
                    names(chart_data_results) <- toupper(names(chart_data_results))
                    write_data(chart_data_results, "debug_chart_data_results.csv")
                    flagged_modulesn_list <- unique(tmp_flagged_results[, "MODULESN"])
                    chart_data_modulesn_list <- unique(chart_data_results[, "MODULESN"])
                    #
                    intersection_modulesn_list <- intersect(flagged_modulesn_list, 
                                                            chart_data_modulesn_list)
                    #
                    # create list of SNs which will not have chart data
                    #
                    no_chart_data_flagged_results <- 
                        tmp_flagged_results[ ! (tmp_flagged_results$MODULESN %in% intersection_modulesn_list), ]
                    #
                    # now create list of SN which have chart data
                    #
                    chart_data_flagged_results <- empty_results()
                    all_cols_chart_data_flagged_results <- empty_results()
                    #
                    flagged_results_wo_chart_data <- subset(tmp_flagged_results, select=-c(CHART_DATA_VALUE))
                    #
                    for (irec in 1:length(intersection_modulesn_list)) {
                        modulesn <- flagged_results_wo_chart_data[irec, "MODULESN"]
                        #
                        old_flagged_rec <- flagged_results_wo_chart_data[irec, ]
                        #
                        chart_data_rec <- chart_data_results[chart_data_results$MODULESN == modulesn, 
                                                             c("MODULESN", "CHART_DATA_VALUE")]
                        new_flagged_rec <- merge(old_flagged_rec, chart_data_rec, by="MODULESN")
                        #
                        chart_data_flagged_results <- rbind(chart_data_flagged_results,
                                                            new_flagged_rec)
                        #
                        all_cols_chart_data_rec <- chart_data_results[chart_data_results$MODULESN == modulesn, ]
                        new_all_cols_flagged_rec <- merge(old_flagged_rec, all_cols_chart_data_rec, by="MODULESN")
                        #
                        all_cols_chart_data_flagged_results <- rbind(all_cols_chart_data_flagged_results,
                                                                     new_all_cols_flagged_rec)
                    }
                    #
                    if (nrow(all_cols_chart_data_flagged_results) > 0) {
                        print(sprintf("INFO: Starting chart data processing for flagged data ..."))
                        write_data(all_cols_chart_data_flagged_results, "debug_flagged_chart_data.csv")
                        chart_time <- system.time({
                            write_data(all_cols_chart_data_flagged_results %>% 
                                       rename_all(toupper) %>%
                                       select(-c(PL.X,FLAG_DATE.X,IHN_LEVEL3_DESC)) %>%
                                       rename(PL=PL.Y,FLAG_DATE=FLAG_DATE.Y,SN=MODULESN,DATA_SERIES=WZPROBEC) %>% 
                                       select(c(PHN_PATTERNS_SK,PL,SN,FLAG_DATE,CHART_DATA_VALUE,DATA_SERIES)) %>%
                                       distinct(PHN_PATTERNS_SK,PL,SN,FLAG_DATE,CHART_DATA_VALUE,DATA_SERIES),
                                       'chart_data.csv')
                        })
                        print(chart_time)
                    } else {
                        print("INFO: No chart data found for flagged instruments")
                    }
                } else {
                    print("INFO: Skip assigning chart data for flagged instruments")
                }
            }
        }
        #
        # were any modulesn found?
        #
        not_flagged_results <- data.frame()
        if (nrow(modulesn_results) == 0) {
            #
            # nothing was found
            #
            not_flagged_results <- empty_results()
        } else {
            #
            # create not-flagged/healthy modulesn list
            #
            names(modulesn_results) <- toupper(names(modulesn_results))
            #
            not_flagged_results <- modulesn_results
            #
            if (nrow(flagged_results) > 0) {
                #
                # remove flagged SNs from the healthy/wealthy/wise list
                #
                flagged_modulesn_list <- unique(flagged_results[, "MODULESN"])
                not_flagged_results <- subset(not_flagged_results, ! (MODULESN %in% flagged_modulesn_list))
            }
            #
            if (nrow(not_flagged_results) > 0) {
                if ("PL" %in% names(not_flagged_results)) {
                    not_flagged_results <- within(not_flagged_results,
                    {
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
                } else {
                    not_flagged_results <- within(not_flagged_results,
                    {
                        if ( ! is.na(product_line_code)) {
                            PL <- product_line_code
                        } else {
                            PL <- ""
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
                }
                write_data(not_flagged_results, "debug_not_flagged_results.csv")
                #
                # product lines codes are already assigned at this point.
                # here we assign chart data values, if we have any to assign.
                # there may be more than one set of values to assign per SN,
                # so assign by groups. 
                #
                if (nrow(chart_data_results) > 0) {
                    print("INFO: Assigning chart data for healthy instruments")
                    #
                    # get list of SNs which have chart data and are both in the 
                    # healthy list and the chart data list.
                    #
                    names(chart_data_results) <- toupper(names(chart_data_results))
                    not_flagged_modulesn_list <- unique(not_flagged_results[, "MODULESN"])
                    chart_data_modulesn_list <- unique(chart_data_results[, "MODULESN"])
                    #
                    intersection_modulesn_list <- intersect(not_flagged_modulesn_list, 
                                                            chart_data_modulesn_list)
                    #
                    # create list of SNs which will not have chart data
                    #
                    no_chart_data_not_flagged_results <- 
                        not_flagged_results[ ! (not_flagged_results$MODULESN %in% intersection_modulesn_list), ]
                    #
                    # now create list of SN which have chart data
                    #
                    chart_data_not_flagged_results <- empty_results()
                    all_cols_chart_data_not_flagged_results <- empty_results()
                    #
                    not_flagged_results_wo_chart_data <- subset(not_flagged_results, select=-c(CHART_DATA_VALUE))
                    #
                    for (irec in 1:length(intersection_modulesn_list)) {
                        modulesn <- not_flagged_results_wo_chart_data[irec, "MODULESN"]
                        #
                        old_not_flagged_rec <- not_flagged_results_wo_chart_data[irec, ]
                        #
                        chart_data_rec <- chart_data_results[chart_data_results$MODULESN == modulesn, 
                                                             c("MODULESN", "CHART_DATA_VALUE")]
                        new_not_flagged_rec <- merge(old_not_flagged_rec, chart_data_rec, by="MODULESN")
                        #
                        chart_data_not_flagged_results <- rbind(chart_data_not_flagged_results,
                                                                new_not_flagged_rec)
                        #
                        all_cols_chart_data_rec <- chart_data_results[chart_data_results$MODULESN == modulesn, ]
                        new_all_cols_not_flagged_rec <- merge(old_not_flagged_rec, all_cols_chart_data_rec, by="MODULESN")
                        #
                        all_cols_chart_data_not_flagged_results <- rbind(all_cols_chart_data_not_flagged_results,
                                                                         new_all_cols_not_flagged_rec)
                    }
                    #
                    if (nrow(all_cols_chart_data_not_flagged_results) > 0) {
                        print(sprintf("INFO: Starting chart data processing for not-flagged data ..."))
                        write_data(all_cols_chart_data_not_flagged_results, "debug_not_flagged_chart_data.csv")
                        chart_time <- system.time({
                            write_data(all_cols_chart_data_not_flagged_results %>% 
                                       rename_all(toupper) %>%
                                       select(-c(PL.X,FLAG_YN,FLAG_DATE.X,IHN_LEVEL3_DESC)) %>%
                                       rename(PL=PL.Y,FLAG_DATE=FLAG_DATE.Y,SN=MODULESN,DATA_SERIES=WZPROBEC) %>% 
                                       select(c(PHN_PATTERNS_SK,PL,SN,FLAG_DATE,CHART_DATA_VALUE,DATA_SERIES)) %>%
                                       distinct(PHN_PATTERNS_SK,PL,SN,FLAG_DATE,CHART_DATA_VALUE,DATA_SERIES),
                                       'chart_data.csv')
                        })
                        print(chart_time)
                    } else {
                        print("INFO: No chart data found for healthy instruments")
                    }
                } else {
                    print("INFO: Skip assigning chart data for healthy instruments")
                }
            } else {
                not_flagged_results <- empty_results()
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
    if (use_suppression) {
        dbDisconnect(rel_db_conn)
    }
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
