#
# generic main routine 
#
#########################################################################
# 
# required R libraries
#
library(DBI)
library(RJDBC)
library(dplyr)
library(sparklyr)
#
options(max.print=100000)
options(warning.length = 5000)
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
# source common util and algorithm files
#
source("utils.R")
source("algorithm.R")
#
# start of try-catch error-handling block
#
errors <- make_error_handler()
errors$phm_patterns_sk("NONE")
errors$occurred(FALSE)
#
tryCatch({
#
# read in config and parameters files
#
config <- read.csv("config.csv", stringsAsFactors=FALSE)
rownames(config) <- config[,1]
#
params <- read.csv("input.csv", stringsAsFactors=FALSE)
params$PHM_PATTERNS_SK_DUP <- params$PHM_PATTERNS_SK
param_sets <- split(params, list(params$PHM_PATTERNS_SK))
#
# substitute dates into query
#
sql_query <- query_subs(sql_query, 
                        get_test_period(number_of_days),
                        "VALUE")
#
# open database connection
#
db_conn <- dx_connect_to_db(config)
#
# execute query on all the parameter sets
#
append_to_file <- FALSE
print_col_names <- TRUE
columns_to_keep <- c("MODULESN",
                     "FLAG_DATE",
                     "PL",
                     "PHN_PATTERNS_SK",
                     "IHN_LEVEL3_DESC",
                     "FLAG_YN",
                     "CHART_DATA_VALUE")
#
for (params in param_sets) {
    #
    # substitute parameters into query and set algorithm 
    # name for error reporting.
    #
    rownames(params) <- params[,"PARAMETER_NAME"]
    errors$phm_patterns_sk(unique(params[ , "PHM_PATTERNS_SK_DUP"])[1])
    final_sql_query <- query_subs(sql_query, params, "PARAMETER_VALUE")
    print(params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"])
    #
    # execute query and handle any errors
    #
    tryCatch({
            errors$occurred(FALSE)
            query_time <- system.time({
            results <- dbGetQuery(db_conn, final_sql_query)
        })},
        error=errors$handler
    )
    #
    if (errors$occurred()) {
        print(sprintf("ERROR CAUGHT. NROW = %d", 0))
        next
    } else if (nrow(results) == 0) {
        print(sprintf("NROW = %d", 0))
        print(query_time)
        next
    }
    print(sprintf("NROW = %d", nrow(results)))
    print(query_time)
    #
    # all column names to uppercase
    #
    names(results) <- toupper(names(results))
    #
    # add extra columns required in the output file
    #
    results <- within(results,
    {
        PL <- product_line_code
        PHN_PATTERNS_SK <- 
            unique(params[ , "PHM_PATTERNS_SK_DUP"])[[1]]
        IHN_LEVEL3_DESC <- 
            params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"]
        THRESHOLD_DESCRIPTION <- 
            params["THRESHOLD_DESCRIPTION", "PARAMETER_VALUE"]
        #
        FLAG_YN <- 1
        CHART_DATA_VALUE <- 1
    })
    #
    # additional algorithm post-processing if needed.
    #
    results <- post_processing(results, params, db_conn, final_sql_query)
    #
    # strip out unwanted columns and rename columns
    #
    results <- subset(results, select=columns_to_keep)
    results <- distinct(results)
    #
    names(results)[names(results) == "MODULESN"] <- "SN"
    #
    # give correct order for printing data
    #
    write.table(results[,c("PHN_PATTERNS_SK",
                          "PL",
                          "SN",
                          "FLAG_DATE",
                          "CHART_DATA_VALUE",
                          "FLAG_YN",
                          "IHN_LEVEL3_DESC")],
                file="results.csv",
                append=append_to_file,
                row.names=FALSE,
                col.names=print_col_names,
                sep=",")
    #
    append_to_file <- TRUE
    print_col_names <- FALSE
}
#
# close DB connection
#
errors$phm_patterns_sk("NONE")
errors$occurred(FALSE)
#
dbDisconnect(db_conn)
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

