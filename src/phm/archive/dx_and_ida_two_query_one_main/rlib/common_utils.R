#
# common utility functions
#
#########################################################################
#
# standard usage message
#
usage <- function() {
    print("usage: script.R [-h] [-T] -w work.dir [-c config.file] [-p param.file] [-o output.file]")
}
#
# read test dates from a csv file
#
read_test_dates <- function(filename)
{
    test_dates <- read_csv_file(filename,"TEST DATES")
    rownames(test_dates) <- test_dates[,1]
    return(data.frame(NAME=c("START_DATE", 
                             "END_DATE"),
                      VALUE=c(test_dates["START_DATE","VALUE"],
                              test_dates["END_DATE","VALUE"])))
}
#
# generate a test period 
#
get_test_period <- function(options, number_of_days)
{
    test_dates <- data.frame()
    #
    if (options$test == TRUE) {
        #
        # get test dates from a file 
        #
        test_dates <- read_test_dates("test_dates.csv")
    } else {
        #
        # sanity check
        #
        if (number_of_days <= 0) {
            stop(sprintf("Invalid number of days given: %s", 
                         number_of_days))
        }
        #
        # calculate date range
        #
        now = Sys.Date()
        test_date_format <- ""
        if (options$config_type == "dx") {
            # return a date comparable to character transaction date field
            test_date_format <- "%Y-%m-%d"
        } else if (options$config_type == "ida") {
            # return a date comparable to Date datetimestamplocal
            test_date_format <- "%m/%d/%Y %H:%M:%S"
        } else {
            stop(sprintf("Unknown configuration type: %s", 
                         options$config_type))
        }
        #
        test_dates <- 
            data.frame(NAME=c("START_DATE", "END_DATE"),
                       VALUE=c(format(now-number_of_days, test_date_format),
                               format(now, test_date_format)))
    }
    #
    rownames(test_dates) <- test_dates[,1]
    #
    return(test_dates)
}
#
# read a csv file with sanity checks
#
read_csv_file <- function(filename, type_of_file)
{
    #
    # sanity checks on file
    #
    if (is.null(filename)) {
        stop(sprintf("%s file was not given.", type_of_file))
    } else if ( ! file.exists(filename)) {
        stop(sprintf("%s file %s does not exist.", type_of_file, filename))
    }
    #
    return(read.csv(filename, stringsAsFactors=FALSE))
}
#
# substitute parameters for variables in query
#
query_subs <- function(query_template, substitutions, value_column_name)
{
    query <- query_template
    #
    if (nrow(substitutions) > 0) {
        for (rownm in rownames(substitutions)) {
            query <- gsub(sprintf("<%s>", rownm),
                          substitutions[rownm, value_column_name],
                          query,
                          fixed = TRUE)
        }
    }
    #
    return(query)
}
#
# save query to a flagged or not flagged files.
#
make_save_query <- function()
{
    # use a closure to save values between calls
    flagged_append <- FALSE
    not_flagged_append <- FALSE
    #
    save_query <- function(query, flagged)
    {
        if (flagged) {
            cat(query,file="flagged.sql",sep="\n",append=flagged_append)
            flagged_append <<- TRUE
        } else {
            cat(query,file="not_flagged.sql",sep="\n",append=not_flagged_append)
            not_flagged_append <<- TRUE
        }
    }
    return(save_query)
}
#
save_query <- make_save_query()
#
# write error messages to errors.csv
#
make_error_handler <- function()
{
    #
    # use a closure to save values between calls
    #
    phm_patterns_sk <- "NONE"
    occurred <- FALSE
    file_name <- "errors.csv"
    #
    error.append <- FALSE
    col.names <- TRUE
    #
    # create error handler and assign functions
    #
    eh <- list()
    #
    eh$handler <- function(emsg)
    {
        occurred <<- TRUE
        print(emsg)
        write.table(list(PHM_PATTERNS_SK=c(phm_patterns_sk),
                         ERROR_MESSAGE=c(paste(unlist(emsg),
                                               collapse=" "))),
                    file=file_name,
                    append=error.append,
                    row.names=FALSE,
                    col.names=col.names,
                    sep=",")
        error.append <<- TRUE
        col.names <<- FALSE
    }
    #
    eh$phm_patterns_sk <- function(x=NA) {
        if ( ! is.na(x)) {
            phm_patterns_sk <<- x
        }
        return(phm_patterns_sk)
    }
    #
    eh$occurred <- function(x=NA) {
        if ( ! is.na(x)) {
            occurred <<- x
        }
        return(occurred)
    }
    #
    eh$file_name <- function(x=NA) {
        if ( ! is.na(x)) {
            file_name <<- x
        }
        return(file_name)
    }
    #
    return(eh)
}
#
errors <- make_error_handler()
#
#
# write all the records to the results file
#
write_results <- function(options,
                          flagged_records,
                          not_flagged_records,
                          keep=c("MODULESN",
                                 "FLAG_DATE",
                                 "PL",
                                 "PHN_PATTERNS_SK",
                                 "IHN_LEVEL3_DESC",
                                 "THRESHOLD_DESCRIPTION",
                                 "FLAG_YN",
                                 "CHART_DATA_VALUE"))
{
    append <- FALSE
    col.names <- TRUE
    #
    for (record in rbind(flagged_records, not_flagged_records)) {
        if (nrow(record) > 0) {
            #
            # upper case columns names for filtering and printing
            #
            names(record) <- toupper(names(record))
            #
            # strip out unwanted columns
            #
            record <- subset(record, select=keep)
            #
            write.table(record, 
                        file=options$output, 
                        append=append,
                        row.names=FALSE,
                        col.names=col.names,
                        sep=",")
            append <- TRUE
            col.names <- FALSE
        }
    }
}
#
# generate an empty list
#
empty_results <- function()
{
    new_nc <- 4
    new_cnms <- c("PHN_PATTERNS_SK",
                  "PL",
                  "IHN_LEVEL3_DESC",
                  "THRESHOLD_DESCRIPTION")
    empty_results <- data.frame(matrix(ncol=new_nc, nrow=0))
    colnames(empty_results) <- new_cnms
    #
    return(empty_results)
}
#
# generate a query and execute it. process results afterwards.
#
exec_query <- function(params, 
                       db_conn, 
                       query_template, 
                       options, 
                       test_period, 
                       flagged)
{
    #
    # check if we skip the query (say not-flagged).
    #
    if (is.na(query_template)) {
        #
        # no query. no results.
        #
        return(empty_results())
    }
    #
    # set patterns for any errors
    #
    errors$phm_patterns_sk(unique(params[ , "PHM_PATTERNS_SK_DUP"])[1])
    #
    # easy to access parameters if we assign row names
    #
    rownames(params) <- params[,"PARAMETER_NAME"]
    #
    # substitute values into query
    #
    query <- query_subs(query_template, test_period, "VALUE")
    query <- query_subs(query, params, "PARAMETER_VALUE")
    save_query(query, flagged)
    query <- gsub("[\n\r]"," ", query)
    #
    if (options$query_only) {
        #
        # only the query is needed
        #
        return(empty_results())
    }
    #
    print(params["IHN_LEVEL3_DESC", "PARAMETER_VALUE"])
    #
    tryCatch({
        errors$occurred(FALSE)
        query_time <- system.time({
            results <- dbGetQuery(db_conn, query)
        })},
        error=errors$handler
    )
    #
    if (errors$occurred()) {
        #
        # error caught. return an empty data frame
        #
        print(sprintf("ERROR CAUGHT. NROW = %d", 0))
        return(empty_results())
    } else if (nrow(results) == 0) {
        #
        # nothing found. return an empty data frame
        #
        print(sprintf("NROW = %d", 0))
        print(query_time)
        return(empty_results())
    }
    #
    print(sprintf("NROW = %d", nrow(results)))
    print(query_time)
    #
    # add extra columns required in the output file
    #
    results$PL <- options$product_line_code
    results$PHN_PATTERNS_SK <- unique(params[ , "PHM_PATTERNS_SK_DUP"])[[1]]
    results$IHN_LEVEL3_DESC <- params["IHN_LEVEL3_DESC",
                                      "PARAMETER_VALUE"]
    results$THRESHOLD_DESCRIPTION <- params["THRESHOLD_DESCRIPTION",
                                            "PARAMETER_VALUE"]
    #
    results$FLAG_YN <- ifelse((flagged), 1, 0)
    results$CHART_DATA_VALUE <- ifelse((flagged), 1, 0)
    #
    return(results)
}
#
# connect to database
#
dx_connect_to_db <- function(config)
{
    db_driver <- RJDBC::JDBC(driverClass=config["JDBC_DRIVER_CLASS","VALUE"],
                             classPath = config["JDBC_CLASSPATH","VALUE"],
                             identifier.quote="'")
#
    db_conn <- dbConnect(db_driver, 
                         config["DB_CONN_STRING","VALUE"],
                         LogLevel = config["LOGLEVEL","VALUE"],
                         LogPath = config["LOGPATH","VALUE"],
                         workgroup = config["WORKGROUP","VALUE"],
                         UseResultSetStreaming = config["USERESULTSETSTREAMING","VALUE"],
                         S3OutputLocation = config["S3OUTPUTLOCATION","VALUE"],
                         AwsCredentialsProviderClass = config["AWSCREDENTIALSPROVIDERCLASS","VALUE"],
                         AwsCredentialsProviderArguments = config["AWSCREDENTIALSPROVIDERARGUMENTS","VALUE"],
                         s3_staging_dir = config["S3_STAGING_DIR","VALUE"],
                         user = config["DB_USER","VALUE"],
                         password = config["DB_PASSWORD","VALUE"])
    return(db_conn)
}
#
ida_connect_to_db <- function(config)
{
    db_driver <- RJDBC::JDBC(driverClass=config["JDBC_DRIVER_CLASS","VALUE"],
                             classPath=config["JDBC_CLASSPATH","VALUE"])
    db_dsn <- sprintf(config["JDBC_DSN_FORMAT","VALUE"],
                      config["DB_HOST","VALUE"],
                      config["DB_PORT","VALUE"],
                      config["DB_NAME","VALUE"])
    db_conn <- dbConnect(db_driver,
                         db_dsn,
                         config["DB_USER","VALUE"],
                         config["DB_PASSWORD","VALUE"])
    return(db_conn)
}
#
connect_to_db <- function(options)
{
    if (options$use_config) {
        if (options$config_type == "dx") {
            return(dx_connect_to_db(options$config))
        } else if (options$config_type == "ida") {
            return(ida_connect_to_db(options$config))
        } else {
            stop(sprintf("Unknown config type: %s", options$config_type))
        }
    } else if (file.exists("input.csv")) {
        #
        # connection file exists. use it. the db connection
        # is assumed to be named "db_conn". return it.
        #
        source("input.csv")
        return(db_conn)
    } else {
        stop("input.csv file is missing")
    }
}
#
# connect to db and generate and save flagged and not flagged data
#
generate_data <- function(param_sets, 
                          options,
                          test_period,
                          flagged_query_template,
                          not_flagged_query_template)
{
    #
    # do we only generate the queries?
    #
    if (options$query_only) {
        #
        # only queries
        #
        dummy <- lapply(param_sets, 
                        exec_query, 
                        NA, 
                        flagged_query_template,
                        options,
                        test_period,
                        TRUE)
        dummy <- lapply(param_sets, 
                        exec_query, 
                        NA, 
                        not_flagged_query_template,
                        options,
                        test_period,
                        FALSE)
        return(NA)
    }
    #
    # open database connection
    #
    db_conn <- connect_to_db(options)
    #
    # execute flagged data query on all the parameter sets
    #
    flagged_records <- lapply(param_sets, 
                              exec_query, 
                              db_conn, 
                              flagged_query_template,
                              options,
                              test_period,
                              TRUE)
    #
    # execute not-flagged data query on all the parameter sets
    #
    not_flagged_records <- lapply(param_sets, 
                                  exec_query, 
                                  db_conn, 
                                  not_flagged_query_template,
                                  options,
                                  test_period,
                                  FALSE)
    #
    # close DB connection
    #
    dbDisconnect(db_conn)
    #
    # save results
    #
    write_results(options,
                  flagged_records,
                  not_flagged_records)
}
#
# main entry point to algorithms
#
real_main <- function(config_type, 
                      number_of_days, 
                      flagged_query, 
                      not_flagged_query,
                      product_line_code)
{
    specs <- matrix(c('help', 'h', 0, 'logical',
                      'query_only', 'Q', 0, 'logical',
                      'test', 'T', 0, 'logical',
                      'work', 'w', 1, 'character',
                      'config', 'c', 0, 'logical',
                      'params', 'p', 1, 'character',
                      'output', 'o', 1, 'character'),
                    byrow=TRUE, 
                    ncol=4)
    #
    options <- getopt(specs)
    #
    # check if usage message was requested
    #
    if ( ! is.null(options$help)) {
        usage()
        q(status=2)
    }
    #
    # change to working directory
    #
    if (is.null(options$work)) {
        message(sprintf("\nWorking directory was not given.\nDefaulting to current directory: %s",getwd()))
    } else {
        setwd(options$work)
        message(sprintf("\nSet working directory: %s", getwd()))
    }
    #
    # set default values
    #
    options$config_type <- config_type
    #
    if (nchar(Sys.getenv("DEV_ROOT")) == 0) {
        stop("Enviroment variable DEV_ROOT is NOT set.")
    }
    if (is.null(options$output)) {
        options$output <- "results.csv"
    }
    if (is.null(options$query_only)) {
        options$query_only <- FALSE
    }
    if (is.null(options$test)) {
        options$test <- FALSE
    }
    if (is.null(options$params)) {
        options$params <- "parameters.csv"
    }
    if (is.null(options[["config"]])) {
        options$use_config <- FALSE
    } else {
        options$use_config <- TRUE
        #
        # read in config file
        #
        config <- read_csv_file(filename=file.path(Sys.getenv("DEV_ROOT"),
                                                   "config",
                                                   options$config_type,
                                                   "config.csv"),
                                type_of_file="Configuration")
        #
        # access name-value records by assigning the name 
        # as the row name.
        #
        rownames(config) <- config[,1]
        #
        options$config <- config
    }
    #
    # save product line code for results
    #
    options$product_line_code <- product_line_code
    #
    # remove any old error file.
    #
    if (file.exists(errors$file_name())) {
        file.remove(errors$file_name())
    }
    #
    # get start and end dates
    #
    test_period <- get_test_period(options, number_of_days) 
    print(sprintf("START DATE: %s, END DATE: %s",
                  test_period["START_DATE","VALUE"],
                  test_period["END_DATE","VALUE"]))
    #
    # read in parameters file 
    #
    params <- read_csv_file(filename=options$params, 
                            type_of_file="Parameters")
    #
    # duplicate the patterns column so we have access to the value
    # in the lapply
    #
    params$PHM_PATTERNS_SK_DUP <- params$PHM_PATTERNS_SK
    #
    # split up the parameter sets by pattern IDs. the list will
    # be passed to lapply to generate the data sets.
    # 
    param_sets <- split(params, list(params$PHM_PATTERNS_SK))
    #
    # generate flagged and not flagged data
    #
    process_time <- system.time({
        generate_data(param_sets, 
                      options, 
                      test_period,
                      flagged_query, 
                      not_flagged_query)
    })
    #
    print(process_time)
}
#
main <- function(config_type, 
                 number_of_days, 
                 flagged_query, 
                 not_flagged_query,
                 product_line_code="205")
{
    tryCatch(real_main(config_type, 
                       number_of_days, 
                       flagged_query, 
                       not_flagged_query,
                       product_line_code),
             error=errors$handler)
}

