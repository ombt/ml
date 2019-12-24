#
# common utility functions
#
#########################################################################
#
# connect to database
#
reliability_dsn_connect_to_db <- function(config)
{
    db_conn <- dbConnect(odbc::odbc(), config["RELIABILITY_DB_NAME","VALUE"])
    #
    return(db_conn)
}
#
reliability_non_dsn_connect_to_db <- function(config)
{
    db_conn <- dbConnect(sprintf("driver={SQL Server};server=%s;database=%s;uid=%s;pwd=%s!",
                                 config["RELIABILITY_DB_SERVER","VALUE"],
                                 config["RELIABILITY_DB_NAME","VALUE"],
                                 config["RELIABILITY_DB_USER","VALUE"],
                                 config["RELIABILITY_DB_PASSWORD","VALUE"]))
    #
    return(db_conn)
}
#
reliability_connect_to_db <- function(config)
{
    if (toupper(config["RELIABILITY_DB_CONN_TYPE","VALUE"]) == "DSN") {
        return(reliability_dsn_connect_to_db(config))
    } else {
        return(reliability_non_dsn_connect_to_db(config))
    }
    #
    return(db_conn)
}
#
dx_custom_connect_to_db <- function(config)
{
    db_driver <- RJDBC::JDBC(driverClass=config["ATHENA_JDBC_DRIVER_CLASS","VALUE"],
                             classPath = Sys.glob(config["ATHENA_JDBC_CLASSPATH","VALUE"]),
                             identifier.quote="'")
    #
    db_conn <- dbConnect(db_driver, 
                         config["ATHENA_DB_CONN_STRING","VALUE"],
                         LogLevel = config["ATHENA_LOGLEVEL","VALUE"],
                         LogPath = config["ATHENA_LOGPATH","VALUE"],
                         workgroup = config["ATHENA_WORKGROUP","VALUE"],
                         UseResultSetStreaming = config["ATHENA_USERESULTSETSTREAMING","VALUE"],
                         S3OutputLocation = config["ATHENA_S3OUTPUTLOCATION","VALUE"],
                         AwsCredentialsProviderClass = config["ATHENA_AWSCREDENTIALSPROVIDERCLASS","VALUE"],
                         AwsCredentialsProviderArguments = config["ATHENA_AWSCREDENTIALSPROVIDERARGUMENTS","VALUE"],
                         s3_staging_dir = config["ATHENA_S3_STAGING_DIR","VALUE"])
    return(db_conn)
}
#
dx_saml_connect_to_db <- function(config)
{
    db_driver <- RJDBC::JDBC(driverClass=config["ATHENA_JDBC_DRIVER_CLASS","VALUE"],
                             classPath = config["ATHENA_JDBC_CLASSPATH","VALUE"],
                             identifier.quote="'")
    #
    db_conn <- dbConnect(db_driver, 
                         config["ATHENA_DB_CONN_STRING","VALUE"],
                         LogLevel = config["ATHENA_LOGLEVEL","VALUE"],
                         LogPath = config["ATHENA_LOGPATH","VALUE"],
                         workgroup = config["ATHENA_WORKGROUP","VALUE"],
                         UseResultSetStreaming = config["ATHENA_USERESULTSETSTREAMING","VALUE"],
                         S3OutputLocation = config["ATHENA_S3OUTPUTLOCATION","VALUE"],
                         AwsCredentialsProviderClass = config["ATHENA_AWSCREDENTIALSPROVIDERCLASS","VALUE"],
                         AwsCredentialsProviderArguments = config["ATHENA_AWSCREDENTIALSPROVIDERARGUMENTS","VALUE"],
                         s3_staging_dir = config["ATHENA_S3_STAGING_DIR","VALUE"],
                         user = config["ATHENA_DB_USER","VALUE"],
                         password = config["ATHENA_DB_PASSWORD","VALUE"])
    return(db_conn)
}
#
dx_connect_to_db <- function(config)
{
    if (toupper(config["ATHENA_CREDENTIAL_PROVIDER","VALUE"]) == "CUSTOM") {
        return(dx_custom_connect_to_db(config))
    } else {
        return(dx_saml_connect_to_db(config))
    }
}
#
spark_connect_to_db <- function(config)
{
    #
    # place values in environment for driver to access.
    #
    Sys.setenv(SPARK_HOME      = config["SPARK_HOME","VALUE"])
    Sys.setenv(HADOOP_CONF_DIR = config["HADOOP_CONF_DIR","VALUE"])
    Sys.setenv(YARN_CONF_DIR   = config["YARN_CONF_DIR","VALUE"])
    #
    # create connection configuration object
    #
    spark_cfg <- spark_config()
    #
    spark_cfg$spark.executor.instances <-
        config["SPARK_EXECUTOR_INSTANCES","VALUE"]
    spark_cfg$spark.executor.cores <-
        config["SPARK_EXECUTOR_CORES","VALUE"]
    spark_cfg$spark.executor.memory <- 
        config["SPARK_EXECUTOR_MEMORY","VALUE"]
    spark_cfg$spark.dynamicAllocation.enabled <-
        config["SPARK_DYANMICALLOCATION_ENABLED","VALUE"]
    #
    # connect to Spark.
    #
    # Commenting the way we connect to the cluster in "yarn-client" mode
    # use this if you are trying to use "yarn-client"
    # spark_conn <- spark_connect(master=config["SPARK_MASTER","VALUE"],
    #                             config=spark_cfg, 
    #                             version=config["SPARK_VERSION","VALUE"])
    #
    # Connect to spark using apache livy
    #
    spark_conn <- spark_connect(master=config["LIVY_MASTER","VALUE"],
                                method="livy") 
 	
    #
    return(spark_conn)
}
#
connect_to_db <- function(config)
{
    if (config_type == "dx") {
        return(dx_connect_to_db(config))
    } else if (config_type == "spark") {
        return(spark_connect_to_db(config))
    } else {
        stop(sprintf("INFO: Unknown config type: %s", config_type))
    }
}
#
disconnect_from_db <- function(db_conn)
{
    if (config_type == "spark") {
        spark_disconnect(db_conn)
    } else {
        dbDisconnect(db_conn)
    }
}
#
# read test dates from a csv file
#
read_test_dates <- function(filename)
{
    test_dates <- read_csv_file(filename,"TEST DATES")
    rownames(test_dates) <- test_dates[,1]
    return(data.frame(NAME=c("START_DATE", 
                             "END_DATE",
                             "MODULESN_START_DATE",
                             "MODULESN_END_DATE",
                             "HEALTHY_FLAG_DATE"),
                      VALUE=c(test_dates["START_DATE","VALUE"],
                              test_dates["END_DATE","VALUE"],
                              test_dates["MODULESN_START_DATE","VALUE"],
                              test_dates["MODULESN_END_DATE","VALUE"],
                              test_dates["HEALTHY_FLAG_DATE","VALUE"])))
}
#
# generate a test period 
#
get_test_period <- function(number_of_days, 
                            modulesn_number_of_days=1)
{
    test_dates <- data.frame()
    #
    if (file.exists("test_dates.csv")) {
        #
        # get test dates from a file 
        #
        test_dates <- read_test_dates("test_dates.csv")
    } else {
        #
        # sanity check
        #
        if (number_of_days <= 0) {
            stop(sprintf("INFO: Invalid number of days given: %s", 
                         number_of_days))
        }
        #
        # calculate date range
        #
        now = Sys.Date()
        test_date_format <- ""
        if ((config_type == "dx") ||
            (config_type == "spark")) {
            # return a date comparable to character transaction date field
            test_date_format <- "%Y-%m-%d"
            healthy_date_format <- "%Y%m%d"
        } else if (config_type == "ida") {
            # return a date comparable to Date datetimestamplocal
            test_date_format <- "%m/%d/%Y %H:%M:%S"
            healthy_date_format <- "%m%d%Y"
        } else {
            stop(sprintf(INFO: "Unknown configuration type: %s", 
                         config_type))
        }
        #
        test_dates <- 
            data.frame(NAME=c("START_DATE", 
                              "END_DATE",
                              "MODULESN_START_DATE",
                              "MODULESN_END_DATE",
                              "HEALTHY_FLAG_DATE"),
                       VALUE=c(format(now-number_of_days, 
                                      test_date_format),
                               format(now, 
                                      test_date_format),
                               format(now-modulesn_number_of_days, 
                                      test_date_format),
                               format(now, 
                                      test_date_format),
                               format(now-1, 
                                      healthy_date_format)))
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
        stop(sprintf("INFO: %s file was not given.", type_of_file))
    } else if ( ! file.exists(filename)) {
        stop(sprintf("INFO: %s file %s does not exist.", type_of_file, filename))
    }
    #
    return(read.csv(filename, comment.char="#", stringsAsFactors=FALSE))
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
    print(paste("INFO UNMATCHED:", unlist(regmatches(query,gregexpr("<[A-Z0-9_]+>", query)))))
    #
    return(query)
}
#
# save data to a file
#
make_save_to_file <- function()
{
    # use a closure to save values between calls
    already_seen <- c()
    #
    save_to_file <- function(data, file_name)
    {
        append <- TRUE
        #
        if ( ! (file_name %in% already_seen)) {
            append <- FALSE
            already_seen <<- c(already_seen, file_name)
        }
        #
        cat(data,file=file_name,sep="\n",append=append)
    }
    return(save_to_file)
}
#
save_to_file <- make_save_to_file()
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
    total_errors <- 0
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
        total_errors <<- total_errors + 1
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
    eh$total_errors <- function(x=NA) {
        if ( ! is.na(x)) {
            total_errors <<- x
        }
        return(total_errors)
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
# write all the records to the results file
#
make_write_results <- function()
{
    # use a closure to save values between calls
    append <- FALSE
    col.names <- TRUE
    #
    write_results <- function(results,
                              keep=c("MODULESN",
                                     "FLAG_DATE",
                                     "PL",
                                     "PHN_PATTERNS_SK",
                                     "IHN_LEVEL3_DESC",
                                     "FLAG_YN",
                                     "CHART_DATA_VALUE"))
    {
        if (nrow(results) > 0) {
            #
            # upper case columns names for filtering and printing
            #
            names(results) <- toupper(names(results))
            #
            # strip out unwanted columns and rename columns
            #
            results <- subset(results, select=keep)
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
                        append=append,
                        row.names=FALSE,
                        col.names=col.names,
                        sep=",")
            append <<- TRUE
            col.names <<- FALSE
        }
    }
    #
    return(write_results)
}
#
write_results <- make_write_results()
#
# generate an empty list
#
empty_results <- function()
{
    new_cnms <- c("PHN_PATTERNS_SK",
                  "PL",
                  "MODULESN",
                  "FLAG_DATE",
                  "CHART_DATA_VALUE",
                  "FLAG_YN",
                  "IHN_LEVEL3_DESC")
    new_nc <- length(new_cnms)
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
                       test_period)
{
    #
    # substitute values into queries
    #
    query <- query_subs(query_template, test_period, "VALUE")
    query <- query_subs(query, params, "PARAMETER_VALUE")
    save_to_file(query, "query.sql")
    query <- gsub("[\n\r]", " ", query)
    #
    # execute query
    #
    tryCatch({
        errors$occurred(FALSE)
        query_time <- system.time({
            query_results <- dbGetQuery(db_conn, query)
        })},
        error=errors$handler
    )
    #
    if (errors$occurred()) {
        #
        # error caught. return an empty data frame
        #
        print(sprintf("INFO: ERROR CAUGHT. NROW = %d", 0))
        return(empty_results())
    } else if (nrow(query_results) == 0) {
        #
        # nothing found. return an empty data frame
        #
        print(sprintf("INFO: NROW = %d", 0))
        print(query_time)
        return(empty_results())
    }
    #
    print(sprintf("INFO: NROW = %d", nrow(query_results)))
    print(query_time)
    #
    return(query_results)
}
#
# processing of flagged query results. overwritten by algorithm
# if necessary.
#
default_post_flagged_processing <- function(flagged_results, 
                                            db_conn, 
                                            params, 
                                            options, 
                                            test_period)
{
    #
    # does nothing by default.
    #
    return(flagged_results)
}
#
post_flagged_processing <- default_post_flagged_processing
#
# load parquet files for spark, if needed. default version
# does nothing. is overwritten by algorithm when needed.
#
default_spark_load_data <- function(db_conn,
                                    param_sets, 
                                    test_period)
{
}
#
spark_load_data <- default_spark_load_data
