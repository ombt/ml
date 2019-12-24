#
# common utility functions
#
#########################################################################
#
# function: 	reliability_dsn_connect_to_db
#
# description: 	connect to Reliability database using an existing DSN.
#
# parameters: 	config - data.frame created from reading config file.
#
# returns: 	ODBC database connection
#
#########################################################################
#
reliability_dsn_connect_to_db <- function(config)
{
    db_conn <- dbConnect(odbc::odbc(), config["RELIABILITY_DB_NAME","VALUE"])
    #
    return(db_conn)
}
#
#########################################################################
#
# function: 	reliability_non_dsn_connect_to_db
#
# description:	connect to Reliability database using using DB server,
#		user, password, DB name defined in config file data.frame
#
# parameters: 	config - data.frame created from reading config file.
#
# returns: 	ODBC database connection
#
#########################################################################
#
reliability_non_dsn_connect_to_db <- function(config)
{
    print(sprintf("INFO: Reliability DB Non-DSN Connection: %s", config["RELIABILITY_DB_NAME","VALUE"]))
    #
    # db_conn <- dbConnect(odbc(),
    #                      Driver='SQL Server',
    #                      Server=config["RELIABILITY_DB_SERVER","VALUE"],
    #                      Database=config["RELIABILITY_DB_NAME","VALUE"],
    #                      UID=config["RELIABILITY_DB_USER","VALUE"],
    #                      PWD=config["RELIABILITY_DB_PASSWORD","VALUE"])
    #
    db_conn <- 
        dbConnect(odbc::odbc(),
                  Driver=config["SQL_SERVER_ODBC_DRIVER_NAME","VALUE"] ,
                  Server=paste(config["RELIABILITY_DB_SERVER_NAME","VALUE"],",",config["RELIABILITY_DB_INSTANCE_PORT","VALUE"]),
                  Database=config["RELIABILITY_DB_NAME","VALUE"],
                  UID=config["RELIABILITY_DB_USER","VALUE"],
                  PWD=config["RELIABILITY_DB_PASSWORD","VALUE"],
                  Port=config["RELIABILITY_DB_PORT","VALUE"])
    #
    return(db_conn)
}
#
#########################################################################
#
# function:	reliability_connect_to_db
#
# description:	connects to Reliability database using DSN or non-DSN 
#		connection method.
#
# parameters: 	config - data.frame created from reading config file.
#
# returns: 	ODBC database connection
#
#########################################################################
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
#########################################################################
#
# function:	dx_custom_connect_to_db
#
# description:	connects to DX database using a custom JDBC class.
#
# parameters: 	config - data.frame created from reading config file.
#
# returns: 	JDBC database connection
#
#########################################################################
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
#########################################################################
#
# function:	dx_saml_connect_to_db
#
# description:	connect DX datanbase using a SAML-based connection. 
#
# parameters: 	config - data.frame created from reading config file.
#
# returns: 	JDBC database connection
#
#########################################################################
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
#########################################################################
#
# function:	dx_connect_to_db
#
# description:	main entry point for connecting to DX database. 
#
# parameters: 	config - data.frame created from reading config file.
#
# returns: 	JDBC database connection
#
#########################################################################
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
#########################################################################
#
# function:	spark_connect_to_db
#
# description:	cpnnect to DX database using Spark
#
# parameters: 	config - data.frame created from reading config file.
#
# returns:	SPARK database connection 
#
#########################################################################
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
#########################################################################
#
# function:	connect_to_db
#
# description:	main entry point to connect to a remote database: 
#		DX via Athena or DX via Spark.
#
# parameters: 	config - data.frame created from reading config file.
#
# returns: 	JDBC Athena or SPARK database connection
#
#########################################################################
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
#########################################################################
#
# function:	disconnect_from_db
#
# description:	main entry to disconnect from DX database 
#
# parameters:	db_conn - database connection object
#
# returns: 	nothing
#
#########################################################################
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
#########################################################################
#
# function:	read_test_dates
#
# description:	read test dates from a csv file
#
# parameters:	filename - name of test dates file
#
# returns:	test dates data.frame (see below)
#
#########################################################################
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
#########################################################################
#
# function:	get_test_period
#
# description:	generate a test period for algorithm
#
# parameters:	number_of_days - number of days for algorithm
#		modulesn_number_of_days - number of days for healthy 
#			instrument list.
#
# returns:	data frame with test dates. see below.
#
#		data.frame(NAME=c("START_DATE", 
#                                 "END_DATE",
#                                 "MODULESN_START_DATE",
#                                 "MODULESN_END_DATE",
#                                 "HEALTHY_FLAG_DATE"),
#                          VALUE=c(format(now-number_of_days, 
#                                         test_date_format),
#                                  format(now, 
#                                         test_date_format),
#                                  format(now-modulesn_number_of_days, 
#                                         test_date_format),
#                                  format(now, 
#                                         test_date_format),
#                                  format(now-1, 
#                                         healthy_date_format)))
#
#########################################################################
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
#########################################################################
#
# function:	read_csv_file
#
# description:	read a csv file with sanity checks
#
# parameters:	filename - path to CSV file
#		type_of_file - label for type of file used in errors
#
# returns: 	data.frame defined by contents of CSV file. comments
#		starting with '#' are allowed and strings are not converted
#		to factors.
#
#########################################################################
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
#########################################################################
#
# function:	query_subs
#
# description:	substitute parameters for variables in query
#
# parameters:	query_template - query containing variables with the forma
#			"<name of variable>".
#		substitutions - data frame used for substitutions contains
#			columns which correspond to names and values. the rows
#			are names after the values in the names column.
#		value_column_name - name of column containing the values.
#
# returns:	query with variables replaced with values from the
#		substitutions data.frame. some variables may not be
#		defined using this substitutions data frame.
#
#########################################################################
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
    unmatched <- paste(unique(sort(unlist(regmatches(query,
                                                     gregexpr("<[A-Z0-9_]+>", 
                                                              query))))), collapse=' ')
    if (nchar(unmatched) > 0) {
        print(sprintf("INFO: UNMATCHED VARS: %s", unmatched))
    } else {
        print("INFO: ALL VARS WERE MATCHED")
    }
    #
    return(query)
}
#
#########################################################################
#
# function:	make_save_to_file
#
# description:	create function to save data to a file
#
# parameters:	none
#
# returns: 	save_to_file function and attached scope
#
#########################################################################
#
make_save_to_file <- function()
{
    # use a closure to save values between calls
    already_seen <- c()
    #
    #########################################################################
    #
    # function:		save_to_file
    #
    # description:	function to save data to a file
    #
    # parameters:	data - data to save 
    #			file_name - file path to store data
    #
    # returns:		none
    #
    #########################################################################
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
#########################################################################
#
# function:	make_error_handler
#
# description:	create a function to write error messages to errors.csv
#
# parameters:	none
#
# returns: 	pseudo-object error handler
#
#########################################################################
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
    #########################################################################
    #
    # function:		handler
    #
    # description:	callback used by try-catch block in case of an error
    #
    # parameters:	emdg -	error message, usually the message used in 
    #				the stop() call.
    #
    # returns:		none
    #
    #########################################################################
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
    #########################################################################
    #
    # function:		phm_patterns_sk
    #
    # description:	set/get function
    #
    # parameters:	phm_patterns_sk - new value or NA
    #
    # returns:		current value of phm_patterns_sk
    #
    #########################################################################
    #
    eh$phm_patterns_sk <- function(x=NA) {
        if ( ! is.na(x)) {
            phm_patterns_sk <<- x
        }
        return(phm_patterns_sk)
    }
    #
    #########################################################################
    #
    # function:		occurred
    #
    # description:	set/get function
    #
    # parameters:	occurred - new value or NA
    #
    # returns:		current value of occurred
    #
    #########################################################################
    #
    eh$occurred <- function(x=NA) {
        if ( ! is.na(x)) {
            occurred <<- x
        }
        return(occurred)
    }
    #
    #########################################################################
    #
    # function:		total_errors
    #
    # description:	set/get function
    #
    # parameters:	total_errors - new value or NA
    #
    # returns:		current value of total_errors
    #
    #########################################################################
    #
    eh$total_errors <- function(x=NA) {
        if ( ! is.na(x)) {
            total_errors <<- x
        }
        return(total_errors)
    }
    #
    #########################################################################
    #
    # function:		file_name
    #
    # description:	set/get function
    #
    # parameters:	file_name - new value or NA
    #
    # returns:		current value of file_name
    #
    #########################################################################
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
#########################################################################
#
# function:	make_write_results
#
# description:	create a function to write all the records to the results file
#
# parameters:	none
#
# returns: 	write_results function and scope
#
#########################################################################
#
make_write_results <- function()
{
    # use a closure to save values between calls
    append <- FALSE
    col.names <- TRUE
    #
    #########################################################################
    #
    # function:		write_results
    #
    # description:	write data to results.csv file
    #
    # parameters:	results - data to save 
    #			keep - list of fields to keep. 
    #
    # returns:		none
    #
    #########################################################################
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
            # results <- distinct(results)
            results <- results %>% distinct(PHN_PATTERNS_SK,
                                            PL,
                                            MODULESN,
                                            CHART_DATA_VALUE,
                                            FLAG_YN,
                                            IHN_LEVEL3_DESC,
                                           .keep_all=TRUE)
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
#########################################################################
#
# function:	make_write_data
#
# description:	create a function to write data out
#
# parameters:	none
#
# returns: 	write_data fucntion and scope
#
#########################################################################
#
make_write_data <- function()
{
    # use a closure to save values between calls
    already_seen <- c()
    #
    #########################################################################
    #
    # function:		write_data
    #
    # description:	function to save data to a file
    #
    # parameters:	data - data to save 
    #			file_name - file path to store data
    #
    # returns:		none
    #
    #########################################################################
    #
    write_data <- function(records, file_name, use_col_names=FALSE)
    {
        if (nrow(records) > 0) {
            append <- TRUE
            col.names <- FALSE
            #
            if ( ! (file_name %in% already_seen)) {
                append <- FALSE
                col.names <- TRUE
                already_seen <<- c(already_seen, file_name)
            }
            #
            if (use_col_names) {
                col.names <- TRUE
            }
            #
            names(records) <- toupper(names(records))
            suppressWarnings(
                write.table(records,
                            file=file_name,
                            append=append,
                            row.names=FALSE,
                            col.names=col.names,
                            sep=","))
        }
    }
    #
    return(write_data)
}
#
write_data <- make_write_data()
#
#########################################################################
#
# function:	empty_results
#
# description:	generate an empty list
#
# parameters:	none
#
# returns: 	data frame for results.csv with correct columns, but 
#		with zero rows.
#
#########################################################################
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
#########################################################################
#
# function:	exec_query
#
# description:	generate a query and execute it. process results afterwards.
#
# parameters:	params - one parameter set from list of parameter sets
#			in input.csv file.
#		db_conn - database connection created by connect_to_db()
#		query_template - query with variables 
#		test_period - test period data.frame created by 
#			get_test_period()
#
# returns: 	query results set
#
#########################################################################
#
exec_query <- function(params, 
                       db_conn, 
                       query_template, 
                       test_period)
{
    #
    # substitute values into queries
    #
    query <- query_template
    if (nrow(test_period) > 0) {
        query <- query_subs(query_template, test_period, "VALUE")
    }
    if (nrow(params) > 0) {
        query <- query_subs(query, params, "PARAMETER_VALUE")
    }
    #
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
#########################################################################
#
# function:	default_post_flagged_processing
#
# description:	default function for processing of flagged query results. 
#		it does nothing. overwritten by algorithm if necessary.
#
# parameters:	flagged_results - query results from flagged query
#		db_conn - database connection created by connect_to_db()
#		params - one parameter set from list of parameter sets
#			in input.csv file.
#		test_period - test period data.frame created by 
#			get_test_period()
#
# returns: 	flagged results after processing
#
#########################################################################
#
default_post_flagged_processing <- function(flagged_results, 
                                            db_conn, 
                                            params, 
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
#########################################################################
#
# function:	default_pre_flagged_processing
#
# description:	default function for pre-processing of flagged query results. 
#		it does nothing. overwritten by algorithm if necessary.
#
# parameters:	flagged_results - query results from flagged query
#		db_conn - database connection created by connect_to_db()
#		params - one parameter set from list of parameter sets
#			in input.csv file.
#		test_period - test period data.frame created by 
#			get_test_period()
#
# returns: 	flagged results after processing
#
#########################################################################
#
default_pre_flagged_processing <- function(flagged_results, 
                                           db_conn, 
                                           params, 
                                           test_period)
{
    #
    # does nothing by default.
    #
    return(flagged_results)
}
#
pre_flagged_processing <- default_pre_flagged_processing
#
#########################################################################
#
# function:	default_spark_load_data
#
# description:	load parquet files for spark, if needed. default version
# 		does nothing. is overwritten by algorithm when needed.
#
# parameters:	db_conn - database connection created by connect_to_db()
#		params - one parameter set from list of parameter sets
#			in input.csv file.
#		test_period - test period data.frame created by 
#			get_test_period()
#
# returns: 	none
#
#########################################################################
#
default_spark_load_data <- function(db_conn,
                                    param_sets, 
                                    test_period)
{
}
#
spark_load_data <- default_spark_load_data
#
#########################################################################
#
# function:	default_generate_suppression
#
# description:	default function to generate list of suppressed module SNs.
#		by default it does nothing. it can be overwritten by
#		an algorithm.
#
# parameters:	params - one parameter set from list of parameter sets
#			in input.csv file.
#		rel_db_conn - Reliability database connection
#		test_period - test period data.frame created by 
#			get_test_period()
#
# returns:	list of suppressed SNs
#
#########################################################################
#
default_generate_suppression <- function(params, 
                                         rel_db_conn, 
                                         test_period)
{
    return(empty_results())
}
#
generate_suppression <- default_generate_suppression
#
