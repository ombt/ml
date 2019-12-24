#
# common utility functions
#
#########################################################################
#
# connect to database
#
#
# connect to database
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
    if (config["ATHENA_CREDENTIAL_PROVIDER","VALUE"] == "custom") {
        return(dx_custom_connect_to_db(config))
    } else {
        return(dx_saml_connect_to_db(config))
    }
}
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
    spark_conn <- spark_connect(master=config["SPARK_MASTER","VALUE"],
                                config=spark_cfg, 
                                version=config["SPARK_VERSION","VALUE"])
    #
    return(spark_conn)
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
# generate a test period 
#
get_test_period <- function(number_of_days)
{
    if (file.exists("test_dates.csv")) {
        test_dates <- read.csv("test_dates.csv", stringsAsFactors=FALSE)
        rownames(test_dates) <- test_dates[,1]
        test_dates <- 
            data.frame(NAME=c("START_DATE", "END_DATE"),
                       VALUE=c(test_dates["START_DATE","VALUE"],
                               test_dates["END_DATE","VALUE"]))
    } else if (number_of_days <= 0) {
        stop(sprintf("Invalid number of days given: %s", number_of_days))
    } else {
        now = Sys.Date()
        test_date_format <- "%Y-%m-%d"
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
# post-processing if needed. default is do nothing.
#
default_post_processing <- function(results,
                                    params, 
                                    db_conn, 
                                    sql_query)
{
    return(results);
}
#
post_processing <- default_post_processing
#
# common code for handling results if a flagged vector is generated.
#
flagged_post_processing <- function(results, flagged)
{
    flagged_modulesn <- unique(results[flagged, "MODULESN"])
    #
    flagged_has_rows <- FALSE
    flagged_results <- results[results$MODULESN %in% flagged_modulesn, ]
    if (nrow(flagged_results) > 0) {
        flagged_has_rows <- TRUE
        flagged_results <- within(flagged_results,
        {
            FLAG_YN <- 1
            CHART_DATA_VALUE <- 1
            for (modulesn in flagged_modulesn) {
                FLAG_DATE[MODULESN == modulesn] <-
                    max(FLAG_DATE[MODULESN == modulesn])
            }
            # new variables are added as new columns, so delete unwanted ones.
            rm(modulesn)
        })
    }
    #
    not_flagged_has_rows <- FALSE
    not_flagged_results <- results[ ! (results$MODULESN %in% flagged_modulesn), ]
    if (nrow(not_flagged_results) > 0) {
        not_flagged_has_rows <- TRUE
        not_flagged_results <- within(not_flagged_results,
        {
            FLAG_YN <- 0
            CHART_DATA_VALUE <- 0
            FLAG_DATE <- substr(max(FLAG_DATE),1,8)
        })
    }
    #
    if (flagged_has_rows && not_flagged_has_rows) {
        return(rbind(flagged_results, not_flagged_results))
    } else if (flagged_has_rows) {
        return(flagged_results)
    } else if (not_flagged_has_rows) {
        return(not_flagged_results)
    } else {
        return(results)
    }
}
