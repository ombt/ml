
#
athena_open_db <- function(db_name = NA,
                           db_host = NA,
                           db_port = NA,
                           abbott_511,
                           db_user,
                           db_password,
                           athena_classpath)
{
    install_and_load <- function (package1, ...)
    {
        verbose <- FALSE
        #
        # convert arguments to vector
        #
        packages <- c(package1, ...)
        #
        # check if loaded and installed
        #
        loaded        <- packages %in% (.packages())
        names(loaded) <- packages
        #
        installed        <- packages %in% rownames(installed.packages())
        names(installed) <- packages
        #
        # start loop to determine if each package is installed
        #
        load_it <- function (package, loaded, installed)
        {
            if (loaded[package])
            {
                if (verbose) print(paste(package, "is loaded"))
            }
            else
            {
                if (verbose) print(paste(package, "is not loaded"))
                if (installed[package])
                {
                    if (verbose) print(paste(package, "is installed"))
                    if (verbose) print(paste("loading", package))
                    do.call("library", list(package))
                }
                else
                {
                    if (verbose) print(paste(package, "is not installed"))
                    if (verbose) print(paste("installing", package))
                    install.packages(package)
                    if (verbose) print(paste("loading", package))
                    do.call("library", list(package))
                }
            }
        }
        #
        lapply(packages, load_it, loaded, installed)
    }
    #
    if ( ! file.exists(athena_classpath))
    {
        stop(sprintf("Cannot find Athena Jar file: %s", 
                     athena_classpath))
    }
    #
    # check if a package is installed. if not, then install
    # and download.
    #
    status <- install_and_load("DBI", "RJDBC")
    #
    db_driver <- RJDBC::JDBC(driverClass="com.simba.athena.jdbc.Driver",
                             classPath = athena_classpath, 
                             identifier.quote="'")
    #
    db_conn <- list()
    #
    db_conn <- dbConnect(db_driver, 
                        "jdbc:awsathena://awsregion=us-east-1",
                         LogLevel = "6",
                         LogPath = file.path(Sys.getenv("HOME"), "logs"),
                         workgroup = "add_service_dx_readonly",
                         UseResultSetStreaming = "0",
                         S3OutputLocation = paste0("s3://abt-bdaa-test-us-east-1-sandbox/athena/", abbott_511),
                         AwsCredentialsProviderClass = "com.simba.athena.amazonaws.auth.profile.ProfileCredentialsProvider",
                         AwsCredentialsProviderArguments = "saml",
                         s3_staging_dir="s3://abt-bdaa-test-us-east-1-sandbox/athenaquerylog/",
                         user = db_user,
                         password = db_password)
    #
    # return(list("conn"=db_conn, "tbls"=dbListTables(db_conn)))
    #
    return(list("conn"=db_conn, "tbls"=NA))
}
#
# close connection
#
athena_close_db <- function(db)
{
    status = dbDisconnect(db$conn)
}
#
# execute a query
#
athena_exec_query <- function(db, query)
{
    return(dbGetQuery(db$conn, query))
}
#
# execute a query
#
athena_exec_query_return_matrix <- function(db, query)
{
    return(as.matrix(dbGetQuery(db$conn, query)))
}
#
# connect to specific IDA DBs
#
connect_to_athena <- function(abbott_511 = "RUMORMX",
                              athena_classpath = 
                                  file.path(Sys.getenv("HOME"), 
                                           "jlib/athena/AthenaJDBC41_2.0.7.jar"))
{
    read_temp_creds <- function(path)
    {
        if (file.exists(path))
        {
            creds <- readLines(path)
            tmp   <- strsplit(creds, " = ")
            creds_df <- data.frame("attr" = sapply(tmp, "[", 1), 
                                   "val" = sapply(tmp, "[", 2),
                                   stringsAsFactors=FALSE)
            creds_df <- creds_df[!is.na(creds_df$attr), ]
            #
            if (("aws_access_key_id" %in% creds_df$attr) &
                ("aws_secret_access_key" %in% creds_df$attr))
            {
                athena_username <- 
                    creds_df$val[creds_df$attr=="aws_access_key_id"]
                athena_password <- 
                    creds_df$val[creds_df$attr=="aws_secret_access_key"]
                return(list(athena_username = athena_username,
                            athena_password = athena_password))
            }
            else
            {
                stop("aws_access_key_id or aws_secret_access_key not found in credentials file")
            }
        }
        else
        {
            stop(sprintf("No credentials file exists at %s", path))
        }
    }
    #
    temp_creds <- read_temp_creds(file.path(Sys.getenv("HOME"), 
                                            "../.aws/credentials"))
    #
    db_conn = athena_open_db(abbott_511 = abbott_511,
                             db_user = temp_creds$athena_username,
                             db_password = temp_creds$athena_password,
                             athena_classpath = athena_classpath)
    #
    return(db_conn)
}
#
athena_exec_sql_file <- function(db_conn=NA, filename=NA)
{
    #
    # sanity checks
    #
    if (is.na(filename))
    {
        stop("Filename is NA.")
    }
    else if ( ! file.exists(filename))
    {
        stop(sprintf("File %s does not exist.", filename))
    }
    #
    # connect to a db if a connection was not passed in
    #
    close_db <- FALSE
    if ( ! ("conn" %in% names(db_conn)))
    {
        db_conn <- connect_to_athena()
        close_db <- TRUE
    }
    #
    # read in file
    #
    contents <- readLines(filename)
    sql_query <- paste(contents, collapse=' ')
    #
    # execute the query
    #
    print(sprintf("SQL Query: %s", sql_query))
    results <- athena_exec_query(db_conn, sql_query);
    #
    # close db connection
    #
    if (close_db == TRUE)
    {
        athena_close_db(db_conn)
    }
    #
    # return results
    #
    return(results)
}

#
# IDA connection data ...
#
# dabbto, dabbto.world, dabbto.oracleoutsourcing.com, dabbto.oneabbott.com =
#   (DESCRIPTION =
#     (ADDRESS =
#       (PROTOCOL = TCP)
#       (HOST = ux00033q-scan.oneabbott.com)
#       (PORT = 1521)
#     )
#     (CONNECT_DATA =
#     (SERVER = DEDICATED)
#     (SERVICE_NAME = dabbto_dcu)
#     )
#   )
#  
# tabbto, tabbto.world, tabbto.oracleoutsourcing.com, tabbto.oneabbott.com =
#   (DESCRIPTION =
#     (ADDRESS =
#       (PROTOCOL = TCP)
#       (HOST = ux00033q-scan.oneabbott.com)
#       (PORT = 1521)
#     )
#     (CONNECT_DATA =
#     (SERVER = DEDICATED)
#     (SERVICE_NAME = tabbto_dcu)
#     )
#   )
#  
# pabbto, pabbto.world, pabbto.oracleoutsourcing.com, pabbto.oneabbott.com =
#  (DESCRIPTION =
#     (ADDRESS =
#       (PROTOCOL = TCP)
#       (HOST = ux00147p-scan.oneabbott.com)
#       (PORT = 1521)
#     )
#     (CONNECT_DATA =
#     (SERVER = DEDICATED)
#     (SERVICE_NAME = pabbto_dcu)
#     )
#   )
# 
ida_open_db <- function(db_name, 
                        db_host,
                        db_port,
                        db_user,
                        db_password,
                        jdbc_classpath)
{
    install_and_load <- function (package1, ...)
    {
        verbose <- FALSE
        #
        # convert arguments to vector
        #
        packages <- c(package1, ...)
        #
        # check if loaded and installed
        #
        loaded        <- packages %in% (.packages())
        names(loaded) <- packages
        #
        installed        <- packages %in% rownames(installed.packages())
        names(installed) <- packages
        #
        # start loop to determine if each package is installed
        #
        load_it <- function (package, loaded, installed)
        {
            if (loaded[package])
            {
                if (verbose) print(paste(package, "is loaded"))
            }
            else
            {
                if (verbose) print(paste(package, "is not loaded"))
                if (installed[package])
                {
                    if (verbose) print(paste(package, "is installed"))
                    if (verbose) print(paste("loading", package))
                    do.call("library", list(package))
                }
                else
                {
                    if (verbose) print(paste(package, "is not installed"))
                    if (verbose) print(paste("installing", package))
                    install.packages(package)
                    if (verbose) print(paste("loading", package))
                    do.call("library", list(package))
                }
            }
        }
        #
        lapply(packages, load_it, loaded, installed)
    }
    #
    # check if a package is installed. if not, then install
    # and download.
    #
    status <- install_and_load("DBI", "RJDBC")
    #
    db_driver <- RJDBC::JDBC(driverClass="oracle.jdbc.OracleDriver", 
                             classPath=jdbc_classpath)
    #
    db_conn <- list()
    #
    db_conn_string <- sprintf("jdbc:oracle:thin:@//%s:%d/%s",
                              db_host,
                              db_port,
                              db_name)
    print(sprintf("DB Connection string: <%s>", db_conn_string))
    #
    db_conn <- dbConnect(db_driver, 
                         db_conn_string,
                         db_user,
                         db_password)
    #
    # return(list("conn"=db_conn, "tbls"=dbListTables(db_conn)))
    #
    return(list("conn"=db_conn, "tbls"=NA))
}
#
# close connection
#
ida_close_db <- function(db)
{
    status <- dbDisconnect(db$conn)
}
#
# execute a query
#
ida_exec_query <- function(db, query)
{
    return(dbGetQuery(db$conn, query))
}
#
# execute a query
#
ida_exec_query_return_matrix <- function(db, query)
{
    return(as.matrix(dbGetQuery(db$conn, query)))
}
#
# connect to specific IDA DBs
#
connect_to_dabbto <- function()
{
    # dabbto, dabbto.world, dabbto.oracleoutsourcing.com, dabbto.oneabbott.com =
    #   (DESCRIPTION =
    #     (ADDRESS =
    #       (PROTOCOL = TCP)
    #       (HOST = ux00033q-scan.oneabbott.com)
    #       (PORT = 1521)
    #     )
    #     (CONNECT_DATA =
    #     (SERVER = DEDICATED)
    #     (SERVICE_NAME = dabbto_dcu)
    #     )
    #   )
    #
    return(ida_open_db(db_name="dabbto" ,
                       db_host="ux00033q-scan.oneabbott.com",
                       db_port=1521,
                       db_user="SVC_PHM_CONNECT",
                       db_password="svc_phmc_0771",
                       jdbc_classpath=file.path(Sys.getenv("HOME"), 
                                               "jlib/OJDBC-Full/ojdbc6.jar")))
}
#
connect_to_pabbto <- function()
    {
    #  
    # pabbto, pabbto.world, pabbto.oracleoutsourcing.com, pabbto.oneabbott.com =
    #  (DESCRIPTION =
    #     (ADDRESS =
    #       (PROTOCOL = TCP)
    #       (HOST = ux00147p-scan.oneabbott.com)
    #       (PORT = 1521)
    #     )
    #     (CONNECT_DATA =
    #     (SERVER = DEDICATED)
    #     (SERVICE_NAME = pabbto_dcu)
    #     )
    #   )
    # 
    return(ida_open_db(db_name="pabbto" ,
                       db_host="ux00147p-scan.oneabbott.com",
                       db_port=1521,
                       db_user="SVC_PHM_CONNECT",
                       db_password="svc_phmc_0771",
                       jdbc_classpath=file.path(Sys.getenv("HOME"), 
                                               "jlib/OJDBC-Full/ojdbc6.jar")))
}

ida_exec_sql_file <- function(filename=NA, db_conn=NA, db_name=NA)
{
    #
    # sanity checks
    #
    if (is.na(filename))
    {
        stop("Filename is NA.")
    }
    else if ( ! file.exists(filename))
    {
        stop(sprintf("File %s does not exist.", filename))
    }
    #
    # connect to a db
    #
    close_db <- FALSE
    if ( ! ("conn" %in% names(db_conn)))
    {
        if (is.na(db_name))
        {
            stop("Database name is NA.")
        }
        else if (db_name == "pabbto")
        {
            db_conn <- connect_to_pabbto()
        }
        else if (db_name == "dabbto")
        {
            db_conn <- connect_to_dabbto()
        }
        else
        {
            stop(sprintf("Unknown IDA database"))
        }
        close_db <- TRUE
    }
    #
    # read in file
    #
    contents <- readLines(filename)
    sql_query <- paste(contents, collapse=' ')
    #
    # execute the query
    #
    print(sprintf("SQL Query: %s", sql_query))
    results <- ida_exec_query(db_conn, sql_query);
    #
    # close db connection
    #
    if (close_db == TRUE)
    {
        ida_close_db(db_conn)
    }
    #
    # return results
    #
    return(results)
}

