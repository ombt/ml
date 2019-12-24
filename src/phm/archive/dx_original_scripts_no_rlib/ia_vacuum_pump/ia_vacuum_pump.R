#
# Alinity IA Vacuum Pump
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
# local functions
#
usage <- function() {
    print("usage: script.R [-h] [-C] -w work.dir [-c config.file] [-p param.file] [-o output.file]")
}
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
flagged_append <- FALSE
not_flagged_append <- FALSE
#
save_query <- function(query, flagged)
{
    if (flagged == "Y") {
        cat(query,file="flagged.sql",sep="\n",append=flagged_append)
        flagged_append <<- TRUE
    } else {
        cat(query,file="not_flagged.sql",sep="\n",append=not_flagged_append)
        not_flagged_append <<- TRUE
    }
}
#
exec_query <- function(params, 
                       db_conn, 
                       query_template, 
                       config, 
                       options, 
                       flagged)
{
    #
    # easy to access parameters if we assign row names
    #
    rownames(params) <- params[,"PARAMETER_NAME"]
    #
    # substitute values into query
    #
    query <- query_subs(query_template, config, "VALUE")
    query <- query_subs(query, params, "PARAMETER_VALUE")
    save_query(query, flagged)
    query <- gsub("[\n\r]"," ", query)
    #
    query_time <- system.time({
        results <- dbGetQuery(db_conn, query)
    })
    print(query_time)
    #
    if (nrow(results) == 0) {
        #
        # create an empty data frame with the correct columns
        #
        new_nc <- ncol(results) + 4
        new_cnms <- c(colnames(results),
                      "FLAG_DATE",
                      "PHN_PATTERNS_SK",
                      "IHM_LEVEL3_DESC",
                      "THRESHOLD_DESCRIPTION")
        empty_results <- data.frame(matrix(ncol=new_nc, nrow=0))
        colnames(empty_results) <- new_cnms
        #
        return(empty_results)
    }
    #
    # add extra columns required in the output file
    #
    results$FLAG_DATE <- config["START_DATE", 
                                "VALUE"]
    results$PHN_PATTERNS_SK <- unique(params[ , "PHM_PATTERNS_SK_DUP"])[1]
    results$IHM_LEVEL3_DESC <- params["IHN_LEVEL3_DESC",
                                      "PARAMETER_VALUE"]
    results$THRESHOLD_DESCRIPTION <- params["THRESHOLD_DESCRIPTION",
                                            "PARAMETER_VALUE"]
    #
    results$FLAG_YN <- flagged
    results$CHART_DATA_VALUE <- ifelse((flagged == "Y"), 1, 0)
    results$CHART_DATA_VALUE_TYPE <- "FLAGGED"
    #
    if ((flagged == "Y") || (options$chart)) {
        results_num_evals <- results
        results_num_evals$CHART_DATA_VALUE <- results$num_evals
        results_num_evals$CHART_DATA_VALUE_TYPE <- "NUM_EVALS"
        #
        results_min_adcvalue <- results
        results_min_adcvalue$CHART_DATA_VALUE <- results$min_adcvalue
        results_min_adcvalue$CHART_DATA_VALUE_TYPE <- "MIN_ADCVALUE"
        #
        return(rbind(results, 
                     results_num_evals,
                     results_min_adcvalue))
    } else {
        return(results)
    }
}
#
exec_flagged_query <- function(param_sets, db_conn, config, options)
{
    #
    # build query
    #
    query_template <- "
select
    evals.moduleserialnumber,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.moduleserialnumber,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.moduleserialnumber,
            vpd.adcvalue
        from
            dx.dx_205_vacuumpressuredata vpd 
        where  
            date_parse('<START_DATE>', 
                       '%m/%d/%Y %T') <= vpd.datetimestamplocal
        and 
            vpd.datetimestamplocal < date_parse('<END_DATE>',
                                                '%m/%d/%Y %T') 
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.moduleserialnumber
    ) evals
where
    evals.min_adcvalue > <VACUUMPUMP_MINADC>
and 
    evals.num_evals >= <VACUUMPUMP_NUMEVALS>
order by
    evals.moduleserialnumber"
    #
    flagged <- "Y"
    #
    results <- lapply(param_sets, 
                      exec_query, 
                      db_conn, 
                      query_template,
                      config,
                      options,
                      flagged)
    #
    return(results)
}
#
exec_not_flagged_query <- function(param_sets, db_conn, config, options)
{
    #
    # build query
    #
    query_template <- "
select
    evals.moduleserialnumber,
    evals.num_evals,
    evals.min_adcvalue
from (
    select
        rawdata.moduleserialnumber,
        count(*) as num_evals,
        min(rawdata.adcvalue) as min_adcvalue
    from (
        select
            vpd.moduleserialnumber,
            vpd.adcvalue
        from
            dx.dx_205_vacuumpressuredata vpd 
        where  
            date_parse('<START_DATE>', 
                       '%m/%d/%Y %T') <= vpd.datetimestamplocal
        and 
            vpd.datetimestamplocal < date_parse('<END_DATE>',
                                                '%m/%d/%Y %T') 
        and 
            vpd.vacuumstatename = '<VACUUMPUMP_STATENAME>'
        and 
            vpd.verifyvacuumsubstatename = 'DisableVacuum'
        ) rawdata
    group by
        rawdata.moduleserialnumber
    ) evals
where not (
    evals.min_adcvalue > <VACUUMPUMP_MINADC>
and 
    evals.num_evals >= <VACUUMPUMP_NUMEVALS>
)
order by
    evals.moduleserialnumber"
    #
    flagged <- "N"
    #
    results <- lapply(param_sets, 
                      exec_query, 
                      db_conn, 
                      query_template,
                      config,
                      options,
                      flagged)
    #
    return(results)
}
#
write_results <- function(options,
                          flagged_records,
                          not_flagged_records)
{
    append <- FALSE
    col.names <- TRUE
    #
    for (record in rbind(flagged_records, not_flagged_records)) {
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
#
generate_data <- function(param_sets, config, options)
{
    #
    # open database connection
    #
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
    #
    # execute flagged data query on all the parameter sets
    #
    flagged_records <- exec_flagged_query(param_sets, 
                                          db_conn, 
                                          config, 
                                          options)
    #
    # execute not-flagged data query on all the parameter sets
    #
    not_flagged_records <- exec_not_flagged_query(param_sets, 
                                                  db_conn, 
                                                  config, 
                                                  options)
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
#####################################################################
#
# parse command line arguments
#
specs <- matrix(c('help', 'h', 0, 'logical',
                  'chart', 'C', 0, 'logical',
                  'work', 'w', 1, 'character',
                  'config', 'c', 1, 'character',
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
# set default values
#
if (nchar(Sys.getenv("PHM_ROOT")) == 0) {
    Sys.setenv(PHM_ROOT = path.expand("~"))
}
if (is.null(options$output)) {
    options$output <- "results.csv"
}
if (is.null(options$chart)) {
    options$chart <- FALSE
}
if (is.null(options$params)) {
    options$params <- "parameters.csv"
}
if (is.null(options$config)) {
    options$config <- file.path(Sys.getenv("PHM_ROOT"),
                                "config",
                                "dx",
                                "config.csv")
}
#
# change to working directory
#
if (is.null(options$work)) {
    message("Working directory was not given. Default to current directory.")
} else {
    setwd(options$work)
}
#
# read in config file
#
config <- read_csv_file(filename=options$config, 
                        type_of_file="Configuration")
#
# pretty-up the name-value records by assigning the name as the
# row name.
#
rownames(config) <- config[,1]
#
# read in parameters file 
#
params <- read_csv_file(filename=options$params, 
                        type_of_file="Parameters")
#
# duplicate the patterns column so we have access to the value
# in the lapply
#
# params <- params %>% mutate(PHM_PATTERNS_SK_DUP=PHM_PATTERNS_SK)
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
    generate_data(param_sets, config, options)
})
#
print(process_time)
#
q(status=0)

