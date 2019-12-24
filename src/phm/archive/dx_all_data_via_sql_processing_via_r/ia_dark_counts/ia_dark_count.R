#
# Alinity IA Optics Dark Count
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
# for debugging ...
#
# options(browserNLdisabled=TRUE)
# options(error=recover)
# browser()
#
# dump_and_quit <- function() {
#     dump.frames(to.file=TRUE)
#     q(status=1)
# }
#
# options(error=dump_and_quit)
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
exec_query <- function(params, 
                       db_conn, 
                       query_template, 
                       config, 
                       options)
{
    #
    # remove new lines and returns since the query may fail.
    #
    query_template <- gsub("[\n\r]"," ", query_template)
    #
    # easy to access parameters if we assign row names
    #
    rownames(params) <- params[,"PARAMETER_NAME"]
    #
    # substitute values into query
    #
    query <- sprintf(query_template, 
                     config["START_DATE",
                            "VALUE"],
                     config["END_DATE",
                            "VALUE"],
                     params["THRESHOLDS_COUNT", 
                            "PARAMETER_VALUE"])
    #
    query_time <- system.time({
        results <- dbGetQuery(db_conn, query)
    })
    print(query_time)
    #
    # check if anything was found
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
                      "THRESHOLDS_DESCRIPTION")
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
    results$THRESHOLD_DESCRIPTION <- params["THRESHOLD DESCRIPTION",
                                            "PARAMETER_VALUE"]
    #
    # get algorithm parameters
    #
    testid <- 
        as.numeric(params["TESTID", "PARAMETER_VALUE"])
    idc_max <- 
        as.numeric(params["INTEGRATEDDARKCOUNT_MAX", "PARAMETER_VALUE"])
    idc_sd <- 
        as.numeric(params["INTEGRATEDDARKCOUNT_SD", "PARAMETER_VALUE"])
    #
    # get flagged records
    #
    flagged_results <- 
        with(results, results[num_testid>=testid &
                              max_idc>=idc_max &
                              sd_idc>=idc_sd,])
    #
    if (nrow(flagged_results) > 0) {
        flagged_results$FLAG_YN <- "Y"
        flagged_results$CHART_DATA_VALUE <- 1
        flagged_results$CHART_DATA_VALUE_TYPE <- "FLAGGED"
        #
        flagged_results_num_tested <- flagged_results
        flagged_results_num_tested$CHART_DATA_VALUE <- flagged_results$num_testid
        flagged_results_num_tested$CHART_DATA_VALUE_TYPE <- "NUM_TESTID"
        #
        flagged_results_max_idc <- flagged_results
        flagged_results_max_idc$CHART_DATA_VALUE <- flagged_results$max_idc
        flagged_results_max_idc$CHART_DATA_VALUE_TYPE <- "MAX_IDC"
        #
        flagged_results_sd_idc <- flagged_results
        flagged_results_sd_idc$CHART_DATA_VALUE <- flagged_results$sd_idc
        flagged_results_sd_idc$CHART_DATA_VALUE_TYPE <- "SD_IDC"
        #
        flagged_results <- rbind(flagged_results,
                                 flagged_results_num_tested,
                                 flagged_results_max_idc,
                                 flagged_results_sd_idc)
    }
    #
    # get not flagged records
    #
    not_flagged_results <- 
        with(results, results[ ! (num_testid>=testid &
                                  max_idc>=idc_max &
                                  sd_idc>=idc_sd),])
    if (nrow(not_flagged_results) > 0) {
        not_flagged_results$FLAG_YN <- "N"
        not_flagged_results$CHART_DATA_VALUE <- 0
        not_flagged_results$CHART_DATA_VALUE_TYPE <- "FLAGGED"
        #
        if (options$chart) {
            not_flagged_results_num_tested <- not_flagged_results
            not_flagged_results_num_tested$CHART_DATA_VALUE <- not_flagged_results$num_testid
            not_flagged_results_num_tested$CHART_DATA_VALUE_TYPE <- "NUM_TESTID"
            #
            not_flagged_results_max_idc <- not_flagged_results
            not_flagged_results_max_idc$CHART_DATA_VALUE <- not_flagged_results$max_idc
            not_flagged_results_max_idc$CHART_DATA_VALUE_TYPE <- "MAX_IDC"
            #
            not_flagged_results_sd_idc <- not_flagged_results
            not_flagged_results_sd_idc$CHART_DATA_VALUE <- not_flagged_results$sd_idc
            not_flagged_results_sd_idc$CHART_DATA_VALUE_TYPE <- "SD_IDC"
            #
            not_flagged_results <- rbind(not_flagged_results,
                                         not_flagged_results_num_tested,
                                         not_flagged_results_max_idc,
                                         not_flagged_results_sd_idc)
        }
    }
    #
    return(rbind(flagged_results, not_flagged_results))
}
#
run_algorithm <- function(param_sets, db_conn, config, options)
{
    #
    # build query
    #
    query_template <- "
select
    dxr.moduleserialnumber as moduleserialnumber,
    count(dxr.testid) as num_testid,
    max(dxr.integrateddarkcount) as max_idc,
    stddev(dxr.integrateddarkcount) as sd_idc
from
    dx.dx_205_result dxr
where
    date_parse('%s', '%%m/%%d/%%Y %%T') <= dxr.datetimestamplocal
and 
    dxr.datetimestamplocal < date_parse('%s', '%%m/%%d/%%Y %%T') 
and
    dxr.integrateddarkcount is not null
and
    dxr.integrateddarkcount >= %s
and
    upper(dxr.moduleserialnumber) like 'AI%%'
group by
    dxr.moduleserialnumber
order by
    dxr.moduleserialnumber"
    #
    results <- lapply(param_sets, 
                      exec_query, 
                      db_conn, 
                      query_template,
                      config,
                      options)
    #
    return(results)
}
#
write_results <- function(options,
                          records)
{
    append <- FALSE
    col.names <- TRUE
    #
    for (record in records) {
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
    records <- run_algorithm(param_sets, 
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
    write_results(options, records)
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
    options$config <- file.path(path.expand("~"),
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
    # debug(generate_data)
    generate_data(param_sets, config, options)
})
#
print(process_time)
#
q(status=0)

