
# this depends on the windows AWS CLI being installed

library("jsonlite")

getSQL <- function(filepath){
  con = file(filepath, "r")
  sql.string <- ""
  
  while (TRUE){
    # suppress the 'incomplete final line' warning (mostly)
    line <- suppressWarnings(readLines(con, n = 1))
    
    if ( length(line) == 0 ){
      break
    }
    line <- gsub("\\t", " ", line)
    if(grepl("--",line) == TRUE){
      line <- paste(sub("--","/*",line),"*/")
    }
    sql.string <- paste(sql.string, line)
  }
  close(con)
  return(sql.string)
}

readTempCreds <- function(path=file.path(Sys.getenv("userprofile"), ".aws\\credentials")){
  if(file.exists(path)){
    creds <- readLines(path)
    # get just the section with [saml]
    saml_start <- which(grepl("\\[saml\\]", creds))
    saml_end <- which(grepl("\\[*\\]", creds))[!which(grepl("\\[*\\]", creds)) %in% saml_start]
    creds <- creds[saml_start : min(saml_end, length(creds))]
    
    tmp <- strsplit(creds, " = ")
    creds_df <- data.frame("attr" = sapply(tmp, "[", 1), 
                           "val" = sapply(tmp, "[", 2),
                           stringsAsFactors=FALSE)
    creds_df <- creds_df[!is.na(creds_df$attr), ]
    
    if("aws_access_key_id" %in% creds_df$attr &
       "aws_secret_access_key" %in% creds_df$attr){
      athena_username <- creds_df$val[creds_df$attr=="aws_access_key_id"]
      athena_password <- creds_df$val[creds_df$attr=="aws_secret_access_key"]
      session_token <- creds_df$val[creds_df$attr=="aws_session_token"]
      return(list(athena_username = athena_username,
                  athena_password = athena_password,
                  session_token = session_token))
    }else{
      stop("aws_access_key_id or aws_secret_access_key not found in credentials file")
    }
  }else{
    stop(paste0("No credentials file exists in at ", path))
  }
}

get_query_id <- function(sqltext="", sqlfile=""){
  if(sqltext=="" & sqlfile=="") stop("error: need sqltext or sqlfile")
  
  if(sqlfile != ""){
    if(sqltext!="" & sqlfile!="") cat("Both sqlfile and sqltext given, using sqlfile")
    if(file.exists(sqlfile)){
      sqltext = getSQL(sqlfile)
    }else{
      stop("error: sqlfile does not exist")
    }
  }
  # get aws creds from aws file.
  creds <- readTempCreds()
  Sys.setenv(AWS_DEFAULT_REGION="us-east-1")
  Sys.setenv(AWS_ACCESS_KEY_ID=creds$athena_username)
  Sys.setenv(AWS_SECRET_ACCESS_KEY=creds$athena_password)
  Sys.setenv(AWS_SESSION_TOKEN=creds$session_token)
  query_id_json <- system(paste0("aws athena start-query-execution --query-string \"", sqltext, 
                                 "\" --result-configuration \"OutputLocation=s3://abt-bdaa-test-us-east-1-sandbox/athenaquerylog/\" --work-group \"add_service_dx_readonly\" "),
                          intern=TRUE)
  query_id <- fromJSON(paste(query_id_json, collapse=""))$QueryExecutionId
  return(query_id)
}

get_query_stats <- function(query_id="", silent=FALSE){
  # query_ids are pretty long, make sure there's at least something there...
  if(nchar(query_id) < 20) stop("error: Invalid query id; id too short")
  creds <- readTempCreds()
  Sys.setenv(AWS_DEFAULT_REGION="us-east-1")
  Sys.setenv(AWS_ACCESS_KEY_ID=creds$athena_username)
  Sys.setenv(AWS_SECRET_ACCESS_KEY=creds$athena_password)
  Sys.setenv(AWS_SESSION_TOKEN=creds$session_token)
  
  stats_json <- system(paste0("aws athena get-query-execution --query-execution-id ", 
                              query_id),
                       intern=TRUE)
  stats_parsed <- fromJSON(stats_json)
  if(stats_parsed$QueryExecution$Status$State == "RUNNING"){
    stop("error: Query still running...")
  }else if(stats_parsed$QueryExecution$Status$State == "SUCCEEDED"){
    stats_bytes <- fromJSON(stats_json)$QueryExecution$Statistics$DataScannedInBytes
    stats_tb <- as.numeric(stats_bytes) / 1000000000000
    cost <- stats_tb * 5
    if(!silent){ 
      print(paste0("Cost for ", round(stats_tb, 6), 
                   "TB scanned is $", round(cost, 6)))
    }
    return(list(tb_scanned=stats_tb, cost=cost))
  }else if(!stats_parsed$QueryExecution$Status$State %in% c("RUNNING", "SUCCEEDED")){
    stop(paste0("error: Unable to handle query status. Status: ", 
                stats_parsed$QueryExecution$Status$State))
  }
}

# example reading sql from text
sqltext <- "select * from dx.dx_205_result limit 1000"
query_id <- get_query_id(sqltext)
Sys.sleep(10) # wait for query to execute
get_query_stats(query_id)

# example reading sql from file
query_id <- get_query_id(sqlfile="sample_dx_query.sql")
Sys.sleep(45)
get_query_stats(query_id)
