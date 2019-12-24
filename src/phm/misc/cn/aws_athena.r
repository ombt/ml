connectAthena <- function(abbott_511, 
                          athenaJarPath,
                          awsCredsPath=file.path(Sys.getenv("userprofile"), ".aws\\credentials")){
  if(!file.exists(athenaJarPath)){ stop("Cannot find Athena Jar file") }
  if(!file.exists(awsCredsPath)){ stop("Cannot find AWS credentials file") }
  
  drv <- JDBC(driverClass="com.simba.athena.jdbc.Driver",
              classPath = athenaJarPath, identifier.quote="'")
  aws <- ini::read.ini(awsCredsPath)
  Sys.setenv(AWS_ACCESS_KEY_ID = aws["saml"][[1]]$aws_access_key_id)
  Sys.setenv(AWS_SECRET_ACCESS_KEY = aws["saml"][[1]]$aws_secret_access_key)
  Sys.setenv(AWS_SESSION_TOKEN = aws["saml"][[1]]$aws_session_token)
  
  provider <- "com.amazonaws.athena.jdbc.shaded.com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
  
  con <- dbConnect(drv, "jdbc:awsathena://awsregion=us-east-1",
                   LogLevel = "6",
                   LogPath = "C:\\",
                   workgroup = "add_service_dx_readonly",
                   UseResultSetStreaming = "0",
                   S3OutputLocation = paste0("s3://abt-bdaa-test-us-east-1-sandbox/athena/", abbott_511),
                   AwsCredentialsProviderClass = "com.simba.athena.amazonaws.auth.profile.ProfileCredentialsProvider",
                   AwsCredentialsProviderArguments = "saml",
                   s3_staging_dir="s3://abt-bdaa-test-us-east-1-sandbox/athenaquerylog/",
                   aws_credentials_provider_class=provider)
  return(con)
}

library(RJDBC)  
library(ini)
conn <- connectAthena("nissecx", "c://oracle//athenajdbc41_2.0.7.jar")
df <- dbGetQuery(conn, "select * from dx.dx_205_result limit 10")
dbDisconnect(conn)
