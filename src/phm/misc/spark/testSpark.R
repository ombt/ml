library(checkpoint)

CHECKPOINT_LOCATION <- Sys.getenv("CHECKPOINT_LOCATION")

checkpoint("2019-07-01",checkpointLocation= CHECKPOINT_LOCATION)

library(sparklyr)

conf <- spark_config()

sc <- spark_connect(master = "yarn-client",config=conf)

#out_csv <- spark_read_parquet(sc,"tab","s3a://abbottlink-parquet/sample-parquet-data/userdata1.parquet")

read_in <- spark_read_parquet(sc,"tab","s3://abt-bdaa-curated-us-east-1-prod/dx/ale/derived/205-alinity-i/Result/transaction_date=2019-07-22")

library(DBI)

out_csv <- dbGetQuery(sc, "SELECT count(*) from tab")

write.csv(out_csv,file='out.csv')

spark_disconnect(sc)
