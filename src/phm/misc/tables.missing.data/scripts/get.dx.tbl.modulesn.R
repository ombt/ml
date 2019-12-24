# 
sink(file="dx.210.tbls.modsn.cnt")
#
library(mondate)
#
db <- connect_to_athena()
#
ad <- read.csv(file="all.210.dates.csv")
#
for (irow in 1:nrow(ad)) {
    tbl <- ad[irow,"table"]
    max_day <- ad[irow,"maxdate"]
    min_day <- ad[irow,"mindate"]
    #
    print(sprintf("Table: %s, Max: %s, Min: %s", tbl, maxd, mind))
    #
    max_date <- as.POSIXlt(max_day)
    max_year <- 1900+max_date$year
    max_month <- max_date$mon + 1
    max_total <- max_year*12 + max_month
    #
    min_date <- as.POSIXlt(min_day)
    min_year <- 1900+min_date$year
    min_month <- min_date$mon + 1
    min_total <- min_year*12 + min_month
    #
    for (year in min_year:max_year) {
        for (month in 1:12) {
            total <- 12*year+month
            if ((min_total <= total) &&
                (total < max_total)) {
                start_year <- year
                start_month <- month
                #
                end_year <- start_year
                end_month <- start_month + 1
                if (end_month > 12) {
                    end_month <- 1
                    end_year <- end_year + 1
                }
                #
                start_date <- sprintf("date_parse('%02d/01/%04d 00:00:00', '%%m/%%d/%%Y %%T')", start_month, start_year)
                end_date <- sprintf("date_parse('%02d/01/%04d 00:00:00', '%%m/%%d/%%Y %%T')", end_month, end_year)
                #
                query <- sprintf("select count(distinct moduleserialnumber) as count_mmodule_sn from %s where %s <= datetimestamplocal and datetimestamplocal < %s", tbl, start_date, end_date)
                print(query)
                print(athena_exec_query(db, query))
            }
        }
    }
}
#
sink()
#
athena_close_db(db)
#
