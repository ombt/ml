dx.210.tbls <- c(
    "dx_210_assayactivity",
    "dx_210_assayactivitycc",
    "dx_210_calcurve",
    "dx_210_calibrationresultcompletion",
    "dx_210_ccaspirationpm",
    "dx_210_ccbulksolutionconsumeddata",
    "dx_210_ccbulksolutionmonitordata",
    "dx_210_ccbulksolutionusagestatusdata",
    "dx_210_cccuvettewashwatervolumedata",
    "dx_210_ccdispensepm",
    "dx_210_cclampmonitordata",
    "dx_210_ccopticsadjusttriggersensordata",
    "dx_210_ccphotodata",
    "dx_210_ccpipettorpmrawdata",
    "dx_210_ccreagentaspirationotherdata",
    "dx_210_ccreagentaspirationpcidata",
    "dx_210_ccreagentcarouselcoolertemperaturedata",
    "dx_210_ccreagentcarouselteknicmotordata",
    "dx_210_ccreagentdispenseotherdata",
    "dx_210_ccreagentdispensepcidata",
    "dx_210_ccreagentwashotherdata",
    "dx_210_ccreagentwashpcidata",
    "dx_210_ccsampleaspirationotherdata",
    "dx_210_ccsampleaspirationpcidata",
    "dx_210_ccsampledispenseotherdata",
    "dx_210_ccsampledispensepcidata",
    "dx_210_ccsamplewashotherdata",
    "dx_210_ccsamplewashpcidata",
    "dx_210_ccwashpm",
    "dx_210_ccwaterbathrefilldata",
    "dx_210_constituentdata",
    "dx_210_experimentprocessing",
    "dx_210_heaterdutycycledata",
    "dx_210_iarm",
    "dx_210_icbmotormove",
    "dx_210_instrumentactivity",
    "dx_210_itvdata",
    "dx_210_lld",
    "dx_210_lldcc",
    "dx_210_messagehistory",
    "dx_210_mndhistory",
    "dx_210_mndrecorddata",
    "dx_210_navigateservicedata",
    "dx_210_opticsrawdata",
    "dx_210_oshealthmonitoringdata",
    "dx_210_pmevent",
    "dx_210_pmrawdata",
    "dx_210_reagentcoolerdata",
    "dx_210_reagentoperation",
    "dx_210_scripting",
    "dx_210_teknicmotor",
    "dx_210_temperaturedata",
    "dx_210_vacuumpressuredata",
    "dx_210_wambulkdata",
    "dx_210_wamdata",
    "dx_205_messagehistory",
    "dx_205_pmevent",
    "dx_205_result",
    "dx_210_result"
)
#
db <- connect_to_athena()
#
sink(file="dx.210.tbls.modsn.cnt")
#
# for (dx.tbl in dx.tbls) {
#
# for (itbl in c(1:3)) {
    # dx.tbl <- dx.tbls[itbl]
for (dx.tbl in dx.210.tbls) {
    print(sprintf("DX TBL ... %s", dx.tbl))
    #
    query <- sprintf("select max(datetimestamplocal) as max_day, min(datetimestamplocal) as min_day from dx.%s", dx.tbl)
    #
    print(query)
    min.max.days <- athena_exec_query(db, query)
    print(min.max.days)
    #
    max_date <- as.POSIXlt(min.max.days$max_day)
    max_year <- 1900+max_deate$year
    max_month <- max_date$mon + 1
    max_total <- max_year*12 + max_month
    #
    min_date <- as.POSIXlt(min.max.days$min_day)
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
                query <- sprintf("select count(distinct moduleserialnumber) as count_mmodule_sn from dx.%s where %s <= datetimestamplocal and datetimestamplocal < %s", dx.tbl, start_date, end_date)
                print(query)
                print(athena_exec_query(db, query))
            }
        }
    }
}
#
#
sink()
#
athena_close_db(db)
#

