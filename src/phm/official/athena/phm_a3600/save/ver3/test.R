#
# Head Down Failure DCM 1006,186,MODULE,DCM
# Head Down Failure DCM 1006,186,ERROR_CODE_VALUE,1006
# Head Down Failure DCM 1006,186,IHN_LEVEL3_DESC,Head Down Failure DCM 1006
# Head Down Failure DCM 1006,186,ALGORITHM_TYPE,ERROR_COUNT
# Head Down Failure DCM 1006,186,ERROR_COUNT,4
# Head Down Failure DCM 1006,186,THRESHOLD_DESCRIPTION,4/day for 2 consecutive days
# Head Down Failure DCM 1006,186,THRESHOLDS_DAYS,2
# Head Down Failure DCM 1006,186,THRESHOLD_DATA_DAYS,2
#
phm_patterns_sk <- 186
#
module <- "DCM"
error_code_value <- "1006"
ihn_level3_desc <- "Head Down Failure DCM 1006"
algorithm_type <- "ERROR_COUNT"
error_count <- 4
threshold_description <- "4/day for 2 consecutive days"
thresholds_days <- 2
threshold_data_days <- 2
#
module_type           <- module
pattern_description   <- error_code_value
threshold_alert       <- ihn_level3_desc
threshold_number      <- error_count
threshold_number_desc <- threshold_description
threshold_number_unit <- thresholds_days
threshold_data_days   <- threshold_data_days
#
data_results <- read.csv("debug_error_count_data.csv")
#
process_error_count_data <- function(data_results,
                                     phm_patterns_sk,
                                     threshold_alert,
                                     threshold_number,
                                     threshold_number_desc,
                                     threshold_number_unit,
                                     threshold_data_days)
{
    curr_day          <- NA
    prev_day          <- NA
    curr_day_tstamp   <- NA
    prev_day_tstamp   <- NA
    consecutive_days  <- TRUE
    curr_day_errcount <- 0
    flagging_days     <- 0
    flag              <- 'no'
    ihn_value         <- NA
    v_insert_count    <- 0
    total_error_count <- 0
    #
    current_sn = " "
    flagged_results <- empty_results()
    #
    for (idr in 1:nrow(data_results)) {
        dr_record <- data_results[idr, ]
        #
        if (nchar(dr_record[1,"A3600_SERIALNUMBER_UC"]) <= 0) {
            next
        }
        if (current_sn != dr_record[1,"A3600_SERIALNUMBER_UC"]) {
            print(sprintf("INFO: Processing SN: %s", dr_record[1,"A3600_SERIALNUMBER_UC"]))
            curr_day          <- NA
            prev_day          <- NA
            curr_day_tstamp   <- NA
            prev_day_tstamp   <- NA
            consecutive_days  <- TRUE
            curr_day_errcount <- 0
            flagging_days     <- 0
            flag              <- 'no'
            ihn_value         <- NA
            v_insert_count    <- 0
            total_error_count <- 0
        } else {
            print(sprintf("INFO: CONTINUE Processing SN: %s", dr_record[1,"A3600_SERIALNUMBER_UC"]))
        }
        current_sn = dr_record[1,"A3600_SERIALNUMBER_UC"]
        current_pl = dr_record[1,"A3600_PRODUCTLINE"]
        #
        flag               <- 'no'
        ihn_value          <- NA
        v_flagged_pl       <- NA
        v_flagged_exp_code <- NA
        #
        FLAG_DATE <- dr_record[1,"FLAG_DATE"]
        FLAG_DATE <- gsub("\\.[0-9][0-9]*","", FLAG_DATE)
        FLAG_DATE <- gsub("[^0-9]","", FLAG_DATE)
        #
        curr_day <- dr_record[1,"FLAG_DATE"]
        # curr_day_tstamp <- as.POSIXct(curr_day, format="%Y-%m-%d %H:%M:%OS")
        curr_day_tstamp <- as.POSIXct(curr_day, format="%Y-%m-%d")
        curr_day_errcount <- dr_record[1,"ERROR_COUNT"]
        #
        if (( ! is.na(prev_day)) && (curr_day_tstamp != (prev_day_tstamp + 24*60*60))) {
            consecutive_days <- FALSE
        }
        #
        if (consecutive_days && (curr_day_errcount >= threshold_number)) {
            flagging_days <- flagging_days + 1
            prev_day <- curr_day
            prev_day_tstamp <- curr_day_tstamp
        }
        #
        if (curr_day_errcount < threshold_number) {
            flagging_days <- 0
            prev_day <- NA
            prev_day_tstamp <- NA
            consecutive_days <- TRUE
        }
        #
print(sprintf("(sn,flagging_days,threshold_number_unit,curr_day_errcount,threshold_number)=(%s,%d,%d,%d,%d)",
              current_sn,flagging_days,threshold_number_unit,curr_day_errcount,threshold_number))
        #
        if ((flagging_days >= threshold_number_unit) &&
            (curr_day_errcount >= threshold_number)) {
            flag <- 'yes'
            ihn_value <- threshold_alert
            #
            flagged_record <- list(PHN_PATTERNS_SK=phm_patterns_sk,
                                   PL=current_pl,
                                   MODULESN=current_sn,
                                   FLAG_DATE=FLAG_DATE,
                                   CHART_DATA_VALUE=1,
                                   FLAG_YN=1,
                                   IHN_LEVEL3_DESC=ihn_value)
            flagged_results <- rbind(flagged_results,
                                     flagged_record,
                                     stringsAsFactors=FALSE)
            #
            print(sprintf("INFO: SN: %s FLAGGED", dr_record[1,"A3600_SERIALNUMBER_UC"]))
        }
    }
    #
    return(flagged_results)
} 
#
print(process_error_count_data(data_results,
                               phm_patterns_sk,
                               threshold_alert,
                               threshold_number,
                               threshold_number_desc,
                               threshold_number_unit,
                               threshold_data_days)[,c("PL","MODULESN")])

