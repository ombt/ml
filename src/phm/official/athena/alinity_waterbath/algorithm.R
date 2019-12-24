#
# Alinity c Waterbath Interference
#
#####################################################################
library(tidyr)
library(lubridate)
#
# algorithm specific
#
flagged_query_template <- "
  select 
    trim(upper(moduleserialnumber)) as MODULESN,
    testcompletiondatelocal,
    length(primarywavelengthreads) - length(regexp_replace(primarywavelengthreads, '[^0-9]', '')) as numspaces,
    primarywavelengthreads
  from
    dx.dx_210_alinity_c_result
  where 
   aimcode = 1037 and
   length(primarywavelengthreads) - length(regexp_replace(primarywavelengthreads, '[^0-9]', '')) > 34 and
   '<START_DATE>' <= transaction_date and 
   transaction_date < '<END_DATE>'"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.moduleserialnumber))) as modulesn
from
    dx.dx_210_alinity_c_result dxr
where
    dxr.moduleserialnumber is not null
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
#
chart_data_query_template <- NA
#
# number of days to check
#
number_of_days <- 2
#
# product line code for output file
#
product_line_code <- "210"
#
# configuration type, athena or spark
#
config_type <- "dx"
#
# pre flagged processing
#
pre_flagged_processing <- function(flagged_results, db_conn, params, test_period){
  # input is the query results from flagged_query_template
  # moving average function
  ma <- function(x, n = 5, sides=2){stats::filter(x, rep(1 / n, n), sides = sides)}
  
  # check for spikes in these read intervals:
  check_these <- list(interval1 = list(start = 5, end = 15), 
                      interval2 = list(start = 23, end = 35))
  
  df <- flagged_results %>%
    separate(primarywavelengthreads, 
             into = c("Pblank", paste0("P", sprintf("%02d", 1:38))), 
             sep = "[^0-9]", 
             extra = "merge", 
             fill = "right") %>%
    mutate_at(vars(starts_with("P")), as.numeric) %>%
    filter(!is.na(P36))  %>%
    mutate(testcompletiondatelocal_ts = ymd_hms(testcompletiondatelocal))
  
  df$spike <- apply(df, 1, function(x){
    tmp_ma <- as.numeric(ma(as.numeric(x[(which(names(df) == "P01") : 
                                            which(names(df) == "P37"))])))
    tmp_ma[1 : (check_these$interval1$start - 1)] <- NA
    tmp_ma[(check_these$interval1$end + 1) : (check_these$interval2$start - 1)] <- NA
    tmp_ma[(check_these$interval2$end + 1) : length(tmp_ma)] <- NA
    any((abs(as.numeric(x[which(names(df) == "P01") : which(names(x) == "P37")]) - 
               tmp_ma) / tmp_ma) > .3, na.rm=TRUE)  #should be .3, changed to .001 for test
    #tmp_ma) / tmp_ma) > .001, na.rm=TRUE)  #should be .3, changed to .001 for test
  })
  
  final <- df %>% 
    group_by(MODULESN) %>%
    summarize(nspikes = sum(spike),
              min_spike_date = ifelse(nspikes > 0, 
                                      min(testcompletiondatelocal[spike]), ""),
              max_spike_date = ifelse(nspikes > 0, 
                                      max(testcompletiondatelocal[spike]), "")) %>%
    mutate(spike24diff = replace_na(difftime(ymd_hms(max_spike_date), 
                                             ymd_hms(min_spike_date), 
                                             units="hours"), 0),
           FLAG_YN = spike24diff > 24,
           flag_date = format(ymd_hms(max_spike_date), "%Y%m%d%H%M%S")) %>%
    filter(FLAG_YN) %>% 
    select(MODULESN, flag_date)
  
  # output should be df with columns modulesn, flag_date as "%Y%m%d%H%i%s", other flag criterial optional
  return(final)
}
