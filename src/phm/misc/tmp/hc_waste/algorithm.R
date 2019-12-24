#
# Alinity c HC Waste
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
number_of_days <- 60
#
# product line code for output file
#
product_line_code <- "210"
#
# configuration type, athena or spark
#
config_type <- "dx"  #ida
#
# pre flagged processing
#
pre_flagged_processing <- function(flagged_results, db_conn, params, test_period){
  # input is the query results from flagged_query_template
  df <- flagged_results %>%
    separate(primarywavelengthreads, 
             into = c("Pblank", paste0("P", sprintf("%02d", 1:38))), 
             sep = "[^0-9]", 
             extra = "merge", 
             fill = "right") %>%
    mutate_at(vars(starts_with("P")), as.numeric) %>%
    filter(!is.na(P36))  %>%
    mutate(testcompletiondatelocal_ts = ymd_hms(testcompletiondatelocal))
  
  # compute slope for reads 22 to 32
  df$ref_slope <- apply(df, 1,
                        function(x)
                          (lm(response ~ read,
                              data = data.frame(
                                response = as.numeric(x[which(names(x) == "P22") : which(names(x) == "P32")]),
                                read = as.numeric(gsub("[^0-9]", "",
                                                       names(x)[which(names(x) == "P22") : which(names(x) == "P32")])))
                          )$coefficients[2] / as.numeric(x[which(names(x) == "P32")]) *100)
  )
  
  df$sl35 <- ifelse(!is.na(df$P35), ((df$P35-df$P34) / df$P35) * 100, NA)
  df$sl36 <- ifelse(!is.na(df$P36), ((df$P36-df$P35) / df$P36) * 100, NA)
  df$sl37 <- ifelse(!is.na(df$P37), ((df$P37-df$P36) / df$P37) * 100, NA)
  
  df$readinc <- pmax(df$sl35, df$sl36, df$sl37, na.rm=TRUE)
  df$slopediff <- df$readinc - df$ref_slope
  df$flag <- df$slopediff >= 5 & df$readinc > 0
  #df$flag <- df$slopediff >= .5 & df$readinc > 0 # to generate flags for apollo verification
  df$flag[is.na(df$flag)] <- FALSE
  
  final <- df %>% 
    group_by(MODULESN) %>%
    summarise(n=n(),
              flag_prop = sum(flag) / n(),
              flag_date = format(max(testcompletiondatelocal_ts), 
                                 "%Y%m%d%H%M%S")) %>%
    mutate(FLAG_YN = n >= 10 & flag_prop >= .5) %>%
    filter(FLAG_YN) %>% 
    select(MODULESN, flag_date)
  
  
  # output should be df with columns modulesn, flag_date as "%Y%m%d%H%i%s", other flag criterial optional
  return(final)
}
