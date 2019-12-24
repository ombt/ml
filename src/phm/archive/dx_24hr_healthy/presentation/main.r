# Alinity IA - Dark Counts PHN
# 06/13/2018
# case 1
args = commandArgs(TRUE)
args = as.character(args)

# Be sure to set the working directory to location where associated files
# and kept and results will be written, unless otherwise specified
setwd(args[1])
.libPaths(paste0(args[1], '/library'))
input = read.csv('input.csv',stringsAsFactors = FALSE)

library(dplyr)
library(RODBC)
library(tidyr)

# ODBC setup is controlled by environmental variables specified in Rprofile.site. These are:
# JDBC_CLASSPATH
# PABBTO_UID
# PABBTO_PWD
# username = Sys.getenv('PABBTO_UID')
# pass_word = Sys.getenv('PABBTO_PWD')
#jdbc_classpath = Sys.getenv('JDBC_CLASSPATH')
#jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath=jdbc_classpath)

# open database connection
#conn <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@//ux00147p-scan.oneabbott.com:1521/pabbto",username,pass_word)



#get all instruments available meeting flagging criteria

#conn.pabbto <- odbcConnect("pabbto",uid=username,pwd=pass_word)
conn.dabbto <- odbcConnect("dabbto",uid=username,pwd=password)

dat <- sqlQuery(conn.dabbto, paste0("
  SELECT
    UPPER(evals.MODULESN) AS SN,
    --evals.FLAG_DATE,
    0 AS DEVICE_VALUE,
    CASE WHEN evals.MAX_IDC >= ",as.numeric(input$PARAMETER_VALUE[which(input$PARAMETER_NAME == 'MAX_IDC')]),"
              AND evals.SD_IDC >= ",as.numeric(input$PARAMETER_VALUE[which(input$PARAMETER_NAME == 'SD_IDC')]),"
              AND evals.NUM_TESTID >= ",as.numeric(input$PARAMETER_VALUE[which(input$PARAMETER_NAME == 'NUM_TESTID')]),"
        THEN 1
        ELSE 0
    END AS FLAG_YN,
    NULL as IHN_LEVEL3_DESC
  FROM
    (SELECT
      R.MODULESN,
      TRUNC(R.LOGDATE_LOCAL) AS FLAG_DATE,
      MAX(R.INTEGRATEDDARKCOUNT) AS MAX_IDC,
      STDDEV(R.INTEGRATEDDARKCOUNT) AS SD_IDC,
      COUNT(DISTINCT(R.TESTID)) AS NUM_TESTID
    FROM
      --IDAQOWNER.ICQ_RESULTS R
      SVC_PHM_ODS.PHM_ODS_ICQ_RESULTS R
    WHERE
      R.LOGDATE_LOCAL >= TRUNC(SYSDATE) - 1
      AND R.LOGDATE_LOCAL < TRUNC(SYSDATE)
      AND R.INTEGRATEDDARKCOUNT IS NOT NULL
    GROUP BY
      R.MODULESN,
      TRUNC(R.LOGDATE_LOCAL)
    ORDER BY
      R.MODULESN,
      TRUNC(R.LOGDATE_LOCAL)
    ) evals
"),believeNRows = FALSE,stringsAsFactors = FALSE) # THIS IS THE NEW QUERY FORMATTED PER REQUIREMENTS

#Add IHN_LEVEL3_DESC
#out$IHN_LEVEL3_DESC = input$PARAMETER_VALUE[which(input$PARAMETER_NAME == 'IHN_LEVEL3_DESC')]

# close database connection
close(conn.dabbto)


# get the census for current batch
conn.ods = odbcConnect("dabbto",uid=username,pwd=password)
census <- sqlQuery(conn.ods,paste0("
  SELECT
    CC.DEVICEID,
    UPPER(CC.MODULESN) SN,
    MAX (IL.PL) PL,
    MAX (IL.CUSTOMER_NUM) CUSTOMER_NUMBER,
    MAX (IL.CUSTOMER) CUSTOMER_NAME,
    MAX (PC.COUNTRY) COUNTRY_NAME,
    MAX (PC.AREAREGION) AREA,
    MAX (IL.CITY) CITY,
    CC.RUN_DATE,
    --MAX (COMPLETIONDATE) MAX_COMPLETION_DATE,
    COUNT (*) DEVICE_SN_CNT
  FROM
    --SVC_PHM_ODS.PHM_ODS_RESULTS_CC CC,
    SVC_PHM_ODS.PHM_ODS_ICQ_RESULTS CC,
    INSTRUMENTLISTING IL,
    PHM_COUNTRY PC
  WHERE
    CC.BATCH_NUM = '",input$PARAMETER_VALUE[which(input$PARAMETER_NAME == 'BATCH_NUM')],"' AND
    CC.RUN_DATE = TO_DATE('",input$PARAMETER_VALUE[which(input$PARAMETER_NAME == 'RUN_DATE')],"','MM/DD/YYYY') AND
    UPPER (CC.MODULESN) = UPPER (IL.SN) AND
    PC.COUNTRY_CODE = IL.COUNTRY_CODE and IL.INST_STATUS='Active'
    GROUP BY CC.DEVICEID, CC.MODULESN, CC.RUN_DATE;
    "),believeNRows = FALSE,stringsAsFactors = FALSE)

close(conn.ods)


#structure output
out = census %>%
  rename(FLAG_DATE = RUN_DATE) %>%
  select(SN,FLAG_DATE) %>%
  left_join(dat,by='SN') %>%
  replace_na(list(DEVICE_VALUE = 0,FLAG_YN = 0,IHN_LEVEL3_DESC = "")) %>%
  mutate(DEVICE_VALUE = ifelse(FLAG_YN == 1,1,0)) #%>%
  #mutate(IHN_LEVEL3_DESC = ifelse((FLAG_YN == 1 & (grepl("AI", SN) == TRUE)), "Wash Monitoring P1", "Aspiration P3"))


# write output to csv file
write.csv(out, file='results.csv', row.names=FALSE)
