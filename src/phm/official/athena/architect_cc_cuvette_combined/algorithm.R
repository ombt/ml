#
# Architect CC Cuvette Combined
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
with rawdata as (
    select
        upper(trim(r.architect_moduleserial)) as modulesn,
        r.architect_productline as pl,
        r.cuvettenumber,
        r.completiondatetime_iso,
        p.logfield24 as disreadyave,
        p.logfield25 as disbeginave
    from
        dx.dx_architect_results r
    inner join
        dx.dx_architect_pm p
    on
        p.resultcode = '30'
    and
        '<START_DATE>' <= p.transaction_date
    and 
        p.transaction_date < '<END_DATE>'
    and 
        ((upper(trim(p.architect_moduleserial)) like 'C4%') or
         (upper(trim(p.architect_moduleserial)) like 'C16%'))
    and
        p.replicateid is not null
    and
        upper(trim(r.architect_moduleserial)) = upper(trim(p.architect_moduleserial))
    and
        r.replicateid = p.replicateid
    where
        '<START_DATE>' <= r.transaction_date
    and 
        r.transaction_date < '<END_DATE>'
    and 
        ((upper(trim(r.architect_moduleserial)) like 'C4%') or
         (upper(trim(r.architect_moduleserial)) like 'C16%'))
    and
        r.replicateid is not null
    and
        r.cuvettenumber is not null
    and
        r.architect_productline is not null
    and
        r.architect_productline in ( '127', '128' )
)
select
    middle1.modulesn,
    middle1.pl,
    middle1.cuvettetype,
    'cc_cuvette_lls' as algorithm,
    date_format(max(middle1.flag_date),'%Y%m%d%H%i%s') as flag_date,
    case when ((100*sum(middle1.exceed_percuv_pct_thld)/count(middle1.cuvettenumber)) > <LOGFIELD24_PCT>)
         then 1
         else 0
         end as flagged
from (
    select
        tbl1.modulesn,
        tbl1.pl,
        tbl1.cuvettenumber,
        tbl1.cuvettetype,
        tbl1.flag_date,
        tbl1.sample_count,
        tbl1.exceed_threshold_count,
        case when ((tbl1.sample_count > 20) and
                   (100*(tbl1.exceed_threshold_count/tbl1.sample_count) > <LOGFIELD24_PCT>))
             then
                 1
             else
                 0
             end as exceed_percuv_pct_thld
    from (
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C4' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disreadyave) as sample_count,
            sum(case when (cast (rawdata.disreadyave as integer) > <LOGFIELD24_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C4%'
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C16-ALINE' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disreadyave) as sample_count,
            sum(case when (cast (rawdata.disreadyave as integer) > <LOGFIELD24_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 0
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C16-BLINE' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disreadyave) as sample_count,
            sum(case when (cast (rawdata.disreadyave as integer) > <LOGFIELD24_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 1
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        ) tbl1
    ) middle1
group by
    middle1.modulesn,
    middle1.pl,
    middle1.cuvettetype
union all
select
    c4middle.modulesn as modulesn,
    c4middle.pl,
    c4middle.cuvettetype,
    'cc_cuvette_status_check' as algorithm,
    date_format(max(c4middle.flag_date),'%Y%m%d%H%i%s') as flag_date,
    case when (count(c4middle.exceed_percuv_pct_thld) <= 4)
         then 1
         else 0
         end as flagged
from (
    select
        c4inner.modulesn,
        c4inner.pl,
        c4inner.cuvettenumber,
        c4inner.cuvettetype,
        c4inner.flag_date,
        c4inner.sample_count,
        c4inner.exceed_threshold_count,
        case when ((c4inner.sample_count > 20) and
                   (100*(c4inner.exceed_threshold_count/c4inner.sample_count) > <LOGFIELD25_PCT>))
             then
                 1
             else
                 0
             end as exceed_percuv_pct_thld
    from (
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C4' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > <LOGFIELD25_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C4%'
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        ) c4inner
    ) c4middle
group by
    c4middle.modulesn,
    c4middle.pl,
    c4middle.cuvettetype
union all
select
    c16middle.modulesn as modulesn,
    c16middle.pl,
    c16middle.cuvettetype,
    'cc_cuvette_status_check' as algorithm,
    date_format(max(c16middle.flag_date),'%Y%m%d%H%i%s') as flag_date,
    case when (count(c16middle.exceed_percuv_pct_thld) <= 7)
         then 1
         else 0
         end as flagged
from (
    select
        c16inner.modulesn,
        c16inner.pl,
        c16inner.cuvettenumber,
        c16inner.cuvettetype,
        c16inner.flag_date,
        c16inner.sample_count,
        c16inner.exceed_threshold_count,
        case when ((c16inner.sample_count > 20) and
                   (100*(c16inner.exceed_threshold_count/c16inner.sample_count) > <LOGFIELD25_PCT>))
             then
                 1
             else
                 0
             end as exceed_percuv_pct_thld
    from (
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C16-ALINE' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > <LOGFIELD25_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 0
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C16-BLINE' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > <LOGFIELD25_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 1
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        ) c16inner
    ) c16middle
group by
    c16middle.modulesn,
    c16middle.pl,
    c16middle.cuvettetype
union all
select
    middle2.modulesn as modulesn,
    middle2.pl,
    middle2.cuvettetype,
    'cc_cuvette_wash_subassembly' as algorithm,
    date_format(max(middle2.flag_date),'%Y%m%d%H%i%s') as flag_date,
    case when ((100*sum(middle2.exceed_percuv_pct_thld)/count(middle2.cuvettenumber)) > <LOGFIELD25_PCT>)
         then 1
         else 0
         end as flagged
from (
    select
        tbl1.modulesn,
        tbl1.pl,
        tbl1.cuvettenumber,
        tbl1.cuvettetype,
        tbl1.flag_date,
        tbl1.sample_count,
        tbl1.exceed_threshold_count,
        case when ((tbl1.sample_count > 20) and
                   (100*(tbl1.exceed_threshold_count/tbl1.sample_count) > <LOGFIELD25_PCT>))
             then
                 1
             else
                 0
             end as exceed_percuv_pct_thld
    from (
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C4' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > <LOGFIELD25_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C4%'
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C16-ALINE' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > <LOGFIELD25_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 0
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        union all
        select
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber,
            'C16-BLINE' as cuvettetype,
            max(rawdata.completiondatetime_iso) as flag_date,
            count(rawdata.disbeginave) as sample_count,
            sum(case when (cast (rawdata.disbeginave as integer) > <LOGFIELD25_THRESHOLD>)
                     then 1
                     else 0
                     end) as exceed_threshold_count
        from
            rawdata
        where
            rawdata.modulesn like 'C16%'
        and
            mod(rawdata.cuvettenumber,2) = 1
        group by
            rawdata.modulesn,
            rawdata.pl,
            rawdata.cuvettenumber
        ) tbl1
    ) middle2
group by
    middle2.modulesn,
    middle2.pl,
    middle2.cuvettetype"
#
modulesn_query_template <- "
select
    distinct(upper(trim(dxr.architect_moduleserial))) as modulesn,
    dxr.architect_productline as pl
from
    dx.dx_architect_results dxr
where
    dxr.architect_moduleserial is not null
and
    dxr.architect_productline is not null
and
    dxr.architect_productline in ( '127', '128' )
and
    '<MODULESN_START_DATE>' <= dxr.transaction_date
and 
    dxr.transaction_date < '<MODULESN_END_DATE>'"
#
use_suppression <- FALSE
chart_data_query_template <- NA
#
# number of days to check
#
number_of_days <- 7
#
# product line code for output file
#
product_line_code <- NA
#
# configuration type, athena or spark
#
config_type <- "dx"
#
# post flagged-query R processing
#
post_flagged_processing <- function(results,
                                    db_conn, 
                                    params, 
                                    options, 
                                    test_period) 
{
    #
    names(results) <- toupper(names(results))
    #
    # identify MODSN flagged by CC Cuvette LLS test
    #
    lls_flagged <- 
        results[(results$ALGORITHM == "cc_cuvette_lls") &
                 (results$FLAGGED == 1), ]
    #
    # get list of MODSN already flagged.
    #
    lls_flagged_modulesn <- unique(lls_flagged[,"MODULESN"])
    #
    # remove all data for the LLS flagged MODSN
    #
    results <-
        results[ ! (results$MODULESN %in% lls_flagged_modulesn), ]
    #
    #
    # identify MODSN flagged by CC Cuvette Status Check test
    #
    status_flagged <- 
        results[(results$ALGORITHM == "cc_cuvette_status_check") &
                 (results$FLAGGED == 1), ]
    #
    # get list of MODSN already flagged.
    #
    status_flagged_modulesn <- unique(status_flagged[,"MODULESN"])
    #
    # remove all data for the Status flagged MODSN
    #
    results <-
        results[ ! (results$MODULESN %in% status_flagged_modulesn), ]
    #
    # identify MODSN flagged by CC Cuvette Wash Subassembly test
    #
    wash_flagged <- 
        results[(results$ALGORITHM == "cc_cuvette_wash_subassembly") &
                 (results$FLAGGED == 1), ]
    #
    # get list of MODSN already flagged.
    #
    washed_flagged_modulesn <- unique(wash_flagged[,"MODULESN"])
    #
    # remove all data for the Wash flagged MODSN
    #
    not_flagged <- 
        results[ ! (results$MODULESN %in% washed_flagged_modulesn), ]
    #
    # combined the flagged sets 
    #
    results <- rbind(lls_flagged,
                     status_flagged,
                     wash_flagged)
    #
    # assigned extra fields needed for output.
    #
    param_sets <- split(params, list(params$ALGORITHM_NAME))
    param_sets_names <- tolower(names(param_sets))
    #
    lls_idx    <- grep("lls.*board", param_sets_names)
    status_idx <- grep("status.*check", param_sets_names)
    wash_idx   <- grep("wash.*subassembly", param_sets_names)
    #
    lls_params    <- param_sets[[lls_idx]]
    status_params <- param_sets[[status_idx]]
    wash_params   <- param_sets[[wash_idx]]
    #
    lls_phm_patterns_sk    <- lls_params[1,"PHM_PATTERNS_SK"]
    status_phm_patterns_sk <- status_params[1,"PHM_PATTERNS_SK"]
    wash_phm_patterns_sk   <- wash_params[1,"PHM_PATTERNS_SK"]
    #
    lls_ihn_level3_desc <-
        lls_params[lls_params$PARAMETER_NAME=="IHN_LEVEL3_DESC",
                   "PARAMETER_VALUE"]
    status_ihn_level3_desc <-
        status_params[lls_params$PARAMETER_NAME=="IHN_LEVEL3_DESC",
                      "PARAMETER_VALUE"]
    wash_ihn_level3_desc <-
        wash_params[lls_params$PARAMETER_NAME=="IHN_LEVEL3_DESC",
                    "PARAMETER_VALUE"]
    #
    results <- within(results,
    {
        # PL[CUVETTETYPE == "C4"] <- "128"
        # PL[CUVETTETYPE == "C16-ALINE"] <- "127"
        # PL[CUVETTETYPE == "C16-BLINE"] <- "127"
        #
        PHN_PATTERNS_SK[ALGORITHM == "cc_cuvette_lls"] <- lls_phm_patterns_sk
        PHN_PATTERNS_SK[ALGORITHM == "cc_cuvette_status_check"] <- status_phm_patterns_sk
        PHN_PATTERNS_SK[ALGORITHM == "cc_cuvette_wash_subassembly"] <- wash_phm_patterns_sk
        #
        IHN_LEVEL3_DESC[ALGORITHM == "cc_cuvette_lls"] <- lls_ihn_level3_desc
        IHN_LEVEL3_DESC[ALGORITHM == "cc_cuvette_status_check"] <- status_ihn_level3_desc
        IHN_LEVEL3_DESC[ALGORITHM == "cc_cuvette_wash_subassembly"] <- wash_ihn_level3_desc
    })
    #
    return(results)
}
