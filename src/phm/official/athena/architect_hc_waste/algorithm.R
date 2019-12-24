#
# Architect HC Waste 
#
#####################################################################
#
# algorithm specific
#
flagged_query_template <- "
with raw_data_cte as (
select 
    ar.architect_deviceid,
    upper(trim(ar.architect_moduleserial)) as architect_moduleserial,
    ar.architect_productline,
    ar.completiondatetime_iso,
    ar.primarywavelength,
    ar.primarywavelengthreads,
    cast(coalesce(split_part(ar.primarywavelengthreads, ' ', 30),'0.0') as double) as p29,
    cast(coalesce(split_part(ar.primarywavelengthreads, ' ', 31),'0.0') as double) as p30,
    cast(coalesce(split_part(ar.primarywavelengthreads, ' ', 32),'0.0') as double) as p31,
    cast(coalesce(split_part(ar.primarywavelengthreads, ' ', 33),'0.0') as double) as p32,
    cast(coalesce(split_part(ar.primarywavelengthreads, ' ', 34),'0.0') as double) as p33,
    ar.transaction_date
from
    dx.dx_architect_results ar
where
    ar.architect_productline in ( '126', '127', '128' )
and
    ar.transaction_date >= '<START_DATE>'
and
    ar.transaction_date < '<END_DATE>'
and
    ar.exceptioncode = '1053'
and
    cast(regexp_replace(coalesce(split_part(ar.primarywavelengthreads, ' ', 1),'0'), '\\..*$') as integer) >= 34

order by
    ar.architect_moduleserial asc,
    ar.completiondatetime_iso asc
),
munged_data_cte as (
select 
    raw.architect_deviceid,
    raw.architect_moduleserial,
    raw.architect_productline,
    raw.completiondatetime_iso,
    raw.primarywavelength,
    raw.primarywavelengthreads,
    raw.p29,
    raw.p30,
    raw.p31,
    raw.p32,
    raw.p33,
    case when (raw.p29/nullif(raw.p30,0) - 
               greatest(raw.p31/nullif(raw.p32,0),
                        raw.p32/nullif(raw.p33,0))) < -0.03
         then 1
         else 0
         end as slope_diff_flag,
    raw.transaction_date
from
    raw_data_cte raw
order by
    raw.architect_moduleserial asc,
    raw.completiondatetime_iso asc
),
flagged_cte as (
select 
    mud.architect_moduleserial,
    mud.architect_productline,
    max(mud.completiondatetime_iso) as flag_date,
    count(*) as count_slop,
    avg(mud.slope_diff_flag) as average_slop
from
    munged_data_cte mud
group by
    mud.architect_moduleserial,
    mud.architect_productline
)
select 
    flagged.architect_moduleserial as modulesn,
    flagged.architect_productline as pl,
    flagged.count_slop,
    flagged.average_slop,
    date_format(flagged.flag_date,'%Y%m%d%H%i%s') as flag_date
from 
    flagged_cte flagged
where
    flagged.count_slop >= 10
and
    flagged.average_slop >= 0.5"
#
modulesn_query_template <- "
select distinct
    upper(trim(dxr.architect_moduleserial)) as modulesn,
    dxr.architect_productline as pl
from
    dx.dx_architect_results dxr
where
    dxr.architect_moduleserial is not null
and
    dxr.architect_productline is not null
and
    dxr.architect_productline in ( '126', '127', '128' )
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
number_of_days <- 1
#
# product line code for output file
#
product_line_code <- NA
#
# configuration type, athena or spark
#
config_type <- "dx"
#
