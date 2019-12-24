#
# general purpose utils
#
closedevs <- function()
{
    while (dev.cur() > 1) { dev.off() }
}

closealldevs <- function()
{
    closedevs()
}

mysrc <- function(file)
{
    closealldevs()
    source(file=file, echo=TRUE, max.deparse.length=10000)
}

lnrow <- function(dfrm) 
{
    lapply(dfrm, nrow)
}

lncol <- function(dfrm) 
{
    lapply(dfrm, ncol)
}

lclass <- function(dfrm) 
{
    lapply(dfrm, class)
}

lhead <- function(dfrm) 
{
    lapply(dfrm, head)
}

lnames <- function(dfrm) 
{
    lapply(dfrm, names)
}
#
# estimate for e using monte-carlo. just choose a value for N.
#
# from R-bloggers
#
mc_e <- function(N=100000)
{
    1/mean(N*diff(sort(runif(N+1))) > 1)
}
#
# convert time string to unix time, and vice-versa
#
#	Code	Meaning			Code	Meaning
#	%a	Abbreviated weekday	%A	Full weekday
#	%b	Abbreviated month	%B	Full month
#	%c	Locale-specific 
#			date and time	%d	Decimal date
#	%H	Decimal hours (24 hour)	%I	Decimal hours (12 hour)
#	%j	Decimal day of the year	%m	Decimal month
#	%M	Decimal minute		%p	Locale-specific AM/PM
#	%S	Decimal second		%U	Decimal week of the year 
#						(starting on Sunday)
#	%w	Decimal Weekday 
# 			(0=Sunday)	%W	Decimal week of the year 
#						(starting on Monday)
#	%x	Locale-specific Date	%X	Locale-specific Time
#	%y	2-digit year		%Y	4-digit year
#	%z	Offset from GMT		%Z	Time zone (character)
#
datetime_to_tstamp <- function(datetime,
                               format = "%Y/%m/%d %H:%M:%S",
                               tz = Sys.timezone())
{
    for (dt in as.vector(datetime))
    {
        print(as.numeric(strptime(dt, format=format, tz=tz)));
    }
}
#
tstamp_to_datetime <- function(tstamp,
                               format = "%Y/%m/%d %H:%M:%S",
                               tz = Sys.timezone())
{
    for (ts in as.vector(tstamp))
    {
        print(as.POSIXct(ts, origin="1970-01-01", tz=tz));
    }
}
#
# list function which uses globs instead if regular expression
#
lf <- function(globpat="*",env.pos=1)
{
    return(ls(pattern=glob2rx(pattern=globpat),
              envir=as.environment(env.pos)))
}
#
# open/close a sink file
#
open_sink <- function(sink_file)
{
    print(sprintf("OPEN SINK FILE: <%s>", sink_file))
    if (sink_file != "")
    {
        sink(sink_file)
    }
}

close_sink <- function(sink_file)
{
    print(sprintf("CLOSE SINK FILE: <%s>", sink_file))
    if (sink_file != "")
    {
        sink()
    }
}
#
# check if a symbol exists without having to quote it.
#
my.exists <- function(sym, inherits=TRUE)
{
    sym <- deparse(substitute(sym))
    env <- parent.frame()
    return(exists(sym, env, inherits=inherits))
}

#
# union the columns in a dataframe
#
df_union <- function(mydf)
{
    if ( ! is.data.frame(mydf))
    {
        stop(sprintf("%s is NOT a data frame.",
                     deparse(substitute(mydf))))
    }
    else if (ncol(mydf) == 0)
    {
        return(c())
    }
    else if (ncol(mydf) == 1)
    {
        return(mydf[[1]])
    }
    else
    {
        udf = mydf[[1]]
        for (i in 2:ncol(mydf))
        {
            udf = union(udf, mydf[[i]])
        }
        return(udf)
    }
}

list_union <- function(myl)
{
    if ( ! is.list(myl))
    {
        stop(sprintf("%s is NOT a list.", deparse(substitute(myl))))
    }
    else if (length(myl) == 0)
    {
        return(c())
    }
    else if (length(myl) == 1)
    {
        return(myl[[1]])
    }
    else
    {
        umyl = myl[[1]]
        for (i in 2:length(myl))
        {
            umyl = union(umyl, myl[[i]])
        }
        return(umyl)
    }
}

list_intersection <- function(myl)
{
    if ( ! is.list(myl))
    {
        stop(sprintf("%s is NOT a list.", deparse(substitute(myl))))
    }
    else if (length(myl) == 0)
    {
        return(c())
    }
    else if (length(myl) == 1)
    {
        return(c())
    }
    else
    {
        int_myl = myl[[1]]
        for (i in 2:length(myl))
        {
            int_myl = intersection(int_myl, myl[[i]])
        }
        return(int_myl)
    }
}
#
# open/close a log file
#
open_log_file <- function(log_file="LOG_FILE", append=TRUE, split=TRUE)
{
    if (( ! is.na(log_file)) && (log_file != ""))
    {
        options(try.outFile=stdout())
        sink(log_file, 
             append=append, 
             type=c("output", "message"),
             split=split)
        print(sprintf("%s; OPEN LOG FILE <%s>", Sys.time(), log_file))
        return(TRUE)
    }
    else
    {
        print(sprintf("%s; NO LOG FILE GIVEN", Sys.time()))
        return(FALSE)
    }
}

close_log_file <- function()
{
    print(sprintf("%s: CLOSE LOG FILE", Sys.time()))
    options(try.outFile=stderr())
    sink()
    return(TRUE)
}
#
# read in a name=value pair file
#
get_parameters <- function(parameter_file)
{
    contents = readLines(parameter_file)
}

#
# traverse a list and print names
#
nametree <- function(X, prefix = "") {
    if ( is.list(X) ) {
        for ( i in seq_along(X) ) { 
            cat( prefix, names(X)[i], "\n", sep="" )
            nametree(X[[i]], paste0(prefix, "  "))
        }
    }
}

