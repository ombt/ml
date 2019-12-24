#
# remove item from environment
#
ifrm <- function(x, env=globalenv())
{
    if (exists(x, envir=env))
    {
        rm(list=x, envir=env);
    }
}
#
# check if a package is installed. if not, then install
# and download.
#
install_and_load <- function (package1, ...)
{
    verbose = FALSE
    #
    # convert arguments to vector
    #
    packages <- c(package1, ...)
    #
    # check if loaded and installed
    #
    loaded        <- packages %in% (.packages())
    names(loaded) <- packages
    #
    installed        <- packages %in% rownames(installed.packages())
    names(installed) <- packages
    #
    # start loop to determine if each package is installed
    #
    for (package in packages)
    {
        if (loaded[package])
        {
            if (verbose) print(paste(package, "loaded"))
        }
        else
        {
            if (verbose) print(paste(package, "not loaded"))
            if (installed[package])
            {
                if (verbose) print(paste(package, "installed"))
                do.call("library", list(package))
            }
            else
            {
                if (verbose) print(paste(package, "not installed"))
                install.packages(package)
                do.call("library", list(package))
            }
        }
    } 
}
#
# same as above but it uses lapply() instead of a loop.
#
lapply_install_and_load <- function (package1, ...)
{
    verbose = FALSE
    #
    # convert arguments to vector
    #
    packages <- c(package1, ...)
    #
    # check if loaded and installed
    #
    loaded        <- packages %in% (.packages())
    names(loaded) <- packages
    #
    installed        <- packages %in% rownames(installed.packages())
    names(installed) <- packages
    #
    # start loop to determine if each package is installed
    #
    load_it <- function (package, loaded, installed)
    {
        if (loaded[package])
        {
            if (verbose) print(paste(package, "is loaded"))
        }
        else
        {
            if (verbose) print(paste(package, "is not loaded"))
            if (installed[package])
            {
                if (verbose) print(paste(package, "is installed"))
                if (verbose) print(paste("loading", package))
                do.call("library", list(package))
            }
            else
            {
                if (verbose) print(paste(package, "is not installed"))
                if (verbose) print(paste("installing", package))
                install.packages(package)
                if (verbose) print(paste("loading", package))
                do.call("library", list(package))
            }
        }
    }
    #
    lapply(packages, load_it, loaded, installed)
}
