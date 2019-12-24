read_csv_file <- function(csv_path, 
                          fname,
                          character_cols = c(),
                          integer_cols = c(),
                          numeric_cols = c(),
                          null_cols=c(), 
                          not_null_cols=c(), 
                          separator = "\t",
                          nrows=-1)
{
    #
    # build path to csv file.
    #
    path = paste(csv_path, fname, sep = "/")
    #
    # get column names 
    #
    headset = read.csv(path, 
                       header = TRUE, 
                       sep="\t", 
                       nrows = 10)
    classes = sapply(headset, class)
    #
    # set requested classes
    #
    if (length(character_cols) > 0)
    {
        classes[names(classes) %in% character_cols] = "character"
    }
    if (length(integer_cols) > 0)
    {
        classes[names(classes) %in% integer_cols] = "integer"
    }
    if (length(numeric_cols) > 0)
    {
        classes[names(classes) %in% numeric_cols] = "numeric"
    }
    #
    # default any unknown columns to character
    #
    all_classes = c(character_cols, 
                    integer_cols, 
                    numeric_cols)
    classes[!(names(classes) %in% all_classes)] = "character"
    #
    # remove any unwanted columns
    #
    if (length(null_cols) > 0)
    {
        classes[names(classes) %in% null_cols] = "NULL";
    }
    if (length(not_null_cols) > 0)
    {
        classes[!(names(classes) %in% not_null_cols)] = "NULL";
    }
    #
    # read in csv file and return
    #
    return(read.csv(path, 
                    header=TRUE, 
                    sep=separator, 
                    colClasses=classes, 
                    nrows=nrows))
}
