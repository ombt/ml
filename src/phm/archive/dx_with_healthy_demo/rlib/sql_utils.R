#
# utilities for building sql statements
#
sql_generate_in_clause <- function(db_column, values, equal_to=TRUE)
{
    #
    # coerce to a vector
    #
    vvals <- as.vector(values)
    #
    # check if we succeeded
    #
    if ((!is.vector(vvals)) || (length(vvals) == 0))
    {
        # nothing was given
        return("")
    }
    #
    # generate equal/in clause depending on the number of
    # values in the vector and whether it is a character string.
    #
    clause = ""
    oper = ""
    #
    if (length(vvals) == 1)
    {
        if (typeof(vvals) == "character")
        {
            if (equal_to == TRUE)
            {
                oper = " = '"
            }
            else
            {
                oper = " != '"
            }
            clause = paste(db_column, oper, vvals[1], "'", sep="")
        }
        else
        {
            if (equal_to == TRUE)
            {
                oper = "="
            }
            else
            {
                oper = "!="
            }
            clause = paste(db_column, oper, vvals[1])
        }
    }
    else
    {
        if (typeof(vvals) == "character")
        {
            if (equal_to == TRUE)
            {
                oper = " in ( '"
            }
            else
            {
                oper = " not in ( '"
            }
            clause = paste(db_column, oper, paste(vvals,collapse="', '"), "' )", sep="")
        }
        else
        {
            if (equal_to == TRUE)
            {
                oper = "in ("
            }
            else
            {
                oper = "not in ("
            }
            clause = paste(db_column, oper, paste(vvals,collapse=","), ")")
        }
    }

    return(clause);
}
#
sql_generate_range_clause <- function(db_column, values, in_range=TRUE)
{
    #
    # coerce to a vector
    #
    vvals <- as.vector(values)
    #
    # check if we succeeded
    #
    if ((!is.vector(vvals)) || (length(vvals) == 0))
    {
        # nothing was given
        return("")
    }
    else if (length(vvals) != 2)
    {
        stop(sprintf("Range Clause MUST ONLY have two values, MIN and MAX."))
    }
    #
    # get MIN and MAX values
    #
    min = vvals[1];
    max = vvals[2];
    #
    if (min > max)
    {
        tmp = min
        min = max
        max = tmp
    }
    #
    clause = ""
    #
    if (typeof(vvals) == "character")
    {
        if (in_range == TRUE)
        {
            clause = sprintf("('%s' <= '%s') AND ('%s' <= '%s')", 
                             min, db_column, db_column, max)
        }
        else
        {
            clause = sprintf("NOT (('%s' <= '%s') AND ('%s' <= '%s'))", 
                             min, db_column, db_column, max)
        }
    }
    else
    {
        if (in_range == TRUE)
        {
            clause = sprintf("(%s <= %s) AND (%s <= %s)", 
                             min, db_column, db_column, max)
        }
        else
        {
            clause = sprintf("NOT ((%s <= %s) AND (%s <= %s))", 
                             min, db_column, db_column, max)
        }
    }

    return(clause);
}
#
# load data with a where clause
#
sql_add_to_clause <- function(oper, clause, new_clause)
{
    if (new_clause == "")
    {
        return(clause);
    }
    else if (clause == "")
    {
        return(paste("(", new_clause, ")"))
    }
    else
    {
        return(paste(clause, oper, "(", new_clause, ")"))
    }
}
#
