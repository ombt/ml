//
// system headers
//
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <time.h>
#include <algorithm>
#include <fstream>
#include <sstream>

//
// classes includes
//
#include "StdStringUtils.H"

//-----------------------------------------------------------------------------
// Procedure:   trim_left
//
// Description: remove WS from left end of a string.
//
// Parameters:  const string & - string to to trim.
//              const string & - string with characters to remove.
//
// Returns:     string - trimmed string
//-----------------------------------------------------------------------------
string
trim_left(const string &source, const string &list)
{
    string work = source;
    return work.erase(work.find_last_not_of(list)+1);
}

//-----------------------------------------------------------------------------
// Procedure:   trim_right
//
// Description: remove WS from right end of a string.
//
// Parameters:  const string & - string to to trim.
//              const string & - string with characters to remove.
//
// Returns:     string - trimmed string
//-----------------------------------------------------------------------------
string
trim_right(const string &source, const string &list)
{
    string work = source;
    return work.erase(0, work.find_first_not_of(list));
}

//-----------------------------------------------------------------------------
// Procedure:   trim
//
// Description: remove WS from both ends of a string.
//
// Parameters:  const string & - string to to trim.
//              const string & - string with characters to remove.
//
// Returns:     string - trimmed string
//-----------------------------------------------------------------------------
string
trim(const string &source, const string &list)
{
    string work = source;
    return trim_left(trim_right(work, list), list);
}

//-----------------------------------------------------------------------------
// Procedure:   read_file
//
// Description: read file into a string.
//
// Parameters:  const string & - path to file
//              string & - contents returned
//
// Returns:     none
//-----------------------------------------------------------------------------
void
read_file(const string &path, string &contents)
{
    ifstream infile(path.c_str());
    stringstream buffer;
    buffer << infile.rdbuf();
    contents = buffer.str();
    infile.close();
}

//-----------------------------------------------------------------------------
// Procedure:   to_uppercase
//
// Description: convert string to uppercase in place
//
// Parameters:  string & - string to convert to uppercase
//
// Returns:     none
//-----------------------------------------------------------------------------
void
to_uppercase(string &s)
{
    transform(s.begin(), s.end(), s.begin(), ::toupper);
}

//-----------------------------------------------------------------------------
// Procedure:   to_lowercase
//
// Description: convert string to lowercase in place
//
// Parameters:  string & - string to convert to lowercase
//
// Returns:     none
//-----------------------------------------------------------------------------
void
to_lowercase(string &s)
{
    transform(s.begin(), s.end(), s.begin(), ::tolower);
}

//-----------------------------------------------------------------------------
// Procedure:   replace_extra_ws
//
// Description: replace two or more contiguous WS with one char.
//
// Parameters:  const string & - string to scan
//              const string & - string with characters to remove.
//              const string & - replacement string.
//
// Returns:     string - trimmed string
//-----------------------------------------------------------------------------
string
replace_extra_ws(const string &source, 
                 const string &list, 
                 const string &replace)
{
    string work = source;
    for (size_t start = 0;
        (start = work.find_first_of(list, start)) != string::npos;
         start += replace.length())
    {
        size_t end = work.find_first_not_of(list, start+1);
        if (end == string::npos)
        {
            end = work.size();
        }
        work.replace(start, end-start, replace);
    }
    return work;
}

//-----------------------------------------------------------------------------
// Procedure:   remove_char
//
// Description: remove a character from entire string
//
// Parameters:  string & - string to process
//              char - character to remove
//
// Returns:     number of characters removed
//-----------------------------------------------------------------------------
int
remove_char(string &s, char c_to_remove)
{
    int before = s.size();
    s.erase(std::remove(s.begin(), s.end(), c_to_remove), s.end());
    return (before-s.size());
}

