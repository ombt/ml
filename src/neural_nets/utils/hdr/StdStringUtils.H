
#ifndef	__STD_STRING_UTILS_H
#define	__STD_STRING_UTILS_H

//
// system includes
//
#include <string>
#include <vector>

using namespace std;

//
// split - split string using a set of delimiters.
// trim_left - remove WS on left side of string.
// trim_right - remove WS on right side of string.
// trim - remove WS on both ends of string.
// read_file - read text file into a string.
// to_uppercase - convert string to upper case.
// to_lowercase - convert string to lower case.
// replace_extra_ws - replace one or more contiguous WS with one char.
// remove_char - remove a character throught out string
//
template <class ContainerType>
void split(const string &str, 
           ContainerType &tokens,
           const string &delimiters, 
           bool trim_empty);
string trim_left(const string &source, 
                 const string &list = "\"\t\n\r ");
string trim_right(const string &source, 
                  const string &list = "\"\t\n\r ");
string trim(const string &source, 
            const string &list = "\"\t\n\r ");
void read_file(const string &path, string &contents);
void to_uppercase(string &s);
void to_lowercase(string &s);
string replace_extra_ws(const string &source,
                        const string &list = "\"\t\n\r ",
                        const string &replace = " ");
int remove_char(string &s, char c_to_remove);

#include "StdStringUtils.I"

#endif

