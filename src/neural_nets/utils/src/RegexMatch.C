//
// system headers
//
#include <unistd.h>
#include <string>
#include <list>
#include <vector>
#include <regex.h>
#include <assert.h>

//
// local includes
//
#include "RegexMatch.H"
#include "ExtUseCntPtr.H"

//------------------------------------------------------------------------------
// Procedure:   RegexMatch()
//
// Description: Ctor 
//
// Parameters:  const char * - regular expression
//              bool - ignore case if true
//
// Returns:     None
//------------------------------------------------------------------------------
PRegexMatch::PRegexMatch(const char *re_str, bool ignore_case)
{
    if (ignore_case == true)
    {
        //
        // case-insensitive search
        //
        assert(::regcomp(&_re, re_str, REG_EXTENDED|REG_ICASE) == 0);
    }
    else
    {
        //
        // case-sensitive search (default case)
        //
        assert(::regcomp(&_re, re_str, REG_EXTENDED) == 0);
    }
}

//------------------------------------------------------------------------------
// Procedure:   ~PRegexMatch
//
// Description: dtor 
//
// Parameters:  None
//
// Returns:     None
//------------------------------------------------------------------------------
PRegexMatch::~PRegexMatch()
{
    regfree(&_re);
}

//------------------------------------------------------------------------------
// Procedure:   operator const regex_t * () const
//
// Description: typecast to expose compiled string
//
// Parameters:  object of this type
//
// Returns:     internal compiled RE
//------------------------------------------------------------------------------
PRegexMatch::operator const regex_t * () const
{
    return &_re;
}

//------------------------------------------------------------------------------
// Procedure:   operator()
//
// Description: compares string to internal compilex regular expression
//
// Parameters:  const char * - string to check
//
// Returns:     true if match, else false
//------------------------------------------------------------------------------
bool
PRegexMatch::operator()(const char *buffer) const
{
    int match = ::regexec(*this, buffer, (size_t)0, NULL, 0);
    return ((match == 0) ? true : false);
}

//------------------------------------------------------------------------------
// Procedure:   match()
//
// Description: compares string to compiled regular expression
//
// Parameters:  PRegexMatch & - compiled RE.
//              const char * - string to check
//
// Returns:     true if match, else false
//------------------------------------------------------------------------------
bool
PRegexMatch::match(const PRegexMatch &re, const char *buffer)
{
    return re(buffer);
}

//------------------------------------------------------------------------------
// Procedure:   operator()
//
// Description: compares string to internal compilex regular expression
//
// Parameters:  const char * - string to check
//              vector<string> & - list of substrings that matched.
//              int - maximum substring matches (defaults to 100).
//
// Returns:     true if match, else false
//------------------------------------------------------------------------------
bool
PRegexMatch::operator()(const char *buffer,
                        vector<string> &subs,
                        int max_matches) const
{
    subs.clear();

    if (max_matches <= 0)
    {
        return (*this)(buffer);
    }
    else
    {
        max_matches += 1; // entire string is always match 0, so
                          // add one to handle the expected number
                          // of matches.

        ExtUseCntPtr<regmatch_t> pmatches;
        pmatches = new regmatch_t [max_matches];
        assert(pmatches != NULL);

        int match = ::regexec(*this, buffer, max_matches, pmatches, 0);
        if (match == 0)
        {
            for (int i=0; 
                 (i<max_matches) && 
                 ((pmatches[i].rm_so != -1) &&
                  (pmatches[i].rm_eo != -1));
                 ++i)
            {
                subs.push_back(
                    string(buffer+pmatches[i].rm_so,
                           pmatches[i].rm_eo-pmatches[i].rm_so));
            }
        }

        return ((match == 0) ? true : false);
    }
}

//------------------------------------------------------------------------------
// Procedure:   match()
//
// Description: compares string to compiled regular expression
//
// Parameters:  PRegexMatch & - compiled RE.
//              const char * - string to check
//              vector<string> & - list of substrings that matched.
//              int - maximum substring matches (defaults to 100).
//
// Returns:     true if match, else false
//------------------------------------------------------------------------------
bool
PRegexMatch::match(const PRegexMatch &re, 
                   const char *buffer,
                   vector<string> &subs,
                   int max_matches)
{
    return re(buffer, subs, max_matches);
}

//------------------------------------------------------------------------------
// Procedure:   search()
//
// Description: search strings to compiled regular expression
//
// Parameters:  PRegexMatch & - compiled RE.
//              const char * - string to check
//              vector<string> & - list of strings that matched.
//              int - maximum substring matches (defaults to 100).
//
// Returns:     true if match, else false
//------------------------------------------------------------------------------
bool
PRegexMatch::search(const PRegexMatch &re, 
                    const char *buffer,
                    vector<string> &subs,
                    int max_matches)
{
    const char *p = buffer;
    regmatch_t m[max_matches];

    while(true)
    {
        int i = 0;
        int no_match = ::regexec(*&re, p, max_matches, m, 0);
        if(no_match)
        {
            return no_match;
        }

        for(i = 0; i < max_matches; i++)
        {
            int start;
            int finish;

            if(m[i].rm_so == -1)
            {
                break;
            }

            start = m[i].rm_so + (p - buffer);
            finish = m[i].rm_eo + (p - buffer);
            if(i == 0)
            {
                subs.push_back(string(buffer + start, 
                                finish - start));
            }
        }
        p += m[0].rm_eo;
    }
    return 0;
}

