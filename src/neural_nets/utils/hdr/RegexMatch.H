#ifndef _REGEX_MATCH_H
#define _REGEX_MATCH_H

// 
// OS includes
// 
#include <regex.h>
#include <vector>
#include <string>

using namespace std;

//
// check a string using a regular expression.
//
class PRegexMatch
{
protected: // member data types
    enum { MAX_MATCHES = 10 } ;

public: // member functions
    //
    // ctors and dtor
    //
    PRegexMatch(const char *re_str, bool ignore_case = false);
    ~PRegexMatch();

    //
    // operator const regex * - exposes compiled RE.
    // operator() - checks string for a match using the RE.
    // match - checks string for a match using any RE.
    // match - checks string for a match using any RE and return
    //         substrings.
    // search- look for all matched token in a string.
    //
    operator const regex_t * () const;
    bool operator()(const char *buffer) const;
    bool operator()(const char *buffer,
                    vector<string> &subs,
                    int max_matches = MAX_MATCHES) const;
    static bool match(const PRegexMatch &re, 
                      const char *buffer);
    static bool match(const PRegexMatch &re, 
                      const char *buffer,
                      vector<string> &subs,
                      int max_matches = MAX_MATCHES);
    static bool search(const PRegexMatch &re, 
                       const char *buffer,
                       vector<string> &subs,
                       int max_matches = MAX_MATCHES);

protected: // member data
    regex_t _re;

private: // do not allow these ... for now.
    PRegexMatch();
    PRegexMatch(const PRegexMatch &);
    PRegexMatch &operator=(const PRegexMatch &);
};

#endif
