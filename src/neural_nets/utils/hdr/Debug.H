#ifndef __OMBT_DEBUG_H
#define __OMBT_DEBUG_H

// headers
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <iostream>

// check for overflow
#ifdef OVERFLOWCHECK
#define CheckForOverFlow(L_x, L_y) { \
        if (fabs(L_x) != 0 && fabs(L_y) != 0) \
        { \
                double L_lx, L_ly; \
                L_lx = log10(fabs(L_x)); \
                L_ly = log10(fabs(L_y)); \
                MustBeTrue(fabs(L_lx+L_ly) < 308); \
        } \
}
#else
#define CheckForOverFlow(L_x, L_y)
#endif

// check macros
#if defined(__STDC__)
#if defined(USEEXCEPTION)
#define MustBeTrue(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "INTERNAL ERROR AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << #EXPR << std::endl; \
		throw #EXPR ; \
	}

#else
#define MustBeTrue(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "INTERNAL ERROR AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << #EXPR << std::endl; \
		abort(); \
	}
#endif

#define CheckReturn(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "ERROR RETURN AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << #EXPR << std::endl; \
		return(NOTOK); \
	}

#define ShouldBeTrue(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "WARNING AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << #EXPR << std::endl; \
	}

#else

#if defined(USEEXCEPTION)
#define MustBeTrue(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "INTERNAL ERROR AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << "EXPR" << std::endl; \
		throw "EXPR" ; \
	}
#else
#define MustBeTrue(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "INTERNAL ERROR AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << "EXPR" << std::endl; \
		abort(); \
	}
#endif

#define CheckReturn(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "ERROR RETURN AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << "EXPR" << std::endl; \
		return(NOTOK); \
	}

#define ShouldBeTrue(EXPR) \
	if (!(EXPR)) { \
		std::cerr << "WARNING AT "; \
		std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__; \
		std::cerr << ": " << "EXPR" << std::endl; \
	}
#endif

// simple debugging macros
#ifdef DEBUG

#define TMSG(_tmsg) \
	std::cerr << "TRACE " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " << _tmsg << std::endl
#define TRACE() \
	std::cerr << "TRACE " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " << std::endl
#define RETURN(myretval) { \
	std::cerr << "RETURN " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " << std::endl; \
	return(myretval); \
}
#if defined(__STDC__)
#define DUMP(myvalue) \
	std::cerr << "DUMP " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " #myvalue " = " << myvalue << std::endl
#define DUMP2(myvalue, myvalue2) \
	std::cerr << "DUMP " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " #myvalue " = " << myvalue << ", " #myvalue2 " = " << myvalue2 << std::endl
#define DMP(myvalue) \
	std::cerr << "DUMP " << __FUNCTION__ << "'" << __LINE__ << ": " #myvalue " = " << myvalue << std::endl
#define DMP2(myvalue, myvalue2) \
	std::cerr << "DUMP " << __FUNCTION__ << "'" << __LINE__ << ": " #myvalue " = " << myvalue << ", " #myvalue2 " = " << myvalue2 << std::endl
#else
#define DUMP(myvalue) \
	std::cerr << "DUMP " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": myvalue = " << myvalue << std::endl
#define DUMP2(myvalue, myvalue2) \
	std::cerr << "DUMP " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": myvalue = " << myvalue << ", myvalue2 = " << myvalue2 << std::endl
#define DMP(myvalue) \
	std::cerr << "DUMP " << __FUNCTION__ << "'" << __LINE__ << ": myvalue = " << myvalue << std::endl
#define DMP2(myvalue, myvalue2) \
	std::cerr << "DUMP " << __FUNCTION__ << "'" << __LINE__ << ": myvalue = " << myvalue << ", myvalue2 = " << myvalue2 << std::endl
#endif
#define DUMPS(myvalue) \
	std::cerr << "DUMP " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " << myvalue << std::endl

#define IFTRACE() { \
	extern int _iftrace; \
	if (_iftrace) \
		std::cerr << "TRACE " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " << std::endl; \
}

#define IFTMSG(_tmsg) { \
	extern int _iftrace; \
	if (_iftrace) \
		std::cerr << "TRACE " << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": " << _tmsg << std::endl; \
}

#define DEFINETRACE() \
	int _iftrace

#define EXTERNTRACE() \
	extern int _iftrace

#define ENABLETRACE() { \
	extern int _iftrace; \
	_iftrace = 1; \
}

#define DISABLETRACE() { \
	extern int _iftrace; \
	_iftrace = 0; \
}

#define GETTRACE() _iftrace

#else

#define TMSG(_tmsg)
#define TRACE()
#define RETURN(myretval)
#define DUMP(myvalue)
#define DUMPS(myvalue)
#define IFTMSG()
#define IFTRACE()
#define DEFINETRACE()
#define EXTERNTRACE()
#define ENABLETRACE()
#define DISABLETRACE()
#define GETTRACE() 

#endif

#define ERROR(errmsg, errval) { \
	std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": "; \
	if ((errval) > 0) \
	{ \
		std::cerr << (errmsg) << " (errno = " << (errval); \
		std::cerr << ", " << ::strerror((errval)) << ")" << std::endl; \
	} \
	else \
	{ \
		std::cerr << (errmsg) << " (errno = " << (errval) << ")" << std::endl; \
	} \
}
#define ERRORD(errmsg, errdata, errval) { \
	std::cerr << __FILE__ << "'" << __FUNCTION__ << "'" << __LINE__ << ": "; \
	if ((errval) > 0) \
	{ \
		std::cerr << (errmsg) << " (errno = " << (errval); \
		std::cerr << ", " << ::strerror((errval)) << ")" << std::endl; \
	} \
	else \
	{ \
		std::cerr << (errmsg) << " (errno = " << (errval) << ")" << std::endl; \
	} \
	std::cerr << "(data = " << (errdata) << ")" << std::endl; \
}

#endif
