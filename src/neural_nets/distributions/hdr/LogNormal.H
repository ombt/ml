#ifndef __OMBT_LOGNORMAL_H
#define __OMBT_LOGNORMAL_H

// system headers
#include <stdio.h>
#include <string.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "ExtUseCntPtr.H"
#include "Gaussian.H"

// lognormal distribution
class LogNormal
{
public:
    // ctors and dtor
    LogNormal();
    LogNormal(double muln, double sigmaln2, const Random &rng);
    LogNormal(const LogNormal &src);
    ~LogNormal();

    // assignment
    LogNormal &operator=(const LogNormal &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double muln_;
    double sigmaln2_;
    ExtUseCntPtr<Gaussian> pgrng_;
};

#endif
