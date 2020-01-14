#ifndef __OMBT_WEIBULL_H
#define __OMBT_WEIBULL_H

// system headers
#include <stdio.h>
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// weibull distribution
class Weibull
{
public:
    // ctors and dtor
    Weibull();
    Weibull(double alpha, double beta, const Random &rng);
    Weibull(const Weibull &src);
    ~Weibull();

    // assignment
    Weibull &operator=(const Weibull &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double alpha_;
    double beta_;
    Random rng_;
};

#endif
