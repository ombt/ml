#ifndef __OMBT_EXPONENTIAL_H
#define __OMBT_EXPONENTIAL_H

// system headers
#include <stdio.h>
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// exponential distribution
class Exponential
{
public:
    // ctors and dtor
    Exponential();
    Exponential(double beta, const Random &rng);
    Exponential(const Exponential &src);
    ~Exponential();

    // assignment
    Exponential &operator=(const Exponential &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double beta_;
    Random rng_;
};

#endif
