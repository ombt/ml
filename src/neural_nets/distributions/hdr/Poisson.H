#ifndef __OMBT_POISSON_H
#define __OMBT_POISSON_H

// system headers
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// poisson distribution
class Poisson
{
public:
    // ctors and dtor
    Poisson();
    Poisson(double lambda, const Random &rng);
    Poisson(const Poisson &src);
    ~Poisson();

    // assignment
    Poisson &operator=(const Poisson &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double lambda_;
    double a_;
    Random rng_;
};

#endif
