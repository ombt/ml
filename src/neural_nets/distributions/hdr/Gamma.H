#ifndef __OMBT_GAMMA_H
#define __OMBT_GAMMA_H

// system headers
#include <stdio.h>
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// gamma distribution
class Gamma
{
public:
    // ctors and dtor
    Gamma();
    Gamma(double beta, double alpha, const Random &rng);
    Gamma(const Gamma &src);
    ~Gamma();

    // assignment
    Gamma &operator=(const Gamma &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double alpha_;
    double beta_;
    Random rng_;
};

#endif
