#ifndef __OMBT_BETA_H
#define __OMBT_BETA_H

// system headers
#include <stdio.h>
#include <string.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "ExtUseCntPtr.H"
#include "Gamma.H"

// beta distribution
class Beta
{
public:
    // ctors and dtor
    Beta();
    Beta(double alpha1, double alpha2, const Random &rng);
    Beta(const Beta &src);
    ~Beta();

    // assignment
    Beta &operator=(const Beta &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double alpha1_;
    double alpha2_;
    ExtUseCntPtr<Gamma> pgrng1_;
    ExtUseCntPtr<Gamma> pgrng2_;
};

#endif
