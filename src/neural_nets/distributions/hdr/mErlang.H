#ifndef __OMBT_MERLANG_H
#define __OMBT_MERLANG_H

// system headers
#include <stdio.h>
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// exponential distribution
class mErlang
{
public:
    // ctors and dtor
    mErlang();
    mErlang(double beta, unsigned long m, const Random &rng);
    mErlang(const mErlang &src);
    ~mErlang();

    // assignment
    mErlang &operator=(const mErlang &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double beta_;
    unsigned long m_;
    Random rng_;
};

#endif
