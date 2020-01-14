#ifndef __OMBT_GEOMETRIC_H
#define __OMBT_GEOMETRIC_H

// system headers
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// geometric distribution
class Geometric
{
public:
    // ctors and dtor
    Geometric();
    Geometric(double p, const Random &rng);
    Geometric(const Geometric &src);
    ~Geometric();

    // assignment
    Geometric &operator=(const Geometric &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double p_;
    Random rng_;
};

#endif
