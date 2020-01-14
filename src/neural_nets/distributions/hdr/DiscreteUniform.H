#ifndef __OMBT_DISCRETE_UNIFORM_H
#define __OMBT_DISCRETE_UNIFORM_H

// system headers
#include <stdio.h>
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// discrete uniform distribution
class DiscreteUniform
{
public:
    // ctors and dtor
    DiscreteUniform();
    DiscreteUniform(double start, double end, const Random &rng);
    DiscreteUniform(const DiscreteUniform &src);
    ~DiscreteUniform();

    // assignment
    DiscreteUniform &operator=(const DiscreteUniform &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double start_;
    double end_;
    Random rng_;
};

#endif
