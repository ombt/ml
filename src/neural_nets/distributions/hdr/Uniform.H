#ifndef __OMBT_UNIFORM_H
#define __OMBT_UNIFORM_H

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// uniform distribution
class Uniform
{
public:
    // ctors and dtor
    Uniform();
    Uniform(double a, double b, const Random &rng);
    Uniform(const Uniform &src);
    ~Uniform();

    // assignment
    Uniform &operator=(const Uniform &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double a_;
    double b_;
    Random rng_;
};

#endif
