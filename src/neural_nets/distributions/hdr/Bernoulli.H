#ifndef __OMBT_BERNOULLI_H
#define __OMBT_BERNOULLI_H

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// uniform distribution
class Bernoulli
{
public:
    // ctors and dtor
    Bernoulli();
    Bernoulli(double p, const Random &rng);
    Bernoulli(const Bernoulli &src);
    ~Bernoulli();

    // assignment
    Bernoulli &operator=(const Bernoulli &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double p_;
    Random rng_;
};

#endif
