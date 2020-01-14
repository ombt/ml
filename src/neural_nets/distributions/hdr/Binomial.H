#ifndef __OMBT_BINOMIAL_H
#define __OMBT_BINOMIAL_H

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// binomial distribution
class Binomial
{
public:
    // ctors and dtor
    Binomial();
    Binomial(double p, unsigned long n, const Random &rng);
    Binomial(const Binomial &src);
    ~Binomial();

    // assignment
    Binomial &operator=(const Binomial &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double p_;
    unsigned long n_;
    Random rng_;
};

#endif
