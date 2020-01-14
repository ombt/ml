#ifndef __OMBT_NEGATIVE_BINOMIAL_H
#define __OMBT_NEGATIVE_BINOMIAL_H

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"
#include "Geometric.H"

// negative binomial distribution
class NegativeBinomial
{
public:
    // ctors and dtor
    NegativeBinomial();
    NegativeBinomial(double p, unsigned long n, const Random &rng);
    NegativeBinomial(const NegativeBinomial &src);
    ~NegativeBinomial();

    // assignment
    NegativeBinomial &operator=(const NegativeBinomial &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    unsigned long n_;
    Geometric grng_;
};

#endif
