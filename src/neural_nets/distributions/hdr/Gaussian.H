//
#ifndef __OMBT_GAUSSIAN_H
#define __OMBT_GAUSSIAN_H

// system headers
#include <stdio.h>
#include <math.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "Random.H"

// gaussian distribution
class Gaussian
{
public:
    // ctors and dtor
    Gaussian();
    Gaussian(double mu, double sigma2, const Random &rng);
    Gaussian(const Gaussian &src);
    ~Gaussian();

    // assignment
    Gaussian &operator=(const Gaussian &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    int xindex_;
    double mu_;
    double sigma_;
    double sigma2_;
    Random rng_;
};

#endif
