#ifndef __OMBT_PEARSON_TYPE_V_H
#define __OMBT_PEARSON_TYPE_V_H

// system headers
#include <stdio.h>
#include <string.h>

// local headers
#include "Returns.H"
#include "Debug.H"
#include "ExtUseCntPtr.H"
#include "Gamma.H"

// pearson type V distribution
class PearsonTypeV
{
public:
    // ctors and dtor
    PearsonTypeV();
    PearsonTypeV(double alpha, double beta, const Random &rng);
    PearsonTypeV(const PearsonTypeV &src);
    ~PearsonTypeV();

    // assignment
    PearsonTypeV &operator=(const PearsonTypeV &rhs);

    // get a random value
    double nextValue();

protected:
    // data
    double alpha_;
    double beta_;
    ExtUseCntPtr<Gamma> pgrng_;
};


#endif
