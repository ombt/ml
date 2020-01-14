//
// exponential distribution
//
// F(x) = 1-exp(x/beta)
// X = -beta*log(1-U), U in (0,1).
// or, 
// X = -beta*log(U), U in (0,1).

// headers
#include "Exponential.H"

// ctors and dtor
Exponential::Exponential(): 
    beta_(0), rng_()
{
}

Exponential::Exponential(double beta, const Random &rng): 
    beta_(beta), rng_(rng)
{
}

Exponential::Exponential(const Exponential &src): 
    beta_(src.beta_), rng_(src.rng_)
{
    // nothing to do
}

Exponential::~Exponential()
{
}

// assignment
Exponential &
Exponential::operator=(const Exponential &rhs)
{
    if (this != &rhs)
    {
        beta_ = rhs.beta_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Exponential::nextValue()
{
    double u = rng_.random0to1();
    return(-beta_*log(u));
}

