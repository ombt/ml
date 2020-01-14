//
// bernoulli distribution
//
// F(X) = if (X<=p) return 1, else return 0;
//

// headers
#include "Bernoulli.H"

// ctors and dtor
Bernoulli::Bernoulli(): 
    p_(0), rng_()
{
}

Bernoulli::Bernoulli(double p, const Random &rng): 
    p_(p), rng_(rng)
{
}

Bernoulli::Bernoulli(const Bernoulli &src) :
    p_(src.p_), rng_(src.rng_)
{
    // nothing to do
}

Bernoulli::~Bernoulli()
{
}

// assignment
Bernoulli &
Bernoulli::operator=(const Bernoulli &rhs)
{
    if (this != &rhs)
    {
        p_ = rhs.p_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Bernoulli::nextValue()
{
    double u = rng_.random0to1();
    return((u <= p_) ? 1 : 0);
}

