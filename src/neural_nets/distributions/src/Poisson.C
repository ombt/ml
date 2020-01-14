//
// poisson distribution
//

// headers
#include "Poisson.H"

// ctors and dtor
Poisson::Poisson(): 
    lambda_(0), a_(0), rng_()
{
}

Poisson::Poisson(double lambda, const Random &rng): 
    lambda_(lambda), a_(0), rng_(rng)
{
    a_ = exp(-lambda);
}

Poisson::Poisson(const Poisson &src): 
    lambda_(src.lambda_), a_(src.a_), rng_(src.rng_)
{
    // nothing to do
}

Poisson::~Poisson()
{
}

// assignment
Poisson &
Poisson::operator=(const Poisson &rhs)
{
    if (this != &rhs)
    {
        lambda_ = rhs.lambda_;
        a_ = rhs.a_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Poisson::nextValue()
{
    double b = 1;
    for (unsigned long i=0; ; ++i)
    {
        double U = rng_.random0to1();
        b = b*U;
        if (b < a_) return(i);
    }
}

