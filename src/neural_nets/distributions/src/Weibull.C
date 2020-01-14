//
// weibull distribution
//
// F(x) = alpha*(beta**-alpha)*(x**(n-1))*exp(-(x/beta)**alpha)
// u ~ U(0,1)
// x = beta*(-log(1-u))**(1/alpha)

// headers
#include "Weibull.H"

// ctors and dtor
Weibull::Weibull(): 
    alpha_(0), beta_(0), rng_()
{
}

Weibull::Weibull(double alpha, double beta, const Random &rng): 
    alpha_(alpha), beta_(beta), rng_(rng)
{
}

Weibull::Weibull(const Weibull &src): 
    alpha_(src.alpha_), beta_(src.beta_), rng_(src.rng_)
{
    // nothing to do
}

Weibull::~Weibull()
{
}

// assignment
Weibull &
Weibull::operator=(const Weibull &rhs)
{
    if (this != &rhs)
    {
        alpha_ = rhs.alpha_;
        beta_ = rhs.beta_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Weibull::nextValue()
{
    double u = rng_.random0to1();
    return(beta_*pow(-log(u), 1/alpha_));
}

