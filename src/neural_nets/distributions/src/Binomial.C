//
// binomial distribution
//

// headers
#include "Binomial.H"

// ctors and dtor
Binomial::Binomial(): 
    p_(0), n_(0), rng_()
{
}

Binomial::Binomial(double p, unsigned long n, const Random &rng): 
    p_(p), n_(n), rng_(rng)
{
}

Binomial::Binomial(const Binomial &src): 
    p_(src.p_), n_(src.n_), rng_(src.rng_)
{
    // nothing to do
}

Binomial::~Binomial()
{
}

// assignment
Binomial &
Binomial::operator=(const Binomial &rhs)
{
    if (this != &rhs)
    {
        p_ = rhs.p_;
        n_ = rhs.n_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Binomial::nextValue()
{
    double X = 0;
    for (int i=1; i<=n_; ++i)
    {
        double u = rng_.random0to1();
        if (u<=p_) X += 1;
    }
    return(X);
}

