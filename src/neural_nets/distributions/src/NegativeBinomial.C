//
// negative binomial distribution
//

// headers
#include "NegativeBinomial.H"

// ctors and dtor
NegativeBinomial::NegativeBinomial(): 
    n_(0), grng_()
{
}

NegativeBinomial::NegativeBinomial(double p, unsigned long n, const Random &rng): 
    n_(n), grng_(p, rng)
{
}

NegativeBinomial::NegativeBinomial(const NegativeBinomial &src): 
    n_(src.n_), grng_(src.grng_)
{
    // nothing to do
}

NegativeBinomial::~NegativeBinomial()
{
}

// assignment
NegativeBinomial &
NegativeBinomial::operator=(const NegativeBinomial &rhs)
{
    if (this != &rhs)
    {
        n_ = rhs.n_;
        grng_ = rhs.grng_;
    }
    return(*this);
}

// get value
double
NegativeBinomial::nextValue()
{
    double X = 0;
    for (int i=1; i<=n_; ++i)
    {
        X += grng_.nextValue();
    }
    return(X);
}

