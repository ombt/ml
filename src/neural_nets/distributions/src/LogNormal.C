//
// lognormal distribution
//
// 1) calculate mu, sigma2 from the lognormal
// muln, sigmaln2 as follows:
// mu = log(muln**2/sqrt(sigmaln2+muln**2))
// sigma2 = log((sigmaln2+muln**2)/muln**2)
// 2) generate y ~ N(mu, sigma2).
// 3) return x = exp(y).

// headers
#include "LogNormal.H"

// ctors and dtor
LogNormal::LogNormal(): 
    muln_(0), sigmaln2_(0), pgrng_(NULL)
{
}

LogNormal::LogNormal(double muln, double sigmaln2, const Random &rng): 
    muln_(muln), sigmaln2_(sigmaln2)
{
    pgrng_ = new Gaussian(log(muln*muln/sqrt(sigmaln2+muln*muln)),
                          log((sigmaln2+muln*muln)/(muln*muln)), 
                          rng);
}

LogNormal::LogNormal(const LogNormal &src): 
    muln_(src.muln_), sigmaln2_(src.sigmaln2_), pgrng_(src.pgrng_)
{
    // nothing to do
}

LogNormal::~LogNormal()
{
}

// assignment
LogNormal &
LogNormal::operator=(const LogNormal &rhs)
{
    if (this != &rhs)
    {
        muln_ = rhs.muln_;
        sigmaln2_ = rhs.sigmaln2_;
        pgrng_ = rhs.pgrng_;
    }
    return(*this);
}

// get value
double
LogNormal::nextValue()
{
    double y = pgrng_->nextValue();
    return(exp(y));
}

