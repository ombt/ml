//
// gaussian distribution
//
// F(x) closed form does not exist.
// given X ~ N(0,1), then X' ~ N(mu,sigma2) can
// be obtained using this relation: X' = mu + sqrt(sigma2)*X
// so we only need to generate N(0,1).
//
// 1) generate u1 and u2 as IID U(0,1) and calculate:
// v1 = 2*u1-1, v2 = 2*u2-1
// w = v1**2 + v2**2
// 2) if W > 1, then go back to step 1, else let
// y = sqrt(-2*log(w)/w), x1 = v1*y, x2 = v2*y.
// 3) x1 and x2 are IID N(0,1). return x1 or x2 or alternate.
// 4) x = mu + sqrt(sigma2)*x1 or use x2.
// 

// headers
#include "Gaussian.H"

// ctors and dtor
Gaussian::Gaussian(): 
    xindex_(0), mu_(0), sigma_(0), sigma2_(0), rng_()
{
}

Gaussian::Gaussian(double mu, double sigma2, const Random &rng): 
    xindex_(0), mu_(mu), sigma_(0), sigma2_(sigma2), rng_(rng)
{
    sigma_ = sqrt(sigma2);
}

Gaussian::Gaussian(const Gaussian &src): 
    xindex_(src.xindex_), mu_(src.mu_), 
    sigma_(src.sigma_), sigma2_(src.sigma2_), rng_(src.rng_)
{
    // nothing to do
}

Gaussian::~Gaussian()
{
}

// assignment
Gaussian &
Gaussian::operator=(const Gaussian &rhs)
{
    if (this != &rhs)
    {
        xindex_ = rhs.xindex_;
        mu_ = rhs.mu_;
        sigma_ = rhs.sigma_;
        sigma2_ = rhs.sigma2_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Gaussian::nextValue()
{
    double u1, u2, v1, v2, w;

    do {
        u1 = rng_.random0to1();
        u2 = rng_.random0to1();
        v1 = 2*u1 - 1;
        v2 = 2*u2 - 1;
        w = v1*v1 + v2*v2;
    } while (w > 1);

    double y = sqrt(-2*log(w)/w);
    double x1 = v1*y;
    double x2 = v2*y;

    xindex_ = (xindex_+1)%2;
    if (xindex_)
        return(mu_+sigma_*x1);
    else
        return(mu_+sigma_*x2);
}

