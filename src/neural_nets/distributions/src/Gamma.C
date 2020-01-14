//
// gamma distribution
//
// headers
#include "Gamma.H"

// ctors and dtor
Gamma::Gamma(): 
    alpha_(0), beta_(0), rng_()
{
}

Gamma::Gamma(double alpha, double beta, const Random &rng): 
    alpha_(alpha), beta_(beta), rng_(rng)
{
    MustBeTrue(alpha_ > 0 && beta_ > 0);
}

Gamma::Gamma(const Gamma &src): 
    alpha_(src.alpha_), beta_(src.beta_), rng_(src.rng_)
{
    // nothing to do
}

Gamma::~Gamma()
{
}

// assignment
Gamma &
Gamma::operator=(const Gamma &rhs)
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
Gamma::nextValue()
{
    if (0.0 < alpha_ && alpha_ < 1.0)
    {
        double b = (M_E+alpha_)/M_E;
        while (true)
        {
            double u1 = rng_.random0to1();
            double p = b*u1;
            if (p > 1.0)
            {
                double y = pow(p, 1/alpha_);
                double u2 = rng_.random0to1();
                if (u2 <= exp(-y)) return(beta_*y);
            }
            else
            {
                double y = -log((b-p)/alpha_);
                double u2 = rng_.random0to1();
                if (u2 <= pow(y, alpha_-1.0)) return(beta_*y);
            }
        }
    }
    else if (alpha_ == 1.0)
    {
        double u = rng_.random0to1();
        return(-beta_*log(u));
    }
    else if (alpha_ > 1.0)
    {
        double a = 1.0/pow(2*alpha_-1, 0.5);
        double b = alpha_ - log(4.0);
        double q = alpha_ - 1/alpha_;
        double theta = 4.5;
        double d = 1 + log(theta);
        while (true)
        {
            double u1 = rng_.random0to1();
            double u2 = rng_.random0to1();
            double v = a*log(u1/(1-u1));
            double y = alpha_*exp(v);
            double z = u1*u1*u2;
            double w = b + q*v - y;
            if ((w+d-theta*z) >= 0)
                return(beta_*y);
            if (w >= log(z))
                return(beta_*y);
        }
    }
    else
    {
        MustBeTrue(alpha_ > 0.0);
        return(0);
    }
}

