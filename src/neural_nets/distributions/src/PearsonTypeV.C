//
// pearson type V distribution
//
// 1) y ~ gamma(alpha, 1/beta)
// 2) return x = 1/y.

// headers
#include "PearsonTypeV.H"

// ctors and dtor
PearsonTypeV::PearsonTypeV(): 
    alpha_(0), beta_(0), pgrng_(NULL)
{
}

PearsonTypeV::PearsonTypeV(double alpha, double beta, const Random &rng): 
    alpha_(alpha), beta_(beta), pgrng_(NULL)
{
    pgrng_ = new Gamma(alpha_, 1/beta_, rng);
}

PearsonTypeV::PearsonTypeV(const PearsonTypeV &src): 
    alpha_(src.alpha_), beta_(src.beta_), pgrng_(src.pgrng_)
{
    // nothing to do
}

PearsonTypeV::~PearsonTypeV()
{
}

// assignment
PearsonTypeV &
PearsonTypeV::operator=(const PearsonTypeV &rhs)
{
    if (this != &rhs)
    {
        alpha_ = rhs.alpha_;
        beta_ = rhs.beta_;
        pgrng_ = rhs.pgrng_;
    }
    return(*this);
}

// get value
double
PearsonTypeV::nextValue()
{
    double y = pgrng_->nextValue();
    return(1.0/y);
}

