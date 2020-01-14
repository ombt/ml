//
// beta distribution
//
// 1) y1 ~ gamma(alpha1, 1), y2 ~ gamma(alpha2, 1).
// 2) return x = y1/(y1+y2);

// headers
#include "Beta.H"

// ctors and dtor
Beta::Beta(): 
    alpha1_(0), alpha2_(0), pgrng1_(NULL), pgrng2_(NULL)
{
}

Beta::Beta(double alpha1, double alpha2, const Random &rng): 
    alpha1_(alpha1), alpha2_(alpha2), pgrng1_(NULL), pgrng2_(NULL)
{
    pgrng1_ = new Gamma(alpha1_, 1.0, rng);
    pgrng2_ = new Gamma(alpha2_, 1.0, rng);
}

Beta::Beta(const Beta &src): 
    alpha1_(src.alpha1_), alpha2_(src.alpha2_), 
    pgrng1_(src.pgrng1_), pgrng2_(src.pgrng2_)
{
    // nothing to do
}

Beta::~Beta()
{
}

// assignment
Beta &
Beta::operator=(const Beta &rhs)
{
    if (this != &rhs)
    {
        alpha1_ = rhs.alpha1_;
        alpha2_ = rhs.alpha2_;
        pgrng1_ = rhs.pgrng1_;
        pgrng2_ = rhs.pgrng2_;
    }
    return(*this);
}

// get value
double
Beta::nextValue()
{
    double y1 = pgrng1_->nextValue();
    double y2 = pgrng2_->nextValue();
    return(y1/(y1+y2));
}

