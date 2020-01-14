//
// m-erlang distribution
//
// X = (-beta/m)*log(u1*u2*...*um)

// headers
#include "mErlang.H"

// ctors and dtor
mErlang::mErlang(): 
    beta_(0), m_(0), rng_()
{
}

mErlang::mErlang(double beta, unsigned long m, const Random &rng): 
    beta_(beta), m_(m), rng_(rng)
{
}

mErlang::mErlang(const mErlang &src): 
    beta_(src.beta_), m_(src.m_), rng_(src.rng_)
{
    // nothing to do
}

mErlang::~mErlang()
{
}

// assignment
mErlang &
mErlang::operator=(const mErlang &rhs)
{
    if (this != &rhs)
    {
        beta_ = rhs.beta_;
        m_ = rhs.m_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
mErlang::nextValue()
{
    double product = 1;
    for (int i=1; i<=m_; ++i)
    {
        product *= rng_.random0to1();
    }
    return(-beta_*log(product)/m_);
}

