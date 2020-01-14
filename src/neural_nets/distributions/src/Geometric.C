//
// geometric distribution
//
// U in U(0,1)
// X = ceil(log(U)/log(1-p))
//

// headers
#include "Geometric.H"

// ctors and dtor
Geometric::Geometric(): 
    p_(0), rng_()
{
}

Geometric::Geometric(double p, const Random &rng): 
    p_(p), rng_(rng)
{
}

Geometric::Geometric(const Geometric &src): 
    p_(src.p_), rng_(src.rng_)
{
    // nothing to do
}

Geometric::~Geometric()
{
}

// assignment
Geometric &
Geometric::operator=(const Geometric &rhs)
{
    if (this != &rhs)
    {
        p_ = rhs.p_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Geometric::nextValue()
{
    double U = rng_.random0to1();
    double X = ceil(log(U)/log(1-p_));
    return(X);
}

