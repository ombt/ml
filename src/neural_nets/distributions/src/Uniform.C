//
// uniform distribution
//
// F(x) = (x-a)/(b-a)
// X = a+U*(b-a), U in (0,1).

// headers
#include "Uniform.H"

// ctors and dtor
Uniform::Uniform(): 
    a_(0), b_(0), rng_()
{
}

Uniform::Uniform(double a, double b, const Random &rng): 
    a_(a), b_(b), rng_(rng)
{
}

Uniform::Uniform(const Uniform &src): 
    a_(src.a_), b_(src.b_), rng_(src.rng_)
{
    // nothing to do
}

Uniform::~Uniform()
{
}

// assignment
Uniform &
Uniform::operator=(const Uniform &rhs)
{
    if (this != &rhs)
    {
        a_ = rhs.a_;
        b_ = rhs.b_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
Uniform::nextValue()
{
    double u = rng_.random0to1();
    return(a_+u*(b_-a_));
}

