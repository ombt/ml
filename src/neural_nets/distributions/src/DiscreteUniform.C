//
// discrete uniform distribution
//
// P(x) = i/(j-i+1) for x in {i,i+1,...,j}
// X = i+floor((j-i+1)*U), U in (0,1).

// headers
#include "DiscreteUniform.H"

// ctors and dtor
DiscreteUniform::DiscreteUniform(): 
    start_(0), end_(0), rng_()
{
}

DiscreteUniform::DiscreteUniform(
    double start, double end, const Random &rng): 
    start_(start), end_(end), rng_(rng)
{
}

DiscreteUniform::DiscreteUniform(const DiscreteUniform &src): 
    start_(src.start_), end_(src.end_), rng_(src.rng_)
{
    // nothing to do
}

DiscreteUniform::~DiscreteUniform()
{
}

// assignment
DiscreteUniform &
DiscreteUniform::operator=(const DiscreteUniform &rhs)
{
    if (this != &rhs)
    {
        start_ = rhs.start_;
        end_ = rhs.end_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
DiscreteUniform::nextValue()
{
    double u = rng_.random0to1();
    return(start_+floor((end_-start_+1)*u));
}

