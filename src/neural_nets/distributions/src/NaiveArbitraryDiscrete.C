//
// arbitrary discrete distribution
//
// p(0), p(1), p(2), ..., p(n)
// choose U in (0,1), then return X=I where:
// sum from j=0 to j=I-1 of p(j) <= U < sum from j=0 to I of p(j)

// headers
#include "NaiveArbitraryDiscrete.H"

// ctors and dtor
NaiveArbitraryDiscrete::NaiveArbitraryDiscrete(): 
    dist_(), rng_()
{
}

NaiveArbitraryDiscrete::NaiveArbitraryDiscrete(
    const std::vector<double> &mass, const Random &rng): 
        dist_(), rng_(rng)
{
    long distsize = mass.size() + 1;
    dist_.resize(distsize);
    dist_[0] = 0.0;
    for (int i=1; i<distsize; ++i)
    {
        dist_[i] = dist_[i-1] + mass[i-1];
    }
}

NaiveArbitraryDiscrete::NaiveArbitraryDiscrete(
    const NaiveArbitraryDiscrete &src): 
        dist_(src.dist_), rng_(src.rng_)
{
    // nothing to do
}

NaiveArbitraryDiscrete::~NaiveArbitraryDiscrete()
{
}

// assignment
NaiveArbitraryDiscrete &
NaiveArbitraryDiscrete::operator=(const NaiveArbitraryDiscrete &rhs)
{
    if (this != &rhs)
    {
        dist_ = rhs.dist_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
NaiveArbitraryDiscrete::nextValue()
{
    // very naive way 
    double U = rng_.random0to1();
    long I;
    for (I=dist_.size()-1; I>=0 && U<=dist_[I]; --I) ;
    return(I+1);
}

