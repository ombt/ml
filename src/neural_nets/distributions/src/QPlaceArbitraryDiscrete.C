//
// arbitrary discrete distribution
//
// p(0), p(1), p(2), ..., p(n)
// choose U in (0,1), then return X=I where:

// headers
#include "QPlaceArbitraryDiscrete.H"

// ctors and dtor
QPlaceArbitraryDiscrete::QPlaceArbitraryDiscrete(): 
        qdigits_(0), scale_(0), dist_(), rng_()
{
}

QPlaceArbitraryDiscrete::QPlaceArbitraryDiscrete(
    unsigned int qdigits, const std::vector<double> &mass, const Random &rng): 
            qdigits_(qdigits), scale_(0), dist_(), rng_(rng)
{
    scale_ = 1;
    for (int i=1; i<=qdigits; ++i)
    {
        scale_ *= 10;
    }
    dist_.resize(scale_);

    int I=0;
    unsigned long masssize = mass.size();
    unsigned long diststart = 0;
    unsigned long distend = 0;
    for (I=0; I<masssize; ++I)
    {
        unsigned long distsize = mass[I]*scale_;
        distend = diststart + distsize;
        for (unsigned long disti=diststart; disti<distend; ++disti)
        {
            dist_[disti] = I;
        }
    }
    for ( ; distend<scale_; ++distend)
    {
        dist_[distend] = I;
    }
}

QPlaceArbitraryDiscrete::QPlaceArbitraryDiscrete(
    const QPlaceArbitraryDiscrete &src): 
        dist_(src.dist_), rng_(src.rng_)
{
    // nothing to do
}

QPlaceArbitraryDiscrete::~QPlaceArbitraryDiscrete()
{
}

// assignment
QPlaceArbitraryDiscrete &
QPlaceArbitraryDiscrete::operator=(const QPlaceArbitraryDiscrete &rhs)
{
    if (this != &rhs)
    {
        qdigits_ = rhs.qdigits_;
        scale_ = rhs.scale_;
        dist_ = rhs.dist_;
        rng_ = rhs.rng_;
    }
    return(*this);
}

// get value
double
QPlaceArbitraryDiscrete::nextValue()
{
    unsigned long U = rng_.random()%scale_;
    return(dist_[U]);
}

