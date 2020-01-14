//
// simple pseudo-random number generator
//

// headers
#include "Random.H"

// old version 
static unsigned long oldm = 121500;
static unsigned long olda = 2041;
static unsigned long oldc = 25673;
static unsigned long oldIkey = 71551;

void
setKey(unsigned long newKey)
{
    oldIkey = newKey;
    return;
}

unsigned long
myrandom()
{
    oldIkey = (olda*oldIkey+oldc)%oldm;
    return(oldIkey);
}

unsigned char
myrandomchar()
{
    oldIkey = (olda*oldIkey+oldc)%oldm;
    return('A'+oldIkey%26);
}

// ctors and dtor
Random::Random(): 
    m_(::oldm), a_(::olda), c_(::oldc), Ikey_(::oldIkey)
{
}

Random::Random(unsigned long newKey, 
    unsigned long m, unsigned long a, unsigned long c):
    m_(m), a_(a), c_(c), Ikey_(newKey)
{
}

Random::~Random()
{
}

// operations
void 
Random::setKey(unsigned long newKey)
{
    Ikey_ = newKey;
}

unsigned long 
Random::random() 
{
    Ikey_ = (a_*Ikey_+c_)%m_; return(Ikey_);
}

double 
Random::random0to1() 
{
    Ikey_ = (a_*Ikey_+c_)%m_; return(double(Ikey_)/double(m_));
}

unsigned char 
Random::randomchar() 
{
    Ikey_ = (a_*Ikey_+c_)%m_; return('A'+Ikey_%26);
}


