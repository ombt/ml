//
// simple pseudo-random number generator
//
#ifndef __RANDOM_H
#define __RANDOM_H

extern void setKey(unsigned long);
extern unsigned long myrandom();
extern unsigned char myrandomchar();

// random class
class Random
{
public:
    // ctors and dtor
    Random();
    Random(unsigned long newKey, 
           unsigned long m = (unsigned long)(-1),
           unsigned long a = 663608941, 
           unsigned long c = 0);
    virtual ~Random();

    // operations
    virtual void setKey(unsigned long);
    virtual unsigned long random();
    virtual double random0to1();
    virtual unsigned char randomchar();

protected:
    // data
    unsigned long m_;
    unsigned long a_;
    unsigned long c_;
    unsigned long Ikey_;
};

#endif
