#include <iostream>

#define DUMP(X) \
    std::cout << #X << " = " << (X) << std::endl

void foo3(int &l)
{
    std::cout << "lvalue func3 ... " << l << std::endl;
}

void foo3(int &&r)
{
    std::cout << "rvalue func3 ... " << r << std::endl;
}

int foo2(int &&p)
{
    return 2*p;
}

void foo(const int &p)
{
    std::cout << "Actually works !!! - "
              << p << std::endl;
}

void run()
{
    // this actually compiles !!!
    const int &p10 = 10;

    int p=10;
    foo(p);
    foo(100);

    int x = 10;
    DUMP(x);
    int &&rr = 10;
    DUMP(rr);
    int &&rr2 = foo2(10);
    DUMP(rr2);

    int i=10;
    foo3(i);
    foo3(10);
    foo3(std::move(i));
}

int main(int argc, char **argv)
{
    run();
    return 0;
}
