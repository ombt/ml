#include <iostream>

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
}

int main(int argc, char **argv)
{
    run();
    return 0;
}
