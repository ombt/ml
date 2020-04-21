
#include <iostream>

#define DUMP(X) \
    std::cout << #X << " = " << (X) << std::endl

void foo(int &f)
{
    std::cout << "lvalue foo ... " << f << std::endl;
}

void foo(const int &f)
{
    std::cout << "const lvalue foo ... " << f << std::endl;
}

void foo(int &&f)
{
    std::cout << "rvalue foo ... " << f << std::endl;
}

void run()
{
    int i=10;

    foo(i);
    foo(10);
    foo(std::move(i));
    foo(static_cast<int &&>(i));
}

int main(int argc, char **argv)
{
    run();
    return 0;
}
