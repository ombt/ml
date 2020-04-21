
#include <iostream>
#include <vector>

#define DUMP(X) std::cout << #X << " = " << (X) << std::endl

class A 
{
public:
    A() {
        std::cout << "Default ctor" << std::endl;
        p = new int [100];
        for (auto i=0; i<100; i++)
        {
            p[i] = i;
        }
        DUMP(p);
    }
    A(const A &src) {
        std::cout << "Copy ctor" << std::endl;
        p = new int [100];
        for (auto i=0; i<100; i++)
        {
            p[i] = src.p[i];
        }
        DUMP(p);
        DUMP(src.p);
    }
    A(const A &&src) : p(src.p) {
        std::cout << "Move ctor" << std::endl;
        DUMP(p);
        DUMP(src.p);
    }

protected:
    int *p;
} ;

void run()
{
    A a;
    A b = a;
    A c = std::move(a);
    A d(c);
    A e(std::move(c));
}

int main(int argc, char **argv)
{
    run();
    return 0;
}

