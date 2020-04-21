
#include <iostream>
#include <vector>

class BooBoo
{
public:
    BooBoo(int x, int y) : _x(x), _y(y) { }
    BooBoo(const BooBoo &s) : _x(s._x), _y(s._y) { }
    ~BooBoo() { }

    BooBoo &operator=(const BooBoo &rhs) {
        if (this != &rhs)
        {
            _x = rhs._x;
            _y = rhs._y;
        }
        return *this;
    }

    friend std::ostream &operator<<(std::ostream &os, const BooBoo &src) {
        os << "(X,Y) = ("
           << src._x
           << ","
           << src._y
           << ")"
           << std::endl;
        return os;
    }

protected:
    int _x;
    int _y;
};

void run()
{
    std::vector<BooBoo> vBooBoo = {
        { 1,2 },
        { 2,4 },
        { 3,9 },
        { 4,16 }
    } ;

    for ( auto bb : vBooBoo )
    {
        std::cout << bb << std::endl;
    }
}

int main(int argc, char **argv)
{
    run();
    return 0;
}

