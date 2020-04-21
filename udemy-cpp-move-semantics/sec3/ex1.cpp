
#include <iostream>
#include <vector>

void print_vector(std::vector<int> &v)
{
    auto it = v.begin();
    auto itend = v.end();

    std::cout << "distance ... " << std::distance(it, itend) << std::endl;
    std::cout << "size ... " << v.size() << std::endl;

    for ( ; it!=itend; ++it)
    {
        std::cout << *it << " ";
    }
    std::cout << std::endl;
}

template <typename T>
void swap(T &a, T &b)
{
    T tmp = std::move(a);
    a = std::move(b);
    b = std::move(tmp);
}

void run()
{
    std::vector<int> v1, v2;

    for (auto i=0; i<5; i++)
    {
        v1.push_back(i);
    }

    for (auto i=10; i<15; i++)
    {
        v2.push_back(i);
    }

    print_vector(v1);
    print_vector(v2);

    v2 = std::move(v1);

    print_vector(v1);
    print_vector(v2);

    v1.clear();
    for (auto i=0; i<5; i++)
    {
        v1.push_back(i);
    }

    v2.clear();
    for (auto i=10; i<15; i++)
    {
        v2.push_back(i);
    }

    print_vector(v1);
    print_vector(v2);

    swap(v1, v2);

    print_vector(v1);
    print_vector(v2);
}

int main(int argc, char **argv)
{
    run();
    return 0;
}
